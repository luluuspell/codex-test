import AppKit
import ApplicationServices
import Darwin
import Foundation

private let protocolVersion = 1
private let maxFrameBytes = 1_048_576

private enum BridgeFailure: Error, CustomStringConvertible {
    case message(String)

    var description: String {
        switch self {
        case .message(let value):
            return value
        }
    }
}

private func unixMillis() -> Int64 {
    Int64(Date().timeIntervalSince1970 * 1000.0)
}

private func disableSigPipe(_ fd: Int32) throws {
    var enabled: Int32 = 1
    let result = withUnsafePointer(to: &enabled) { pointer in
        Darwin.setsockopt(
            fd,
            SOL_SOCKET,
            SO_NOSIGPIPE,
            pointer,
            socklen_t(MemoryLayout<Int32>.size)
        )
    }
    guard result == 0 else {
        throw BridgeFailure.message("setsockopt(SO_NOSIGPIPE) failed errno=\(errno)")
    }
}

private func readExact(_ fd: Int32, count: Int) throws -> Data {
    var output = Data()
    output.reserveCapacity(count)
    var remaining = count

    while remaining > 0 {
        let chunkSize = min(remaining, 8192)
        var buffer = [UInt8](repeating: 0, count: chunkSize)
        let received = Darwin.read(fd, &buffer, chunkSize)
        if received == 0 {
            throw BridgeFailure.message("peer closed connection")
        }
        if received < 0 {
            if errno == EINTR {
                continue
            }
            throw BridgeFailure.message("read failed errno=\(errno)")
        }
        output.append(contentsOf: buffer.prefix(Int(received)))
        remaining -= Int(received)
    }
    return output
}

private func readFrame(_ fd: Int32) throws -> [String: Any] {
    let header = try readExact(fd, count: 4)
    let length: UInt32 = header.withUnsafeBytes { raw in
        raw.loadUnaligned(as: UInt32.self).bigEndian
    }
    if length == 0 || length > UInt32(maxFrameBytes) {
        throw BridgeFailure.message("invalid frame length \(length)")
    }
    let payload = try readExact(fd, count: Int(length))
    let object = try JSONSerialization.jsonObject(with: payload)
    guard let dictionary = object as? [String: Any] else {
        throw BridgeFailure.message("frame payload must be JSON object")
    }
    return dictionary
}

private func writeAll(_ fd: Int32, data: Data) throws {
    try data.withUnsafeBytes { raw in
        guard let base = raw.baseAddress else {
            throw BridgeFailure.message("empty write buffer")
        }
        var offset = 0
        while offset < raw.count {
            let written = Darwin.write(
                fd,
                base.advanced(by: offset),
                raw.count - offset
            )
            if written < 0 {
                if errno == EINTR {
                    continue
                }
                throw BridgeFailure.message("write failed errno=\(errno)")
            }
            if written == 0 {
                throw BridgeFailure.message("write returned zero")
            }
            offset += Int(written)
        }
    }
}

private func writeFrame(_ fd: Int32, _ object: [String: Any]) throws {
    let payload = try JSONSerialization.data(
        withJSONObject: object,
        options: [.sortedKeys]
    )
    if payload.isEmpty || payload.count > maxFrameBytes {
        throw BridgeFailure.message("outgoing frame size invalid")
    }
    var length = UInt32(payload.count).bigEndian
    var frame = Data(bytes: &length, count: MemoryLayout<UInt32>.size)
    frame.append(payload)
    try writeAll(fd, data: frame)
}

private func sendError(
    _ fd: Int32,
    code: String,
    message: String
) {
    try? writeFrame(fd, [
        "type": "error",
        "error": [
            "code": code,
            "message": message,
        ],
    ])
}

private func intValue(_ value: Any?) -> Int? {
    guard let number = value as? NSNumber else {
        return nil
    }
    return number.intValue
}

private func int64Value(_ value: Any?) -> Int64? {
    guard let number = value as? NSNumber else {
        return nil
    }
    return number.int64Value
}

private func capabilityManifest() -> [String: Any] {
    let accessibilityTrusted = AXIsProcessTrusted()
    return [
        "desktop.context": "available",
        "desktop.open_file": [
            "state": "unavailable",
            "implementation": "not_implemented",
        ],
        "desktop.accessibility": [
            "state": accessibilityTrusted ? "unavailable" : "permission_required",
            "implementation": "not_implemented",
        ],
        "media.pause": [
            "state": "unavailable",
            "implementation": "not_implemented",
        ],
    ]
}

private struct Receipt {
    let status: String
    let result: [String: Any]
    let evidence: [[String: Any]]
    let error: [String: Any]?
}

private final class BridgeServer {
    private let socketPath: String
    private let sessionToken: String
    private let generation: Int
    private let bridgeInstanceID = "bridge_\(UUID().uuidString.lowercased())"
    private var receipts: [String: Receipt] = [:]

    init(socketPath: String, sessionToken: String, generation: Int) {
        self.socketPath = socketPath
        self.sessionToken = sessionToken
        self.generation = generation
    }

    func run() throws {
        let parent = URL(fileURLWithPath: socketPath).deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: parent,
            withIntermediateDirectories: true
        )
        parent.path.withCString { pointer in
            _ = Darwin.chmod(pointer, mode_t(S_IRWXU))
        }

        var existing = stat()
        let existingResult = socketPath.withCString { pointer in
            Darwin.lstat(pointer, &existing)
        }
        if existingResult == 0 {
            throw BridgeFailure.message(
                "socket path already exists; Engine must verify and clean stale sockets"
            )
        }

        let serverFD = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard serverFD >= 0 else {
            throw BridgeFailure.message("socket() failed errno=\(errno)")
        }
        defer {
            Darwin.close(serverFD)
            socketPath.withCString { pointer in
                _ = Darwin.unlink(pointer)
            }
        }

        var address = sockaddr_un()
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        address.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = Array(socketPath.utf8CString)
        let pathCapacity = MemoryLayout.size(ofValue: address.sun_path)
        guard pathBytes.count <= pathCapacity else {
            throw BridgeFailure.message("Unix socket path is too long")
        }
        withUnsafeMutablePointer(to: &address.sun_path) { tuplePointer in
            tuplePointer.withMemoryRebound(
                to: CChar.self,
                capacity: pathCapacity
            ) { destination in
                for index in 0..<pathCapacity {
                    destination[index] = 0
                }
                for (index, byte) in pathBytes.enumerated() {
                    destination[index] = byte
                }
            }
        }

        let bindResult = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(
                to: sockaddr.self,
                capacity: 1
            ) { rebound in
                Darwin.bind(
                    serverFD,
                    rebound,
                    socklen_t(MemoryLayout<sockaddr_un>.size)
                )
            }
        }
        guard bindResult == 0 else {
            throw BridgeFailure.message("bind() failed errno=\(errno)")
        }
        socketPath.withCString { pointer in
            _ = Darwin.chmod(pointer, mode_t(S_IRUSR | S_IWUSR))
        }
        guard Darwin.listen(serverFD, 16) == 0 else {
            throw BridgeFailure.message("listen() failed errno=\(errno)")
        }

        while true {
            let clientFD = Darwin.accept(serverFD, nil, nil)
            if clientFD < 0 {
                if errno == EINTR {
                    continue
                }
                throw BridgeFailure.message("accept() failed errno=\(errno)")
            }
            autoreleasepool {
                defer { Darwin.close(clientFD) }
                do {
                    try disableSigPipe(clientFD)
                    try handleClient(clientFD)
                } catch {
                    let message = String(describing: error)
                    if message != "peer closed connection" {
                        sendError(
                            clientFD,
                            code: "protocol_error",
                            message: message
                        )
                    }
                }
            }
        }
    }

    private func handleClient(_ fd: Int32) throws {
        var peerUID: uid_t = 0
        var peerGID: gid_t = 0
        guard Darwin.getpeereid(fd, &peerUID, &peerGID) == 0 else {
            throw BridgeFailure.message("getpeereid failed errno=\(errno)")
        }
        guard peerUID == Darwin.geteuid() else {
            sendError(
                fd,
                code: "peer_uid_mismatch",
                message: "connection must come from the same macOS user"
            )
            return
        }

        let hello = try readFrame(fd)
        guard hello["type"] as? String == "hello" else {
            sendError(fd, code: "expected_hello", message: "first frame must be hello")
            return
        }
        guard intValue(hello["protocol_version"]) == protocolVersion else {
            sendError(
                fd,
                code: "unsupported_protocol",
                message: "protocol version mismatch"
            )
            return
        }
        guard hello["session_token"] as? String == sessionToken else {
            sendError(
                fd,
                code: "invalid_session",
                message: "session token rejected"
            )
            return
        }
        guard let engineID = hello["engine_instance_id"] as? String,
              !engineID.isEmpty else {
            sendError(
                fd,
                code: "invalid_engine_instance",
                message: "engine_instance_id is required"
            )
            return
        }

        try writeFrame(fd, [
            "type": "hello_ack",
            "protocol_version": protocolVersion,
            "bridge_instance_id": bridgeInstanceID,
            "generation": generation,
            "capabilities": capabilityManifest(),
        ])

        while true {
            let request: [String: Any]
            do {
                request = try readFrame(fd)
            } catch let failure as BridgeFailure {
                if failure.description == "peer closed connection" {
                    return
                }
                throw failure
            }

            guard request["type"] as? String == "request" else {
                sendError(fd, code: "expected_request", message: "expected request frame")
                return
            }
            try handleRequest(fd, request: request)
        }
    }

    private func handleRequest(
        _ fd: Int32,
        request: [String: Any]
    ) throws {
        guard let requestID = request["request_id"] as? String,
              !requestID.isEmpty,
              let operationID = request["operation_id"] as? String,
              !operationID.isEmpty else {
            sendError(
                fd,
                code: "invalid_correlation",
                message: "request_id and operation_id are required"
            )
            return
        }

        guard intValue(request["expected_bridge_generation"]) == generation else {
            try writeResponse(
                fd,
                requestID: requestID,
                operationID: operationID,
                receipt: Receipt(
                    status: "failed",
                    result: [:],
                    evidence: [],
                    error: [
                        "code": "stale_generation",
                        "message": "Bridge generation changed",
                    ]
                )
            )
            return
        }

        let sentAt = int64Value(request["sent_at_unix_ms"])
        let deadlineAfter = int64Value(request["deadline_after_ms"])
        if let sentAt, let deadlineAfter,
           deadlineAfter > 0,
           unixMillis() > sentAt + deadlineAfter {
            try writeResponse(
                fd,
                requestID: requestID,
                operationID: operationID,
                receipt: Receipt(
                    status: "failed",
                    result: [:],
                    evidence: [],
                    error: [
                        "code": "deadline_exceeded",
                        "message": "request deadline elapsed before execution",
                    ]
                )
            )
            return
        }

        if let previous = receipts[operationID] {
            var replayResult = previous.result
            replayResult["replayed_receipt"] = true
            try writeResponse(
                fd,
                requestID: requestID,
                operationID: operationID,
                receipt: Receipt(
                    status: previous.status,
                    result: replayResult,
                    evidence: previous.evidence,
                    error: previous.error
                )
            )
            return
        }

        guard let method = request["method"] as? String else {
            sendError(fd, code: "invalid_method", message: "method is required")
            return
        }

        let receipt: Receipt
        switch method {
        case "desktop.context":
            receipt = contextReceipt()
        default:
            receipt = Receipt(
                status: "failed",
                result: [:],
                evidence: [],
                error: [
                    "code": "unsupported_method",
                    "message": "method is not implemented by this Bridge",
                ]
            )
        }
        receipts[operationID] = receipt
        try writeResponse(
            fd,
            requestID: requestID,
            operationID: operationID,
            receipt: receipt
        )
    }

    private func contextReceipt() -> Receipt {
        var result: [String: Any] = [
            "observed_at_unix_ms": unixMillis(),
            "frontmost_app_available": false,
        ]
        if let app = NSWorkspace.shared.frontmostApplication {
            result["frontmost_app_available"] = true
            result["pid"] = Int(app.processIdentifier)
            if let name = app.localizedName {
                result["localized_name"] = name
            }
            if let bundleID = app.bundleIdentifier {
                result["bundle_identifier"] = bundleID
            }
        }
        return Receipt(
            status: "succeeded",
            result: result,
            evidence: [[
                "claim": "desktop.context.observed",
                "status": "pass",
                "source": "NSWorkspace",
            ]],
            error: nil
        )
    }

    private func writeResponse(
        _ fd: Int32,
        requestID: String,
        operationID: String,
        receipt: Receipt
    ) throws {
        var response: [String: Any] = [
            "type": "response",
            "request_id": requestID,
            "operation_id": operationID,
            "bridge_generation": generation,
            "status": receipt.status,
            "result": receipt.result,
            "evidence": receipt.evidence,
        ]
        response["error"] = receipt.error ?? NSNull()
        try writeFrame(fd, response)
    }
}

private func requiredEnvironment(
    _ name: String,
    environment: [String: String]
) throws -> String {
    guard let value = environment[name], !value.isEmpty else {
        throw BridgeFailure.message("\(name) is required")
    }
    return value
}

do {
    let environment = ProcessInfo.processInfo.environment
    let socketPath = try requiredEnvironment(
        "LIANDANLU_BRIDGE_SOCKET",
        environment: environment
    )
    let sessionToken = try requiredEnvironment(
        "LIANDANLU_BRIDGE_SESSION_TOKEN",
        environment: environment
    )
    let generationText = environment["LIANDANLU_BRIDGE_GENERATION"] ?? "1"
    guard let generation = Int(generationText), generation > 0 else {
        throw BridgeFailure.message(
            "LIANDANLU_BRIDGE_GENERATION must be positive integer"
        )
    }
    try BridgeServer(
        socketPath: socketPath,
        sessionToken: sessionToken,
        generation: generation
    ).run()
} catch {
    FileHandle.standardError.write(
        Data("LiandanluDesktopBridge fatal: \(error)\n".utf8)
    )
    exit(2)
}
