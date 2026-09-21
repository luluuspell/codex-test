from __future__ import annotations

from dataclasses import dataclass, field
import ctypes
import json
import os
from pathlib import Path
import re
import socket
import stat
import struct
import sys
import time
from typing import Any
import uuid


PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = 1_048_576
_ALLOWED_STATUSES = frozenset({
    "succeeded", "blocked", "failed", "unknown", "cancelled",
})
_METHOD_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,127}$")


class BridgeError(RuntimeError):
    pass


class BridgeTransportError(BridgeError):
    pass


class BridgeProtocolError(BridgeError):
    pass


class BridgeUnavailable(BridgeError):
    pass


class StaleBridgeGeneration(BridgeProtocolError):
    pass


class BridgeRemoteError(BridgeError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class BridgeCapability:
    method: str
    state: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return self.state == "available"


@dataclass(frozen=True, slots=True)
class BridgeSession:
    bridge_instance_id: str
    generation: int
    capabilities: dict[str, BridgeCapability]
    peer_uid: int | None = None
    peer_uid_verified: bool = False

    def capability(self, method: str) -> BridgeCapability | None:
        return self.capabilities.get(method)

    def supports(self, method: str) -> bool:
        cap = self.capability(method)
        return bool(cap and cap.available)


@dataclass(frozen=True, slots=True)
class BridgeResponse:
    request_id: str
    operation_id: str
    status: str
    result: dict[str, Any]
    evidence: tuple[dict[str, Any], ...]
    error: dict[str, Any] | None
    bridge_generation: int


class FrameCodec:
    @staticmethod
    def encode(
        message: dict[str, Any],
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> bytes:
        if not isinstance(message, dict):
            raise BridgeProtocolError("frame payload must be a JSON object")
        try:
            payload = json.dumps(
                message,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise BridgeProtocolError("frame is not JSON serializable") from exc
        if not payload:
            raise BridgeProtocolError("empty frame is not allowed")
        if len(payload) > max_frame_bytes:
            raise BridgeProtocolError(
                f"frame exceeds maximum size: {len(payload)} > {max_frame_bytes}"
            )
        return struct.pack("!I", len(payload)) + payload

    @staticmethod
    def _recv_exact(sock: socket.socket, count: int) -> bytes:
        chunks: list[bytes] = []
        remaining = count
        while remaining:
            try:
                chunk = sock.recv(remaining)
            except socket.timeout as exc:
                raise BridgeTransportError("socket receive timed out") from exc
            except OSError as exc:
                raise BridgeTransportError(f"socket receive failed: {exc}") from exc
            if not chunk:
                raise BridgeTransportError("socket closed before frame completed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    @classmethod
    def recv(
        cls,
        sock: socket.socket,
        *,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> dict[str, Any]:
        header = cls._recv_exact(sock, 4)
        (length,) = struct.unpack("!I", header)
        if length <= 0:
            raise BridgeProtocolError("frame length must be positive")
        if length > max_frame_bytes:
            raise BridgeProtocolError(
                f"incoming frame exceeds maximum size: {length} > {max_frame_bytes}"
            )
        payload = cls._recv_exact(sock, length)
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BridgeProtocolError("incoming frame is not valid UTF-8 JSON") from exc
        if not isinstance(value, dict):
            raise BridgeProtocolError("incoming frame must contain a JSON object")
        return value


def peer_uid(sock: socket.socket) -> int | None:
    """Best-effort peer UID for Unix-domain sockets.

    macOS/BSD expose getpeereid on supported Python builds; Linux exposes
    SO_PEERCRED. None means the platform/runtime did not expose a supported
    credential primitive and callers must decide whether to fail closed.
    """
    getter = getattr(sock, "getpeereid", None)
    if getter is not None:
        try:
            uid, _gid = getter()
            return int(uid)
        except OSError:
            return None

    if sys.platform == "darwin":
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            getpeereid = libc.getpeereid
            getpeereid.argtypes = [
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_uint),
                ctypes.POINTER(ctypes.c_uint),
            ]
            getpeereid.restype = ctypes.c_int
            uid = ctypes.c_uint()
            gid = ctypes.c_uint()
            if getpeereid(sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)) == 0:
                return int(uid.value)
        except (AttributeError, OSError, ValueError):
            pass

    so_peercred = getattr(socket, "SO_PEERCRED", None)
    if so_peercred is not None:
        try:
            raw = sock.getsockopt(socket.SOL_SOCKET, so_peercred, struct.calcsize("3i"))
            _pid, uid, _gid = struct.unpack("3i", raw)
            return int(uid)
        except (OSError, struct.error):
            return None
    return None


def probe_socket(path: str | Path, *, timeout: float = 0.2) -> bool:
    """Return True only when a Unix socket accepts a real connection."""
    candidate = str(path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(candidate)
        return True
    except (OSError, socket.timeout):
        return False
    finally:
        sock.close()


def cleanup_stale_socket(path: str | Path, *, timeout: float = 0.2) -> bool:
    """Remove a dead Unix socket path, never a regular file.

    Returns True only when a stale socket node was actually removed.
    """
    candidate = Path(path)
    try:
        mode = candidate.lstat().st_mode
    except FileNotFoundError:
        return False
    if not stat.S_ISSOCK(mode):
        raise BridgeProtocolError("refusing to unlink non-socket path")
    if probe_socket(candidate, timeout=timeout):
        return False
    candidate.unlink()
    return True


class DesktopBridgeClient:
    def __init__(
        self,
        socket_path: str | Path,
        *,
        session_token: str,
        engine_instance_id: str | None = None,
        connect_timeout: float = 1.0,
        request_timeout: float = 5.0,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        require_same_uid: bool = True,
    ):
        if not session_token:
            raise ValueError("session_token is required")
        self.socket_path = str(socket_path)
        self.session_token = session_token
        self.engine_instance_id = engine_instance_id or f"engine_{uuid.uuid4().hex}"
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.max_frame_bytes = max_frame_bytes
        self.require_same_uid = require_same_uid
        self._socket: socket.socket | None = None
        self.session: BridgeSession | None = None

    @property
    def connected(self) -> bool:
        return self._socket is not None and self.session is not None

    def connect(self) -> BridgeSession:
        if self.connected:
            raise BridgeProtocolError("bridge client is already connected")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout)
        try:
            sock.connect(self.socket_path)
        except OSError as exc:
            sock.close()
            raise BridgeUnavailable(
                f"cannot connect to desktop bridge: {self.socket_path}"
            ) from exc

        uid = peer_uid(sock)
        expected_uid = os.getuid() if hasattr(os, "getuid") else None
        uid_verified = uid is not None and expected_uid is not None and uid == expected_uid
        if self.require_same_uid and not uid_verified:
            sock.close()
            if uid is None:
                raise BridgeProtocolError("peer UID could not be verified")
            raise BridgeProtocolError(
                f"desktop bridge peer UID mismatch: expected {expected_uid}, got {uid}"
            )

        sock.settimeout(self.request_timeout)
        self._socket = sock
        try:
            hello = {
                "type": "hello",
                "protocol_version": PROTOCOL_VERSION,
                "engine_instance_id": self.engine_instance_id,
                "session_token": self.session_token,
            }
            self._send(hello)
            ack = self._recv()
            self.session = self._parse_hello_ack(
                ack,
                peer_uid_value=uid,
                peer_uid_verified=uid_verified,
            )
            return self.session
        except Exception:
            self.close()
            raise

    def _parse_hello_ack(
        self,
        ack: dict[str, Any],
        *,
        peer_uid_value: int | None,
        peer_uid_verified: bool,
    ) -> BridgeSession:
        if ack.get("type") == "error":
            error = ack.get("error") or {}
            raise BridgeRemoteError(
                str(error.get("code", "bridge_error")),
                str(error.get("message", "bridge rejected handshake")),
            )
        if ack.get("type") != "hello_ack":
            raise BridgeProtocolError("expected hello_ack")
        if ack.get("protocol_version") != PROTOCOL_VERSION:
            raise BridgeProtocolError(
                f"protocol mismatch: expected {PROTOCOL_VERSION}, "
                f"got {ack.get('protocol_version')}"
            )
        instance_id = ack.get("bridge_instance_id")
        generation = ack.get("generation")
        if not isinstance(instance_id, str) or not instance_id:
            raise BridgeProtocolError("hello_ack missing bridge_instance_id")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation <= 0:
            raise BridgeProtocolError("hello_ack generation must be a positive integer")
        raw_caps = ack.get("capabilities", {})
        if not isinstance(raw_caps, dict):
            raise BridgeProtocolError("hello_ack capabilities must be an object")
        capabilities: dict[str, BridgeCapability] = {}
        for method, raw in raw_caps.items():
            if not isinstance(method, str) or not _METHOD_RE.fullmatch(method):
                raise BridgeProtocolError(f"invalid capability method: {method!r}")
            if isinstance(raw, str):
                state = raw
                details: dict[str, Any] = {}
            elif isinstance(raw, dict):
                state = raw.get("state")
                details = {
                    str(k): v for k, v in raw.items() if k != "state"
                }
            else:
                raise BridgeProtocolError(
                    f"invalid capability descriptor for {method}"
                )
            if state not in {"available", "permission_required", "unavailable"}:
                raise BridgeProtocolError(
                    f"invalid capability state for {method}: {state!r}"
                )
            capabilities[method] = BridgeCapability(method, str(state), details)
        return BridgeSession(
            bridge_instance_id=instance_id,
            generation=generation,
            capabilities=capabilities,
            peer_uid=peer_uid_value,
            peer_uid_verified=peer_uid_verified,
        )

    def close(self) -> None:
        sock, self._socket = self._socket, None
        self.session = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def __enter__(self) -> "DesktopBridgeClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _send(self, message: dict[str, Any]) -> None:
        if self._socket is None:
            raise BridgeUnavailable("desktop bridge is not connected")
        frame = FrameCodec.encode(
            message,
            max_frame_bytes=self.max_frame_bytes,
        )
        try:
            self._socket.sendall(frame)
        except (OSError, socket.timeout) as exc:
            raise BridgeTransportError("socket send failed") from exc

    def _recv(self) -> dict[str, Any]:
        if self._socket is None:
            raise BridgeUnavailable("desktop bridge is not connected")
        return FrameCodec.recv(
            self._socket,
            max_frame_bytes=self.max_frame_bytes,
        )

    def request(
        self,
        *,
        operation_id: str,
        method: str,
        locator_token: str | None = None,
        arguments: dict[str, Any] | None = None,
        deadline_after_ms: int = 5_000,
    ) -> BridgeResponse:
        if not self.connected or self.session is None:
            raise BridgeUnavailable("desktop bridge is not connected")
        if not operation_id:
            raise ValueError("operation_id is required")
        if not _METHOD_RE.fullmatch(method):
            raise ValueError(f"invalid bridge method: {method!r}")
        if deadline_after_ms <= 0:
            raise ValueError("deadline_after_ms must be positive")
        capability = self.session.capability(method)
        if capability is None:
            raise BridgeProtocolError(f"bridge did not advertise capability: {method}")
        if capability.state == "permission_required":
            raise BridgeRemoteError(
                "permission_required",
                f"{method} requires a macOS permission",
            )
        if not capability.available:
            raise BridgeRemoteError(
                "capability_unavailable",
                f"{method} is unavailable",
            )
        request_id = f"req_{uuid.uuid4().hex}"
        message = {
            "type": "request",
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "operation_id": operation_id,
            "method": method,
            "expected_bridge_generation": self.session.generation,
            "deadline_after_ms": deadline_after_ms,
            "sent_at_unix_ms": int(time.time() * 1000),
            "locator_token": locator_token,
            "arguments": arguments or {},
        }
        self._send(message)
        raw = self._recv()
        return self._parse_response(
            raw,
            request_id=request_id,
            operation_id=operation_id,
        )

    def _parse_response(
        self,
        raw: dict[str, Any],
        *,
        request_id: str,
        operation_id: str,
    ) -> BridgeResponse:
        if raw.get("type") != "response":
            raise BridgeProtocolError("expected response frame")
        if raw.get("request_id") != request_id:
            raise BridgeProtocolError("response request_id mismatch")
        if raw.get("operation_id") != operation_id:
            raise BridgeProtocolError("response operation_id mismatch")
        generation = raw.get("bridge_generation")
        if generation != self.session.generation:
            raise StaleBridgeGeneration(
                f"expected bridge generation {self.session.generation}, got {generation}"
            )
        status = raw.get("status")
        if status not in _ALLOWED_STATUSES:
            raise BridgeProtocolError(f"invalid bridge response status: {status!r}")
        result = raw.get("result") or {}
        evidence = raw.get("evidence") or []
        error = raw.get("error")
        if not isinstance(result, dict):
            raise BridgeProtocolError("response result must be an object")
        if not isinstance(evidence, list) or not all(
            isinstance(item, dict) for item in evidence
        ):
            raise BridgeProtocolError("response evidence must be a list of objects")
        if error is not None and not isinstance(error, dict):
            raise BridgeProtocolError("response error must be an object or null")
        return BridgeResponse(
            request_id=request_id,
            operation_id=operation_id,
            status=str(status),
            result=result,
            evidence=tuple(evidence),
            error=error,
            bridge_generation=int(generation),
        )
