import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import threading
import time

import pytest

from liandanlu_native.desktop_bridge import (
    BridgeProtocolError,
    BridgeRemoteError,
    DesktopBridgeClient,
    FrameCodec,
    PROTOCOL_VERSION,
    StaleBridgeGeneration,
    cleanup_stale_socket,
    peer_uid,
    probe_socket,
)


def start_server(path: Path, handler):
    ready = threading.Event()
    errors = []

    def target():
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(path))
            server.listen(4)
            ready.set()
            conn, _ = server.accept()
            try:
                conn.settimeout(3)
                handler(conn)
            finally:
                conn.close()
        except BaseException as exc:
            errors.append(exc)
            ready.set()
        finally:
            server.close()

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    assert ready.wait(3)
    return thread, errors


def hello_ack(*, generation=7, capabilities=None):
    return {
        "type": "hello_ack",
        "protocol_version": PROTOCOL_VERSION,
        "bridge_instance_id": "bridge_test",
        "generation": generation,
        "capabilities": capabilities or {
            "desktop.context": "available",
            "desktop.open_file": {"state": "available", "permission": "files"},
            "desktop.accessibility": "permission_required",
        },
    }


def test_frame_codec_handles_fragmented_transport():
    left, right = socket.socketpair()
    try:
        frame = FrameCodec.encode({"type": "x", "value": "你好"})
        def send():
            for chunk in (frame[:2], frame[2:7], frame[7:]):
                right.sendall(chunk)
                time.sleep(0.01)
        thread = threading.Thread(target=send)
        thread.start()
        assert FrameCodec.recv(left) == {"type": "x", "value": "你好"}
        thread.join()
    finally:
        left.close()
        right.close()


def test_frame_codec_rejects_oversized_declared_frame_before_body_read():
    left, right = socket.socketpair()
    try:
        right.sendall(struct.pack("!I", 2_000_000))
        with pytest.raises(BridgeProtocolError):
            FrameCodec.recv(left, max_frame_bytes=1024)
    finally:
        left.close()
        right.close()


def test_peer_uid_matches_current_process_when_platform_exposes_credentials():
    left, right = socket.socketpair()
    try:
        uid = peer_uid(left)
        if uid is None:
            pytest.skip("platform Python does not expose Unix peer credentials")
        assert uid == os.getuid()
    finally:
        left.close()
        right.close()


def test_handshake_and_request_round_trip_with_generation_and_evidence():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            hello = FrameCodec.recv(conn)
            assert hello["type"] == "hello"
            assert hello["session_token"] == "secret"
            conn.sendall(FrameCodec.encode(hello_ack()))
            request = FrameCodec.recv(conn)
            assert request["method"] == "desktop.context"
            assert request["expected_bridge_generation"] == 7
            assert request["operation_id"] == "op_1"
            conn.sendall(FrameCodec.encode({
                "type": "response",
                "request_id": request["request_id"],
                "operation_id": request["operation_id"],
                "bridge_generation": 7,
                "status": "succeeded",
                "result": {"frontmost_app": "Finder"},
                "evidence": [{"claim": "frontmost_app", "status": "pass"}],
                "error": None,
            }))

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(path, session_token="secret")
        session = client.connect()
        assert session.generation == 7
        assert session.supports("desktop.context")
        response = client.request(
            operation_id="op_1",
            method="desktop.context",
        )
        assert response.status == "succeeded"
        assert response.result["frontmost_app"] == "Finder"
        assert response.evidence[0]["status"] == "pass"
        client.close()
        thread.join(3)
        assert errors == []


def test_handshake_rejection_is_remote_error_not_transport_success():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            hello = FrameCodec.recv(conn)
            assert hello["session_token"] == "wrong"
            conn.sendall(FrameCodec.encode({
                "type": "error",
                "error": {
                    "code": "invalid_session",
                    "message": "session token rejected",
                },
            }))

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="wrong", require_same_uid=False
        )
        with pytest.raises(BridgeRemoteError) as exc:
            client.connect()
        assert exc.value.code == "invalid_session"
        thread.join(3)
        assert errors == []


def test_permission_required_capability_never_sends_operation_request():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"
        request_seen = []

        def handler(conn):
            FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode(hello_ack()))
            conn.settimeout(0.2)
            try:
                request_seen.append(FrameCodec.recv(conn))
            except Exception:
                pass

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="secret", require_same_uid=False
        )
        client.connect()
        with pytest.raises(BridgeRemoteError) as exc:
            client.request(
                operation_id="op_2",
                method="desktop.accessibility",
            )
        assert exc.value.code == "permission_required"
        client.close()
        thread.join(3)
        assert request_seen == []
        assert errors == []


def test_stale_bridge_generation_is_rejected():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode(hello_ack(generation=4)))
            request = FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode({
                "type": "response",
                "request_id": request["request_id"],
                "operation_id": request["operation_id"],
                "bridge_generation": 5,
                "status": "succeeded",
                "result": {},
                "evidence": [],
                "error": None,
            }))

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="secret", require_same_uid=False
        )
        client.connect()
        with pytest.raises(StaleBridgeGeneration):
            client.request(
                operation_id="op_3",
                method="desktop.context",
            )
        client.close()
        thread.join(3)
        assert errors == []


def test_response_correlation_mismatch_is_protocol_error():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode(hello_ack()))
            request = FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode({
                "type": "response",
                "request_id": "req_wrong",
                "operation_id": request["operation_id"],
                "bridge_generation": 7,
                "status": "succeeded",
                "result": {},
                "evidence": [],
                "error": None,
            }))

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="secret", require_same_uid=False
        )
        client.connect()
        with pytest.raises(BridgeProtocolError):
            client.request(
                operation_id="op_4",
                method="desktop.context",
            )
        client.close()
        thread.join(3)
        assert errors == []


def test_unknown_result_status_is_preserved_for_reconciliation():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode(hello_ack()))
            request = FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode({
                "type": "response",
                "request_id": request["request_id"],
                "operation_id": request["operation_id"],
                "bridge_generation": 7,
                "status": "unknown",
                "result": {},
                "evidence": [],
                "error": {"code": "result_unknown", "message": "reply lost"},
            }))

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="secret", require_same_uid=False
        )
        client.connect()
        response = client.request(
            operation_id="op_5",
            method="desktop.context",
        )
        assert response.status == "unknown"
        assert response.error["code"] == "result_unknown"
        client.close()
        thread.join(3)
        assert errors == []


def test_probe_checks_real_connect_and_stale_cleanup_never_unlinks_live_socket():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(path))
        server.listen(4)
        try:
            assert probe_socket(path)
            assert cleanup_stale_socket(path) is False
        finally:
            server.close()

        assert path.exists()
        assert not probe_socket(path)
        assert cleanup_stale_socket(path)
        assert not path.exists()


def test_cleanup_stale_socket_refuses_regular_file():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "not-a-socket"
        path.write_text("do not delete", encoding="utf-8")
        with pytest.raises(BridgeProtocolError):
            cleanup_stale_socket(path)
        assert path.read_text(encoding="utf-8") == "do not delete"


def test_invalid_method_is_rejected_before_transport_request():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "bridge.sock"

        def handler(conn):
            FrameCodec.recv(conn)
            conn.sendall(FrameCodec.encode(hello_ack()))
            time.sleep(0.1)

        thread, errors = start_server(path, handler)
        client = DesktopBridgeClient(
            path, session_token="secret", require_same_uid=False
        )
        client.connect()
        with pytest.raises(ValueError):
            client.request(
                operation_id="op_6",
                method="OPEN FILE /Users/me/secret",
            )
        client.close()
        thread.join(3)
        assert errors == []
