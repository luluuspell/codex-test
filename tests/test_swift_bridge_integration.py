import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time

import pytest

from liandanlu_native.desktop_bridge import (
    BridgeRemoteError,
    DesktopBridgeClient,
    probe_socket,
)


SWIFT_BRIDGE_BIN = os.environ.get("LIANDANLU_SWIFT_BRIDGE_BIN")


def _bridge_exit_details(process: subprocess.Popen) -> str:
    rc = process.poll()
    if rc is None:
        return "process_alive"
    stdout, stderr = process.communicate(timeout=1)
    return f"process_exited rc={rc}\nstdout={stdout}\nstderr={stderr}"


@pytest.mark.skipif(
    not SWIFT_BRIDGE_BIN,
    reason="real Swift Desktop Bridge binary is built in the dedicated macOS CI job",
)
def test_real_swift_bridge_handshake_security_context_and_receipt_replay():
    with tempfile.TemporaryDirectory() as td:
        socket_path = Path(td) / "liandanlu-desktop.sock"
        env = os.environ.copy()
        env.update({
            "LIANDANLU_BRIDGE_SOCKET": str(socket_path),
            "LIANDANLU_BRIDGE_SESSION_TOKEN": "integration-secret",
            "LIANDANLU_BRIDGE_GENERATION": "42",
        })
        process = subprocess.Popen(
            [SWIFT_BRIDGE_BIN],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.time() + 8
            while time.time() < deadline:
                if socket_path.exists() and probe_socket(socket_path, timeout=0.1):
                    break
                if process.poll() is not None:
                    raise AssertionError(_bridge_exit_details(process))
                time.sleep(0.05)
            else:
                raise AssertionError("Swift Bridge socket did not become connectable")

            mode = stat.S_IMODE(socket_path.stat().st_mode)
            assert mode == 0o600
            assert process.poll() is None, _bridge_exit_details(process)

            # Prove a valid handshake + real read-only observation before testing
            # rejection paths. This makes Bridge-process crashes diagnosable.
            client = DesktopBridgeClient(
                socket_path,
                session_token="integration-secret",
            )
            try:
                session = client.connect()
            except Exception as exc:
                raise AssertionError(
                    f"valid handshake failed: {exc}; {_bridge_exit_details(process)}"
                ) from exc
            assert session.generation == 42
            assert session.peer_uid_verified
            assert session.supports("desktop.context")
            assert not session.supports("desktop.open_file")

            response = client.request(
                operation_id="op_real_swift_context",
                method="desktop.context",
            )
            assert response.status == "succeeded"
            assert response.result["observed_at_unix_ms"] > 0
            assert response.evidence[0]["claim"] == "desktop.context.observed"
            assert response.evidence[0]["source"] == "NSWorkspace"

            replay = client.request(
                operation_id="op_real_swift_context",
                method="desktop.context",
            )
            assert replay.status == "succeeded"
            assert replay.result["replayed_receipt"] is True
            client.close()

            assert process.poll() is None, _bridge_exit_details(process)

            wrong = DesktopBridgeClient(
                socket_path,
                session_token="wrong-secret",
            )
            try:
                with pytest.raises(BridgeRemoteError) as exc:
                    wrong.connect()
            except Exception as failure:
                raise AssertionError(
                    f"invalid-session rejection failed: {failure}; "
                    f"{_bridge_exit_details(process)}"
                ) from failure
            assert exc.value.code == "invalid_session"
            assert process.poll() is None, _bridge_exit_details(process)
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
