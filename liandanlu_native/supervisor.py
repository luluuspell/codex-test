from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import subprocess
import time


class ServiceState(str, Enum):
    REGISTERED = "REGISTERED"
    STARTING = "STARTING"
    READY = "READY"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    RECOVERING = "RECOVERING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


@dataclass(slots=True)
class ServiceRecord:
    service_id: str
    command: tuple[str, ...] = ()
    generation: int = 1
    desired_state: ServiceState = ServiceState.RUNNING
    actual_state: ServiceState = ServiceState.REGISTERED
    process_ok: bool = False
    transport_ok: bool = False
    functional_ok: bool = False
    restart_count: int = 0
    pid: int | None = None
    started_at: float | None = None
    last_heartbeat_at: float | None = None
    startup_deadline_s: float = 5.0
    heartbeat_timeout_s: float = 10.0
    shutdown_deadline_s: float = 3.0
    last_error: str | None = None


@dataclass
class RuntimeSupervisor:
    services: dict[str, ServiceRecord] = field(default_factory=dict)
    _processes: dict[str, subprocess.Popen] = field(default_factory=dict, repr=False)

    def register(
        self,
        service_id: str,
        *,
        command: tuple[str, ...] = (),
        startup_deadline_s: float = 5.0,
        heartbeat_timeout_s: float = 10.0,
        shutdown_deadline_s: float = 3.0,
    ) -> ServiceRecord:
        if service_id in self.services:
            raise ValueError(f"service already registered: {service_id}")
        record = ServiceRecord(
            service_id=service_id, command=command,
            startup_deadline_s=startup_deadline_s,
            heartbeat_timeout_s=heartbeat_timeout_s,
            shutdown_deadline_s=shutdown_deadline_s,
        )
        self.services[service_id] = record
        return record

    def start(self, service_id: str) -> ServiceRecord:
        record = self.services[service_id]
        if not record.command:
            raise ValueError(f"{service_id} has no managed command")
        if service_id in self._processes and self._processes[service_id].poll() is None:
            raise RuntimeError(f"{service_id} already running")
        proc = subprocess.Popen(
            record.command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._processes[service_id] = proc
        ts = time.monotonic()
        record.pid = proc.pid
        record.started_at = ts
        record.last_heartbeat_at = ts
        record.process_ok = True
        record.transport_ok = False
        record.functional_ok = False
        record.actual_state = ServiceState.STARTING
        record.last_error = None
        return record

    def assess(
        self, service_id: str, *, process_ok: bool,
        transport_ok: bool, functional_ok: bool,
    ) -> ServiceRecord:
        record = self.services[service_id]
        record.process_ok = process_ok
        record.transport_ok = transport_ok
        record.functional_ok = functional_ok
        if not process_ok:
            record.actual_state = ServiceState.FAILED
        elif not transport_ok or not functional_ok:
            record.actual_state = ServiceState.DEGRADED
        else:
            record.actual_state = ServiceState.RUNNING
        return record

    def mark_ready(
        self, service_id: str, *, generation: int,
        transport_ok: bool = True, functional_ok: bool = True,
    ) -> ServiceRecord:
        record = self.services[service_id]
        if generation != record.generation:
            raise ValueError("stale service generation")
        record.transport_ok = transport_ok
        record.functional_ok = functional_ok
        record.process_ok = True
        record.actual_state = (
            ServiceState.RUNNING if transport_ok and functional_ok
            else ServiceState.DEGRADED
        )
        record.last_heartbeat_at = time.monotonic()
        return record

    def heartbeat(self, service_id: str, *, generation: int) -> None:
        record = self.services[service_id]
        if generation != record.generation:
            raise ValueError("stale service generation")
        record.last_heartbeat_at = time.monotonic()

    def tick(self, *, now_monotonic: float | None = None) -> None:
        ts = time.monotonic() if now_monotonic is None else now_monotonic
        for service_id, record in self.services.items():
            proc = self._processes.get(service_id)
            if proc is not None and proc.poll() is not None:
                record.process_ok = False
                record.actual_state = ServiceState.FAILED
                record.last_error = f"process exited with {proc.returncode}"
                continue
            if (
                record.actual_state is ServiceState.STARTING
                and record.started_at is not None
                and ts - record.started_at > record.startup_deadline_s
            ):
                record.actual_state = ServiceState.FAILED
                record.last_error = "startup deadline exceeded"
                continue
            if (
                record.actual_state is ServiceState.RUNNING
                and record.last_heartbeat_at is not None
                and ts - record.last_heartbeat_at > record.heartbeat_timeout_s
            ):
                record.actual_state = ServiceState.DEGRADED
                record.last_error = "heartbeat timeout"

    def stop(self, service_id: str) -> ServiceRecord:
        record = self.services[service_id]
        record.desired_state = ServiceState.STOPPED
        record.actual_state = ServiceState.STOPPING
        proc = self._processes.get(service_id)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=record.shutdown_deadline_s)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=1)
        record.process_ok = False
        record.transport_ok = False
        record.functional_ok = False
        record.actual_state = ServiceState.STOPPED
        record.pid = None
        return record

    def restart(self, service_id: str) -> ServiceRecord:
        record = self.services[service_id]
        proc = self._processes.get(service_id)
        if proc is not None and proc.poll() is None:
            self.stop(service_id)
        record.restart_count += 1
        record.generation += 1
        record.desired_state = ServiceState.RUNNING
        return self.start(service_id) if record.command else record
