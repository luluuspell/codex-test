from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


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
    generation: int = 1
    desired_state: ServiceState = ServiceState.RUNNING
    actual_state: ServiceState = ServiceState.REGISTERED
    process_ok: bool = False
    transport_ok: bool = False
    functional_ok: bool = False
    restart_count: int = 0


@dataclass
class RuntimeSupervisor:
    services: dict[str, ServiceRecord] = field(default_factory=dict)

    def register(self, service_id: str) -> ServiceRecord:
        record = ServiceRecord(service_id=service_id)
        self.services[service_id] = record
        return record

    def assess(self, service_id: str, *, process_ok: bool, transport_ok: bool, functional_ok: bool) -> ServiceRecord:
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

    def restart(self, service_id: str) -> ServiceRecord:
        record = self.services[service_id]
        record.restart_count += 1
        record.generation += 1
        record.actual_state = ServiceState.STARTING
        return record
