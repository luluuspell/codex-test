from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from .models import ResourceCapacity, ResourceLease, ResourceRequest, now


class ResourcePersistence(Protocol):
    def acquire_resource_lease(
        self,
        task_id: str,
        owner_id: str,
        request: ResourceRequest,
        capacity: ResourceCapacity,
        *,
        lease_seconds: float,
        now_ts: float | None = None,
    ) -> ResourceLease | None: ...

    def renew_resource_lease(
        self,
        lease: ResourceLease,
        *,
        lease_seconds: float,
        now_ts: float | None = None,
    ) -> ResourceLease | None: ...

    def release_resource_lease(self, lease: ResourceLease) -> bool: ...

    def validate_resource_lease(
        self,
        lease: ResourceLease,
        *,
        now_ts: float | None = None,
    ) -> bool: ...


@dataclass
class ResourceBroker:
    capacity: ResourceCapacity
    persistence: ResourcePersistence | None = None
    _leases: dict[str, ResourceLease] = field(default_factory=dict)

    def _active(self, at: float) -> list[ResourceLease]:
        return [
            lease for lease in self._leases.values()
            if lease.lease_until > at
        ]

    def acquire(
        self,
        task_id: str,
        owner_id: str,
        request: ResourceRequest,
        *,
        lease_seconds: float = 60.0,
        now_ts: float | None = None,
    ) -> ResourceLease | None:
        if not task_id or not owner_id:
            raise ValueError("task_id and owner_id are required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if (
            request.cpu_units > self.capacity.cpu_units
            or request.memory_mb > self.capacity.memory_mb
            or request.gpu_units > self.capacity.gpu_units
        ):
            return None
        if self.persistence is not None:
            return self.persistence.acquire_resource_lease(
                task_id,
                owner_id,
                request,
                self.capacity,
                lease_seconds=lease_seconds,
                now_ts=now_ts,
            )

        ts = now() if now_ts is None else now_ts
        active = [lease for lease in self._active(ts) if lease.task_id != task_id]
        used_cpu = sum(lease.request.cpu_units for lease in active)
        used_memory = sum(lease.request.memory_mb for lease in active)
        used_gpu = sum(lease.request.gpu_units for lease in active)
        active_labels = {
            label
            for lease in active
            for label in lease.request.exclusive_labels
        }
        if (
            used_cpu + request.cpu_units > self.capacity.cpu_units
            or used_memory + request.memory_mb > self.capacity.memory_mb
            or used_gpu + request.gpu_units > self.capacity.gpu_units
            or active_labels.intersection(request.exclusive_labels)
        ):
            return None

        previous = self._leases.get(task_id)
        generation = 1 if previous is None else previous.generation + 1
        lease = ResourceLease(
            task_id=task_id,
            owner_id=owner_id,
            generation=generation,
            request=request,
            claimed_at=ts,
            lease_until=ts + lease_seconds,
        )
        self._leases[task_id] = lease
        return lease

    def renew(
        self,
        lease: ResourceLease,
        *,
        lease_seconds: float = 60.0,
        now_ts: float | None = None,
    ) -> ResourceLease | None:
        if self.persistence is not None:
            return self.persistence.renew_resource_lease(
                lease, lease_seconds=lease_seconds, now_ts=now_ts
            )
        ts = now() if now_ts is None else now_ts
        current = self._leases.get(lease.task_id)
        if (
            current is None
            or current.owner_id != lease.owner_id
            or current.generation != lease.generation
            or current.lease_until <= ts
        ):
            return None
        renewed = ResourceLease(
            task_id=lease.task_id,
            owner_id=lease.owner_id,
            generation=lease.generation,
            request=lease.request,
            claimed_at=lease.claimed_at,
            lease_until=ts + lease_seconds,
        )
        self._leases[lease.task_id] = renewed
        return renewed

    def release(self, lease: ResourceLease) -> bool:
        if self.persistence is not None:
            return self.persistence.release_resource_lease(lease)
        current = self._leases.get(lease.task_id)
        if current is None:
            return False
        if (
            current.owner_id != lease.owner_id
            or current.generation != lease.generation
        ):
            return False
        del self._leases[lease.task_id]
        return True

    def validate(
        self,
        lease: ResourceLease,
        *,
        now_ts: float | None = None,
    ) -> bool:
        if self.persistence is not None:
            return self.persistence.validate_resource_lease(
                lease, now_ts=now_ts
            )
        ts = now() if now_ts is None else now_ts
        current = self._leases.get(lease.task_id)
        return bool(
            current is not None
            and current.owner_id == lease.owner_id
            and current.generation == lease.generation
            and current.lease_until > ts
        )
