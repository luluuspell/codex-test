from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from typing import Any

from .integrity import UNSETTLED, immediate, resource_is_pinned
from .models import ResourceCapacity, ResourceLease, ResourceRequest, now


def _duration(seconds: float) -> None:
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('lease duration must be finite and positive')


def _quantities(value: Any) -> None:
    if any(type(getattr(value, key)) is not int or getattr(value, key) < 0
           for key in ('cpu_units', 'memory_mb', 'gpu_units')):
        raise ValueError('resource quantities must be nonnegative integers')


def _from_row(row: Any) -> ResourceLease:
    return ResourceLease(row['task_id'], row['owner_id'], row['generation'],
        ResourceRequest(row['cpu_units'], row['memory_mb'], row['gpu_units'],
                        tuple(json.loads(row['exclusive_labels_json']))),
        row['claimed_at'], row['lease_until'])


@dataclass
class ResourceBroker:
    """Admission accounting, not an operating-system RAM/GPU limiter.

    An expired lease is not proof that its worker has stopped using resources.
    Unresolved operations pin their reservation until reconciliation finishes.
    """
    capacity: ResourceCapacity
    persistence: Any = None
    _leases: dict[str, ResourceLease] = field(default_factory=dict)
    operations: dict = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        _quantities(self.capacity)

    def _pinned(self, lease: ResourceLease) -> bool:
        return any(op.task_id == lease.task_id and op.state in UNSETTLED
                   and op.resource_lease_owner_id == lease.owner_id
                   and op.resource_lease_generation == lease.generation
                   for op in self.operations.values())

    def _fits(self, request: ResourceRequest, active: list[ResourceLease]) -> bool:
        labels = {label for lease in active for label in lease.request.exclusive_labels}
        return (sum(x.request.cpu_units for x in active) + request.cpu_units <= self.capacity.cpu_units
                and sum(x.request.memory_mb for x in active) + request.memory_mb <= self.capacity.memory_mb
                and sum(x.request.gpu_units for x in active) + request.gpu_units <= self.capacity.gpu_units
                and not labels.intersection(request.exclusive_labels))

    def acquire(self, task_id: str, owner_id: str, request: ResourceRequest, *,
                lease_seconds: float = 60.0, now_ts: float | None = None) -> ResourceLease | None:
        _duration(lease_seconds)
        _quantities(request)
        if not task_id or not owner_id:
            raise ValueError('task_id and owner_id are required')
        if not all(isinstance(label, str) and label for label in request.exclusive_labels):
            raise ValueError('exclusive labels must be nonempty strings')
        ts = now() if now_ts is None else now_ts
        if not math.isfinite(ts):
            raise ValueError('invalid timestamp')
        if self.persistence is None:
            previous = self._leases.get(task_id)
            if previous is not None and (previous.lease_until > ts or self._pinned(previous)):
                return None
            active = [lease for lease in self._leases.values()
                      if lease.lease_until > ts or self._pinned(lease)]
            if not self._fits(request, active):
                return None
            result = ResourceLease(task_id, owner_id, previous.generation + 1 if previous else 1,
                                   request, ts, ts + lease_seconds)
            self._leases[task_id] = result
            return result
        store = self.persistence
        with immediate(store):
            rows = list(store.conn.execute('SELECT * FROM resource_leases'))
            previous = next((row for row in rows if row['task_id'] == task_id), None)
            active_rows = [row for row in rows if row['lease_until'] > ts or resource_is_pinned(store, row)]
            if any(row['task_id'] == task_id for row in active_rows):
                return None
            if not self._fits(request, [_from_row(row) for row in active_rows]):
                return None
            result = ResourceLease(task_id, owner_id, previous['generation'] + 1 if previous else 1,
                                   request, ts, ts + lease_seconds)
            store.conn.execute('''
                INSERT INTO resource_leases(task_id,owner_id,generation,cpu_units,memory_mb,gpu_units,
                    exclusive_labels_json,claimed_at,lease_until) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET owner_id=excluded.owner_id,generation=excluded.generation,
                    cpu_units=excluded.cpu_units,memory_mb=excluded.memory_mb,gpu_units=excluded.gpu_units,
                    exclusive_labels_json=excluded.exclusive_labels_json,
                    claimed_at=excluded.claimed_at,lease_until=excluded.lease_until
            ''', (task_id, owner_id, result.generation, request.cpu_units, request.memory_mb,
                  request.gpu_units, json.dumps(request.exclusive_labels), ts, result.lease_until))
            return result

    def renew(self, lease: ResourceLease, *, lease_seconds: float = 60.0,
              now_ts: float | None = None) -> ResourceLease | None:
        _duration(lease_seconds)
        if self.persistence is not None:
            return self.persistence.renew_resource_lease(lease, lease_seconds=lease_seconds, now_ts=now_ts)
        ts = now() if now_ts is None else now_ts
        if not self.validate(lease, now_ts=ts):
            return None
        result = ResourceLease(lease.task_id, lease.owner_id, lease.generation,
                               lease.request, lease.claimed_at, ts + lease_seconds)
        self._leases[lease.task_id] = result
        return result

    def release(self, lease: ResourceLease) -> bool:
        if self.persistence is None:
            current = self._leases.get(lease.task_id)
            if current is None or (current.owner_id, current.generation) != (lease.owner_id, lease.generation):
                return False
            if self._pinned(current):
                return False
            self._leases[lease.task_id] = ResourceLease(current.task_id, current.owner_id,
                current.generation, current.request, current.claimed_at, 0.0)
            return True
        store = self.persistence
        with immediate(store):
            row = store.conn.execute('SELECT * FROM resource_leases WHERE task_id=?',
                                     (lease.task_id,)).fetchone()
            if row is None or (row['owner_id'], row['generation']) != (lease.owner_id, lease.generation):
                return False
            if resource_is_pinned(store, row):
                return False
            store.conn.execute('UPDATE resource_leases SET lease_until=0 WHERE task_id=? AND owner_id=? AND generation=?',
                               (lease.task_id, lease.owner_id, lease.generation))
            return True

    def validate(self, lease: ResourceLease, *, now_ts: float | None = None) -> bool:
        if self.persistence is not None:
            return self.persistence.validate_resource_lease(lease, now_ts=now_ts)
        current = self._leases.get(lease.task_id)
        ts = now() if now_ts is None else now_ts
        return bool(current is not None and current.owner_id == lease.owner_id
                    and current.generation == lease.generation and current.request == lease.request
                    and current.lease_until > ts)


def release_reconciled_reservation(store: Any, op: Any) -> bool:
    row = store.conn.execute('SELECT * FROM resource_leases WHERE task_id=?', (op.task_id,)).fetchone()
    if row is None or (row['owner_id'], row['generation']) != (
            op.resource_lease_owner_id, op.resource_lease_generation):
        return False
    return ResourceBroker(ResourceCapacity(0, 0, 0), persistence=store).release(_from_row(row))
