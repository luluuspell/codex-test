from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .capabilities import CapabilityRegistry
from .events import EventStore
from .integrity import OperationConflict, StateConflict, TERMINAL_TASKS, UNSETTLED
from .memory import MemoryStore
from .models import OperationState, TaskPhase, TaskState, now
from .resources import release_reconciled_reservation
from .runtime import Capability, OperationRuntime, TaskRuntime, TaskScheduler


@dataclass
class RecoveryCoordinator:
    tasks: TaskRuntime
    operations: OperationRuntime
    memory: MemoryStore | None = None

    def __post_init__(self) -> None:
        self.tasks.operations = self.operations.operations

    @classmethod
    def from_store(cls, store: Any, *, registry: CapabilityRegistry,
                   capabilities: Mapping[str, Capability]) -> RecoveryCoordinator:
        world = store.load_world_model()
        events = EventStore(persistence=store)
        tasks = TaskRuntime(world, events, persistence=store)
        tasks.tasks = store.load_tasks()
        operations = OperationRuntime(world, events, registry,
                                       capabilities=dict(capabilities), persistence=store)
        operations.operations = store.load_operations()
        return cls(tasks, operations, MemoryStore.from_persistence(store))

    def rebuild_scheduler(self) -> TaskScheduler:
        return TaskScheduler.rebuild(self.tasks)

    def recover(self) -> dict[str, Any]:
        attempted = settled = conflicts = released = 0
        errors = []
        for op in list(self.operations.operations.values()):
            if op.state not in UNSETTLED | {OperationState.PREPARED}:
                continue
            try:
                self.operations.recover_operation(op)
                attempted += 1
                if op.state not in UNSETTLED and self.operations.persistence is not None:
                    released += int(release_reconciled_reservation(self.operations.persistence, op))
            except (OperationConflict, StateConflict):
                conflicts += 1  # Another owner/reconciler committed first; do not overwrite it.
            except Exception as exc:
                errors.append({'operation_id': op.operation_id, 'error_type': type(exc).__name__})

        for task in self.tasks.tasks.values():
            try:
                if self.tasks.persistence is not None:
                    self.tasks.refresh(task.task_id)
                if task.state in TERMINAL_TASKS:
                    continue
                pending = self.tasks.has_unsettled(task.task_id)
                if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
                    before = task.state
                    if pending:
                        desired_actual = (TaskState.CANCELLING if task.desired_state is TaskState.CANCELLED
                                          else TaskState.PAUSING)
                        self.tasks.update_runtime_state(task.task_id, state=desired_actual,
                                                        event_type='task.control_waiting_on_operation')
                    else:
                        self.tasks.settle_control(task.task_id, safe_boundary=True)
                    settled += int(before != task.state)
                elif not pending and task.state is TaskState.WAITING and task.wait_reason == 'operation:unresolved':
                    lease = (self.tasks.persistence.get_task_lease(task.task_id)
                             if self.tasks.persistence is not None else None)
                    if lease is None or lease.lease_until <= now():
                        self.tasks.update_runtime_state(task.task_id, state=TaskState.RUNNING,
                            phase=TaskPhase.OBSERVING, event_type='task.recovery_ready')
            except StateConflict:
                conflicts += 1
        self.operations.events.flush_outbox()
        unresolved = sum(op.state in UNSETTLED for op in self.operations.operations.values())
        return {'operations_reconciled': attempted, 'controls_settled': settled,
                'operations_unresolved': unresolved, 'resources_released': released,
                'concurrent_updates_skipped': conflicts, 'errors': errors,
                'status': 'degraded' if unresolved or errors else 'ready'}
