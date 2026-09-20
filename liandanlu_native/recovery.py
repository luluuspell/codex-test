from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .capabilities import CapabilityRegistry
from .events import EventStore
from .models import OperationState, TaskState
from .runtime import Capability, OperationRuntime, TaskRuntime


@dataclass
class RecoveryCoordinator:
    tasks: TaskRuntime
    operations: OperationRuntime

    @classmethod
    def from_store(
        cls,
        store,
        *,
        registry: CapabilityRegistry,
        capabilities: Mapping[str, Capability],
    ) -> "RecoveryCoordinator":
        world = store.load_world_model()
        events = EventStore(persistence=store)
        tasks = TaskRuntime(world, events, persistence=store)
        tasks.tasks = store.load_tasks()
        operations = OperationRuntime(
            world=world,
            events=events,
            registry=registry,
            capabilities=dict(capabilities),
            persistence=store,
        )
        operations.operations = store.load_operations()
        return cls(tasks=tasks, operations=operations)

    def recover(self) -> dict[str, int]:
        reconciled = 0
        controls_settled = 0

        for op in list(self.operations.operations.values()):
            if op.state in {
                OperationState.RUNNING,
                OperationState.UNKNOWN,
                OperationState.RECONCILING,
                OperationState.OBSERVED,
                OperationState.VERIFYING,
            }:
                self.operations.recover_operation(op)
                reconciled += 1

        for task in self.tasks.tasks.values():
            if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
                before = task.state
                self.tasks.settle_control(task.task_id, safe_boundary=True)
                if task.state != before:
                    controls_settled += 1

        self.operations.events.flush_outbox()
        return {
            "operations_reconciled": reconciled,
            "controls_settled": controls_settled,
        }
