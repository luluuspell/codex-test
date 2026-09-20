from __future__ import annotations

from dataclasses import dataclass

from .models import OperationState, TaskState
from .runtime import OperationRuntime, TaskRuntime


@dataclass
class RecoveryCoordinator:
    tasks: TaskRuntime
    operations: OperationRuntime

    def recover(self) -> dict[str, int]:
        reconciled = 0
        resumed = 0
        for op in list(self.operations.operations.values()):
            if op.state in {OperationState.RUNNING, OperationState.UNKNOWN, OperationState.PREPARED}:
                self.operations.recover_operation(op, permission="read" if op.risk_class.value == "READ" else "write")
                reconciled += 1
        for task in self.tasks.tasks.values():
            if task.state in {TaskState.RUNNING, TaskState.QUEUED, TaskState.WAITING} and task.desired_state == TaskState.RUNNING:
                task.state = TaskState.RUNNING
                resumed += 1
        self.operations.events.flush_outbox()
        return {"operations_reconciled": reconciled, "tasks_resumed": resumed}
