from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .context import ContextManifest
from .models import ActionProposal, RiskClass, Task, TaskPhase, TaskState
from .runtime import OperationRuntime, TaskRuntime


class CognitiveModel(Protocol):
    def next_action(self, manifest: ContextManifest) -> ActionProposal: ...


@dataclass
class NativeAgentRunner:
    tasks: TaskRuntime
    operations: OperationRuntime
    model: CognitiveModel

    def step(self, task: Task, manifest: ContextManifest, *, risk: RiskClass = RiskClass.READ, expected_revisions: dict[str, int] | None = None) -> None:
        if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            self.tasks.control(task.task_id, task.desired_state)
            return
        task.state = TaskState.RUNNING
        task.phase = TaskPhase.THINKING
        proposal = self.model.next_action(manifest)
        task.phase = TaskPhase.AUTHORIZING
        op = self.operations.prepare(task, proposal, risk=risk, expected_revisions=expected_revisions or {})
        task.phase = TaskPhase.EXECUTING
        self.operations.execute(op, permission="read" if risk == RiskClass.READ else "write")
        task.phase = TaskPhase.VERIFYING
        self.operations.events.flush_outbox()
