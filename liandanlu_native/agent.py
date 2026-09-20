from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .context import ContextManifest
from .models import ActionProposal, RiskClass, Task, TaskPhase, TaskState
from .policy import PolicyDecision, PolicyEngine
from .runtime import OperationRuntime, TaskRuntime


class CognitiveModel(Protocol):
    def next_action(self, manifest: ContextManifest) -> ActionProposal: ...


@dataclass
class NativeAgentRunner:
    tasks: TaskRuntime
    operations: OperationRuntime
    model: CognitiveModel
    policy: PolicyEngine | None = None
    workspace_id: str = "default"

    def step(
        self,
        task: Task,
        manifest: ContextManifest,
        *,
        risk: RiskClass = RiskClass.READ,
        expected_revisions: dict[str, int] | None = None,
    ) -> None:
        if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            self.tasks.control(task.task_id, task.desired_state)
            return

        task.state = TaskState.RUNNING
        task.phase = TaskPhase.THINKING
        proposal = self.model.next_action(manifest)

        task.phase = TaskPhase.AUTHORIZING
        if self.policy is not None:
            decision = self.policy.evaluate(
                self.workspace_id, proposal.capability, proposal.action, risk
            )
            if decision is PolicyDecision.DENY:
                task.state = TaskState.BLOCKED
                self.tasks.events.enqueue_outbox(
                    event_type="agent.action.blocked",
                    actor="policy",
                    task_id=task.task_id,
                    object_refs=proposal.object_refs,
                    payload={"capability": proposal.capability, "action": proposal.action},
                )
                return
            if decision is PolicyDecision.REQUIRE_CONFIRMATION:
                task.state = TaskState.WAITING
                self.tasks.events.enqueue_outbox(
                    event_type="agent.action.confirmation_required",
                    actor="policy",
                    task_id=task.task_id,
                    object_refs=proposal.object_refs,
                    payload={"capability": proposal.capability, "action": proposal.action},
                )
                return

        op = self.operations.prepare(
            task, proposal, risk=risk, expected_revisions=expected_revisions or {}
        )
        task.phase = TaskPhase.EXECUTING
        self.operations.execute(op, permission="read" if risk == RiskClass.READ else "write")
        task.phase = TaskPhase.VERIFYING
        self.operations.events.flush_outbox()
