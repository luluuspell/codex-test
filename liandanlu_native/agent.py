from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .context import ContextManifest
from .models import ActionProposal, OperationState, Task, TaskPhase, TaskState
from .policy import PolicyDecision, PolicyEngine
from .runtime import OperationRuntime, TaskRuntime


class CognitiveModel(Protocol):
    def next_action(self, manifest: ContextManifest) -> ActionProposal: ...


@dataclass
class NativeAgentRunner:
    tasks: TaskRuntime
    operations: OperationRuntime
    model: CognitiveModel
    policy: PolicyEngine
    workspace_id: str = "default"

    def step(self, task: Task, manifest: ContextManifest) -> None:
        if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            self.tasks.settle_control(task.task_id, safe_boundary=True)
            return

        self.tasks.update_runtime_state(
            task.task_id, state=TaskState.RUNNING, phase=TaskPhase.THINKING,
            event_type="task.agent_step_started",
        )
        proposal = self.model.next_action(manifest)
        spec = self.operations.registry.resolve(proposal)
        expected_revisions = {
            domain: manifest.world_revisions[domain]
            for domain in spec.revision_domains
        }

        self.tasks.update_runtime_state(
            task.task_id, phase=TaskPhase.AUTHORIZING,
            event_type="task.authorizing",
        )
        decision = self.policy.evaluate(
            self.workspace_id, proposal.capability, proposal.action, spec.risk_class
        )
        if decision is PolicyDecision.DENY:
            self.tasks.update_runtime_state(
                task.task_id, state=TaskState.BLOCKED,
                event_type="agent.action.blocked", actor="policy",
            )
            return
        if decision is PolicyDecision.REQUIRE_CONFIRMATION:
            self.tasks.update_runtime_state(
                task.task_id, state=TaskState.WAITING,
                event_type="agent.action.confirmation_required", actor="policy",
            )
            return

        op = self.operations.prepare(
            task, proposal, expected_revisions=expected_revisions
        )
        self.tasks.update_runtime_state(
            task.task_id, phase=TaskPhase.EXECUTING,
            event_type="task.executing",
        )
        self.operations.execute(op)
        if op.state in {
            OperationState.UNKNOWN, OperationState.VERIFYING,
            OperationState.RECONCILING,
        }:
            self.tasks.update_runtime_state(
                task.task_id, state=TaskState.WAITING,
                phase=TaskPhase.VERIFYING, event_type="task.waiting_on_operation",
            )
        elif op.state is OperationState.VERIFIED:
            self.tasks.update_runtime_state(
                task.task_id, state=TaskState.RUNNING,
                phase=TaskPhase.OBSERVING, event_type="task.operation_verified",
            )
        else:
            self.tasks.update_runtime_state(
                task.task_id, state=TaskState.RUNNING,
                phase=TaskPhase.REPLANNING, event_type="task.replanning",
            )
        self.tasks.settle_control(task.task_id, safe_boundary=True)
        self.operations.events.flush_outbox()
