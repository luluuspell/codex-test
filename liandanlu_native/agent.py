from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .context import ContextManifest
from .models import (
    ActionProposal, OperationState, Task, TaskLease, TaskPhase, TaskState,
)
from .policy import PolicyDecision, PolicyEngine
from .resources import ResourceBroker
from .runtime import BudgetExceeded, LeaseLost, OperationRuntime, TaskRuntime


class CognitiveModel(Protocol):
    def next_action(self, manifest: ContextManifest) -> ActionProposal: ...


@dataclass
class NativeAgentRunner:
    tasks: TaskRuntime
    operations: OperationRuntime
    model: CognitiveModel
    policy: PolicyEngine
    resources: ResourceBroker | None = None
    require_task_lease: bool = False
    resource_lease_seconds: float = 60.0

    def step(
        self,
        task: Task,
        manifest: ContextManifest,
        *,
        task_lease: TaskLease | None = None,
    ) -> None:
        if manifest.workspace_id != task.workspace_id:
            raise ValueError("ContextManifest workspace does not match Task")
        if task_lease is not None and task_lease.task_id != task.task_id:
            raise LeaseLost("task lease belongs to another task")
        if self.require_task_lease and task_lease is None:
            self.tasks.wait(
                task.task_id,
                "lease:required",
                event_type="task.execution_lease_required",
            )
            return
        if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            self.tasks.settle_control(task.task_id, safe_boundary=True)
            return

        self.tasks.update_runtime_state(
            task.task_id,
            state=TaskState.RUNNING,
            phase=TaskPhase.THINKING,
            event_type="task.agent_step_started",
        )
        proposal = self.model.next_action(manifest)
        spec = self.operations.registry.resolve(proposal)
        expected_revisions = {
            domain: manifest.world_revisions[domain]
            for domain in spec.revision_domains
        }

        self.tasks.update_runtime_state(
            task.task_id,
            phase=TaskPhase.AUTHORIZING,
            event_type="task.authorizing",
        )
        decision = self.policy.evaluate(
            task.workspace_id,
            proposal.capability,
            proposal.action,
            spec.risk_class,
        )
        if decision is PolicyDecision.DENY:
            self.tasks.update_runtime_state(
                task.task_id,
                state=TaskState.BLOCKED,
                event_type="agent.action.blocked",
                actor="policy",
            )
            return
        if decision is PolicyDecision.REQUIRE_CONFIRMATION:
            self.tasks.update_runtime_state(
                task.task_id,
                state=TaskState.WAITING,
                event_type="agent.action.confirmation_required",
                actor="policy",
            )
            return

        resource_lease = None
        if not spec.resource_request.empty():
            if task_lease is None:
                self.tasks.wait(
                    task.task_id,
                    "resource:task_lease_required",
                    event_type="task.resource_wait",
                )
                return
            if self.resources is None:
                self.tasks.wait(
                    task.task_id,
                    "resource:broker_unavailable",
                    event_type="task.resource_wait",
                )
                return
            resource_lease = self.resources.acquire(
                task.task_id,
                task_lease.owner_id,
                spec.resource_request,
                lease_seconds=self.resource_lease_seconds,
            )
            if resource_lease is None:
                self.tasks.wait(
                    task.task_id,
                    "resource:unavailable",
                    event_type="task.resource_wait",
                )
                return

        try:
            try:
                op = self.operations.prepare(
                    task,
                    proposal,
                    expected_revisions=expected_revisions,
                    task_lease=task_lease,
                    resource_lease=resource_lease,
                )
            except BudgetExceeded as exc:
                self.tasks.wait(
                    task.task_id,
                    f"budget:{exc.reason}",
                    event_type="task.budget_exhausted",
                )
                return
            except LeaseLost:
                self.tasks.wait(
                    task.task_id,
                    "lease:lost",
                    event_type="task.execution_lease_lost",
                )
                return

            self.tasks.update_runtime_state(
                task.task_id,
                phase=TaskPhase.EXECUTING,
                event_type="task.executing",
            )
            try:
                self.operations.execute(op)
            except LeaseLost:
                self.tasks.wait(
                    task.task_id,
                    "lease:lost",
                    event_type="task.execution_lease_lost",
                )
                return

            if op.state in {
                OperationState.UNKNOWN,
                OperationState.VERIFYING,
                OperationState.RECONCILING,
            }:
                self.tasks.update_runtime_state(
                    task.task_id,
                    state=TaskState.WAITING,
                    phase=TaskPhase.VERIFYING,
                    event_type="task.waiting_on_operation",
                )
            elif op.state is OperationState.VERIFIED:
                self.tasks.update_runtime_state(
                    task.task_id,
                    state=TaskState.RUNNING,
                    phase=TaskPhase.OBSERVING,
                    event_type="task.operation_verified",
                )
            else:
                self.tasks.update_runtime_state(
                    task.task_id,
                    state=TaskState.RUNNING,
                    phase=TaskPhase.REPLANNING,
                    event_type="task.replanning",
                )
            self.tasks.settle_control(task.task_id, safe_boundary=True)
            self.operations.events.flush_outbox()
        finally:
            if resource_lease is not None and self.resources is not None:
                self.resources.release(resource_lease)
