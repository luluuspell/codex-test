from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .approvals import ApprovalError, ApprovalService
from .capabilities import InvalidAction
from .context import ContextManifest
from .integrity import TERMINAL_TASKS, UNSETTLED, StateConflict
from .models import ActionProposal, OperationState, Task, TaskLease, TaskPhase, TaskState
from .owned_state import check_owner, update_owned
from .policy import PolicyDecision, PolicyEngine
from .resources import ResourceBroker
from .runtime import BudgetExceeded, OperationRuntime, TaskRuntime
from .world import StaleWorld


class CognitiveModel(Protocol):
    def next_action(self, manifest: ContextManifest) -> ActionProposal: ...


@dataclass
class NativeAgentRunner:
    tasks: TaskRuntime
    operations: OperationRuntime
    model: CognitiveModel
    policy: PolicyEngine
    resources: ResourceBroker | None = None
    require_task_lease: bool | None = None
    resource_lease_seconds: float = 60.0
    approvals: ApprovalService | None = None

    def __post_init__(self) -> None:
        if self.tasks.persistence is not self.operations.persistence:
            raise ValueError('task and operation runtimes must share persistence')
        self.tasks.operations = self.operations.operations
        if self.resources is not None:
            if self.resources.persistence is not self.operations.persistence:
                raise ValueError('resource broker must share the runtime persistence')
            self.resources.operations = self.operations.operations
        if self.approvals is None and self.tasks.persistence is not None:
            # Default creates durable requests, but authorizes NO human principal.
            # An authenticated host must explicitly configure the approval service.
            self.approvals = ApprovalService(self.tasks.persistence, self.operations.registry, self.policy)
        if self.approvals is not None:
            if (self.approvals.store is not self.operations.persistence
                    or self.approvals.registry is not self.operations.registry
                    or self.approvals.policy is not self.policy):
                raise ValueError('approval service must share store, registry and policy')
            self.operations.approval_service = self.approvals

    def _control_pending(self, task: Task) -> bool:
        if task.state in TERMINAL_TASKS or task.state is TaskState.BLOCKED:
            return True
        if task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            self.tasks.settle_control(task.task_id, safe_boundary=True)
            return True
        return False

    def step(self, task: Task, manifest: ContextManifest, *, task_lease: TaskLease | None = None) -> None:
        if manifest.task_id != task.task_id or manifest.workspace_id != task.workspace_id:
            raise ValueError('ContextManifest task/workspace does not match Task')
        check_owner(self.tasks, task.task_id, task_lease)
        if self.tasks.persistence is not None:
            self.tasks.refresh(task.task_id)
        if self._control_pending(task):
            return
        required = self.tasks.persistence is not None if self.require_task_lease is None else self.require_task_lease
        if required and task_lease is None:
            update_owned(self.tasks, task, None, state=TaskState.WAITING, reason='lease:required',
                         event_type='task.execution_lease_required')
            self.operations.events.flush_outbox()
            return

        resource_lease = None
        op = None
        approval_id = None
        try:
            if self.tasks.has_unsettled(task.task_id):
                update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                             reason='operation:unresolved', event_type='task.waiting_on_operation')
                return
            from .models import now
            reason = task.budget.block_reason(now())
            if reason:
                update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                             reason=f'budget:{reason}', event_type='task.budget_exhausted')
                return
            outstanding = self.approvals.outstanding(task.task_id) if self.approvals else None
            if outstanding is not None:
                if outstanding['effective_state'] == 'EXPIRED':
                    raise ApprovalError('approval expired; a new reviewed request is required')
                if outstanding['state'] == 'PENDING':
                    update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                        reason=f'approval:{outstanding["approval_id"]}', event_type='task.waiting_for_approval')
                    return
                approval_id = outstanding['approval_id']
                proposal, expected = self.approvals.approved_action(approval_id)
                update_owned(self.tasks, task, task_lease, state=TaskState.RUNNING,
                    phase=TaskPhase.AUTHORIZING, event_type='task.approved_action_resumed')
            else:
                update_owned(self.tasks, task, task_lease, state=TaskState.RUNNING,
                             phase=TaskPhase.THINKING, event_type='task.agent_step_started')
                proposal = self.model.next_action(manifest)
                check_owner(self.tasks, task.task_id, task_lease)
                if self.tasks.persistence is not None:
                    self.tasks.refresh(task.task_id)
                if self._control_pending(task):
                    return
                try:
                    spec = self.operations.registry.resolve(proposal)
                except InvalidAction:
                    update_owned(self.tasks, task, task_lease, state=TaskState.BLOCKED,
                                 reason='action:invalid', event_type='agent.action.invalid')
                    return
                expected = {domain: manifest.world_revisions[domain] for domain in spec.revision_domains}

            spec = self.operations.registry.resolve(proposal)
            update_owned(self.tasks, task, task_lease, phase=TaskPhase.AUTHORIZING,
                         event_type='task.authorizing')
            decision = self.policy.evaluate(task.workspace_id, proposal.capability,
                                            proposal.action, spec.risk_class)
            if decision is PolicyDecision.DENY:
                update_owned(self.tasks, task, task_lease, state=TaskState.BLOCKED,
                    reason='policy:denied', event_type='agent.action.blocked', actor='policy')
                return
            if decision is PolicyDecision.REQUIRE_CONFIRMATION and approval_id is None:
                if self.approvals is not None:
                    self.approvals.request(task, proposal, expected, task_lease=task_lease)
                else:
                    update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                        reason='policy:confirmation_required', event_type='agent.action.confirmation_required', actor='policy')
                return

            if not spec.resource_request.empty():
                if task_lease is None or self.resources is None:
                    reason = 'resource:task_lease_required' if task_lease is None else 'resource:broker_unavailable'
                    update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                                 reason=reason, event_type='task.resource_wait')
                    return
                resource_lease = self.resources.acquire(task.task_id, task_lease.owner_id,
                    spec.resource_request, lease_seconds=self.resource_lease_seconds)
                if resource_lease is None:
                    update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                                 reason='resource:unavailable', event_type='task.resource_wait')
                    return
            try:
                op = self.operations.prepare(task, proposal, expected_revisions=expected,
                    task_lease=task_lease, resource_lease=resource_lease, approval_id=approval_id)
                update_owned(self.tasks, task, task_lease, phase=TaskPhase.EXECUTING,
                             event_type='task.executing')
                self.operations.execute(op)
            except BudgetExceeded as exc:
                update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                             reason=f'budget:{exc.reason}', event_type='task.budget_exhausted')
                return
            except ApprovalError:
                raise
            except StateConflict:
                if self.tasks.persistence is not None:
                    self.tasks.refresh(task.task_id)
                if self._control_pending(task):
                    return
                raise
            check_owner(self.tasks, task.task_id, task_lease)
            if self.tasks.persistence is not None:
                self.tasks.refresh(task.task_id)
            if self._control_pending(task):
                return
            if op.state in UNSETTLED:
                update_owned(self.tasks, task, task_lease, state=TaskState.WAITING,
                    phase=TaskPhase.VERIFYING, reason='operation:unresolved', event_type='task.waiting_on_operation')
            elif op.state is OperationState.VERIFIED:
                update_owned(self.tasks, task, task_lease, state=TaskState.RUNNING,
                    phase=TaskPhase.OBSERVING, event_type='task.operation_verified')
            else:
                update_owned(self.tasks, task, task_lease, state=TaskState.RUNNING,
                    phase=TaskPhase.REPLANNING, event_type='task.replanning')
        except (ApprovalError, StaleWorld, PermissionError) as exc:
            # Do not replan or broaden permissions after approval becomes invalid.
            check_owner(self.tasks, task.task_id, task_lease)
            if self.tasks.persistence is not None:
                self.tasks.refresh(task.task_id)
            if not self._control_pending(task):
                update_owned(self.tasks, task, task_lease, state=TaskState.BLOCKED,
                             reason=f'authorization:{exc}', event_type='task.authorization_invalid')
        finally:
            if resource_lease is not None and self.resources is not None:
                if op is None or op.state not in UNSETTLED:
                    self.resources.release(resource_lease)
            self.operations.events.flush_outbox()
