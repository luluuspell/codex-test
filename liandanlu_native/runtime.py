from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import heapq
from threading import RLock
from typing import Any, Protocol

from .capabilities import CapabilityRegistry
from .events import EventStore
from .integrity import (
    BudgetExceeded, LeaseLost, OperationConflict, StateConflict, TERMINAL_TASKS,
    UNSETTLED, copy_record, persist_operation, persist_task, reserve_operation,
    start_operation, unsettled_rows,
)
from .models import (
    ActionProposal, GoalClaim, Operation, OperationState, ResourceLease, Task,
    TaskBudget, TaskLease, TaskPhase, TaskState, new_id, now,
)
from .world import WorldModel


class Capability(Protocol):
    def execute(self, operation: Operation, locators: tuple[str, ...]) -> dict[str, Any]: ...
    def verify(self, operation: Operation, result: dict[str, Any]) -> list[dict[str, Any]]: ...
    def reconcile(self, operation: Operation, locators: tuple[str, ...]) -> tuple[bool, dict[str, Any]]: ...


@dataclass
class TaskScheduler:
    """Heap is only an index. Durable workers must use claim_next, not next_task."""
    tasks: TaskRuntime
    interactive: list[tuple[int, float, str]] = field(default_factory=list)
    background: list[tuple[int, float, str]] = field(default_factory=list)
    _leases: dict[str, TaskLease] = field(default_factory=dict)

    @classmethod
    def rebuild(cls, tasks: TaskRuntime) -> TaskScheduler:
        result = cls(tasks)
        for task in tasks.tasks.values():
            if task.state is TaskState.QUEUED and task.desired_state is TaskState.RUNNING:
                result._push(task)
        return result

    def _push(self, task: Task) -> None:
        item = (-task.priority, task.queued_at if task.queued_at is not None else now(), task.task_id)
        heapq.heappush(self.interactive if task.lane == 'interactive' else self.background, item)

    def submit(self, task_id: str) -> Task:
        task = self.tasks.queue(task_id)
        self._push(task)
        return task

    def _pop_valid(self, heap: list) -> str | None:
        while heap:
            priority, queued, task_id = heapq.heappop(heap)
            task = self.tasks.tasks.get(task_id)
            if (task is not None and task.state is TaskState.QUEUED
                    and task.desired_state is TaskState.RUNNING
                    and task.queued_at == queued and task.priority == -priority):
                return task_id
        return None

    def next_task(self) -> str | None:
        return self._pop_valid(self.interactive) or self._pop_valid(self.background)

    def claim_next(self, owner_id: str, *, lease_seconds: float = 30.0,
                   now_ts: float | None = None) -> TaskLease | None:
        if not owner_id or lease_seconds <= 0:
            raise ValueError('owner_id and a positive lease duration are required')
        if self.tasks.persistence is not None:
            return self.tasks.persistence.claim_next_task(owner_id, lease_seconds=lease_seconds, now_ts=now_ts)
        ts = now() if now_ts is None else now_ts
        eligible = [t for t in self.tasks.tasks.values()
                    if t.state is TaskState.QUEUED and t.desired_state is TaskState.RUNNING
                    and (t.task_id not in self._leases or self._leases[t.task_id].lease_until <= ts)]
        if not eligible:
            return None
        task = min(eligible, key=lambda t: (t.lane != 'interactive', -t.priority, t.queued_at or 0, t.task_id))
        previous = self._leases.get(task.task_id)
        lease = TaskLease(task.task_id, owner_id, previous.generation + 1 if previous else 1, ts, ts + lease_seconds)
        self._leases[task.task_id] = lease
        return lease

    def renew(self, lease: TaskLease, *, lease_seconds: float = 30.0,
              now_ts: float | None = None) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError('lease_seconds must be positive')
        if self.tasks.persistence is not None:
            return self.tasks.persistence.renew_task_lease(lease, lease_seconds=lease_seconds, now_ts=now_ts)
        current = self._leases.get(lease.task_id)
        ts = now() if now_ts is None else now_ts
        if (current is None or current.owner_id != lease.owner_id
                or current.generation != lease.generation or current.lease_until <= ts):
            return None
        result = TaskLease(lease.task_id, lease.owner_id, lease.generation, current.claimed_at, ts + lease_seconds)
        self._leases[lease.task_id] = result
        return result

    def release(self, lease: TaskLease) -> bool:
        if self.tasks.persistence is not None:
            return self.tasks.persistence.release_task_lease(lease)
        current = self._leases.get(lease.task_id)
        if current is None or (current.owner_id, current.generation) != (lease.owner_id, lease.generation):
            return False
        self._leases[lease.task_id] = TaskLease(current.task_id, current.owner_id, current.generation, current.claimed_at, 0)
        return True


@dataclass
class OperationRuntime:
    world: WorldModel
    events: EventStore
    registry: CapabilityRegistry
    capabilities: dict[str, Capability] = field(default_factory=dict)
    operations: dict[str, Operation] = field(default_factory=dict)
    persistence: Any = None
    _lock: Any = field(default_factory=RLock, repr=False)

    def __post_init__(self) -> None:
        if self.persistence is not None and (self.events.persistence is not self.persistence
                                             or self.world.persistence is not self.persistence):
            raise ValueError('World, EventLog and Operations must share one persistence authority')

    def _transition(self, op: Operation, state: OperationState, event_type: str,
                    actor: str = 'engine', payload: dict | None = None) -> None:
        previous = op.state
        op.state = state
        try:
            if self.persistence is not None:
                persist_operation(self.persistence, op, previous, event_type, actor, payload)
            else:
                self.events.enqueue_outbox(event_type=event_type, actor=actor,
                    workspace_id=op.workspace_id, task_id=op.task_id,
                    operation_id=op.operation_id, object_refs=op.object_refs, payload=payload or {})
        except BaseException:
            op.state = previous
            if self.persistence is not None:
                current = self.persistence.load_operations().get(op.operation_id)
                if current is not None:
                    copy_record(op, current)
            raise

    def prepare(self, task: Task, proposal: ActionProposal, *, expected_revisions: dict[str, int],
                task_lease: TaskLease | None = None, resource_lease: ResourceLease | None = None) -> Operation:
        spec = self.registry.resolve(proposal)
        self.world.assert_revisions(expected_revisions, workspace_id=task.workspace_id)
        for ref in proposal.object_refs:
            self.world.assert_access(ref, spec.required_permission, workspace_id=task.workspace_id)
        if task_lease is not None and task_lease.task_id != task.task_id:
            raise LeaseLost('task lease belongs to another task')
        if resource_lease is not None:
            if resource_lease.task_id != task.task_id or resource_lease.request != spec.resource_request:
                raise LeaseLost('resource lease does not match the authoritative request')
            if task_lease is not None and resource_lease.owner_id != task_lease.owner_id:
                raise LeaseLost('task and resource lease owners differ')
            if self.persistence is not None and not self.persistence.validate_resource_lease(resource_lease):
                raise LeaseLost('resource lease is stale, expired or forged')
        elif not spec.resource_request.empty():
            raise LeaseLost('this action requires a resource lease')
        op = Operation(operation_id=new_id('op'), task_id=task.task_id, workspace_id=task.workspace_id,
            capability=proposal.capability, action=proposal.action, object_refs=tuple(proposal.object_refs),
            arguments=deepcopy(proposal.arguments), risk_class=spec.risk_class,
            required_permission=spec.required_permission, idempotency_mode=spec.idempotency_mode.value,
            state=OperationState.PREPARED, expected_revisions=dict(expected_revisions),
            task_lease_owner_id=task_lease.owner_id if task_lease else None,
            task_lease_generation=task_lease.generation if task_lease else None,
            resource_lease_owner_id=resource_lease.owner_id if resource_lease else None,
            resource_lease_generation=resource_lease.generation if resource_lease else None)
        if self.persistence is not None:
            reserve_operation(self.persistence, task, op)
        else:
            if task.state in TERMINAL_TASKS or task.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
                raise StateConflict('task is not allowed to dispatch')
            if any(o.task_id == task.task_id and o.state in UNSETTLED for o in self.operations.values()):
                raise OperationConflict('previous operation remains unresolved')
            reason = task.budget.block_reason(now())
            if reason:
                raise BudgetExceeded(reason)
            task.budget.operations_started += 1
            task.revision += 1
            self.events.enqueue_outbox(event_type='operation.prepared', actor='engine',
                workspace_id=task.workspace_id, task_id=task.task_id, operation_id=op.operation_id,
                object_refs=op.object_refs, payload={'budget_operations_started': task.budget.operations_started})
        self.operations[op.operation_id] = op
        return op

    def _verify(self, op: Operation, capability: Capability) -> Operation:
        if op.result is None:
            self._transition(op, OperationState.UNKNOWN, 'operation.verify_missing_result', 'verifier')
            return op
        self._transition(op, OperationState.VERIFYING, 'operation.verifying', 'verifier')
        try:
            evidence = capability.verify(op, op.result)
            if not isinstance(evidence, list) or not all(isinstance(item, dict) for item in evidence):
                raise TypeError('verifier must return a list of evidence records')
        except Exception as exc:
            op.error = repr(exc)
            self._transition(op, OperationState.VERIFYING, 'operation.verify_error', 'verifier', {'error': op.error})
            return op
        op.evidence = deepcopy(evidence)
        passed = bool(evidence) and all(item.get('status') == 'pass' for item in evidence)
        self._transition(op, OperationState.VERIFIED if passed else OperationState.FAILED,
                         'operation.verified' if passed else 'operation.verification_failed',
                         'verifier', {'evidence': evidence})
        return op

    def execute(self, op: Operation) -> Operation:
        capability = self.capabilities.get(op.capability)
        if capability is None:
            raise OperationConflict(f'capability unavailable: {op.capability}')
        if self.persistence is not None:
            locators = start_operation(self.persistence, op)
        else:
            with self._lock:
                if op.state is not OperationState.PREPARED:
                    raise OperationConflict('operation may only be dispatched from PREPARED')
                self.world.assert_revisions(op.expected_revisions, workspace_id=op.workspace_id)
                locators = tuple(self.world.resolve_locator(ref, op.required_permission,
                                 workspace_id=op.workspace_id) for ref in op.object_refs)
                self._transition(op, OperationState.RUNNING, 'operation.started')
        try:
            result = capability.execute(op, locators)
            if not isinstance(result, dict):
                raise TypeError('adapter result must be a dictionary')
        except Exception as exc:
            op.error = repr(exc)
            self._transition(op, OperationState.UNKNOWN, 'operation.unknown', payload={'error': op.error})
            return op
        op.result = result
        self._transition(op, OperationState.OBSERVED, 'operation.observed')
        return self._verify(op, capability)

    def recover_operation(self, op: Operation) -> Operation:
        if self.persistence is not None:
            current = self.persistence.load_operations().get(op.operation_id)
            if current is not None:
                copy_record(op, current)
        if op.state is OperationState.PREPARED:
            if (self.persistence is not None and op.task_lease_owner_id is not None
                    and self.persistence.validate_task_lease_identity(op.task_id,
                        op.task_lease_owner_id, op.task_lease_generation)):
                return op  # A live owner may still be about to dispatch.
            self._transition(op, OperationState.CANCELLED, 'operation.abandoned_prepared', 'recovery')
            return op
        if op.state not in UNSETTLED:
            return op
        if (op.state is OperationState.RUNNING and self.persistence is not None
                and op.task_lease_owner_id is not None
                and self.persistence.validate_task_lease_identity(op.task_id,
                    op.task_lease_owner_id, op.task_lease_generation)):
            return op  # Do not race a live worker.
        capability = self.capabilities.get(op.capability)
        if capability is None:
            op.error = f'capability unavailable: {op.capability}'
            self._transition(op, OperationState.UNKNOWN, 'operation.provider_unavailable', 'recovery')
            return op
        if op.state in {OperationState.OBSERVED, OperationState.VERIFYING} and op.result is not None:
            return self._verify(op, capability)
        self._transition(op, OperationState.RECONCILING, 'operation.reconciling', 'recovery')
        try:
            locators = tuple(self.world.resolve_locator(ref, op.required_permission,
                             workspace_id=op.workspace_id) for ref in op.object_refs)
            applied, result = capability.reconcile(op, locators)
            if type(applied) is not bool or not isinstance(result, dict):
                raise TypeError('legacy reconciliation must return (bool, dict)')
        except Exception as exc:
            op.error = repr(exc)
            self._transition(op, OperationState.UNKNOWN, 'operation.reconcile_error', 'recovery', {'error': op.error})
            return op
        op.result = result
        if not applied:
            # False does NOT establish that the side effect did not happen.
            self._transition(op, OperationState.UNKNOWN, 'operation.reconcile_inconclusive', 'recovery')
            return op
        self._transition(op, OperationState.OBSERVED, 'operation.reconciled', 'recovery', {'applied': True})
        return self._verify(op, capability)


@dataclass
class TaskRuntime:
    world: WorldModel
    events: EventStore
    tasks: dict[str, Task] = field(default_factory=dict)
    persistence: Any = None
    operations: dict[str, Operation] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.persistence is not None and (self.events.persistence is not self.persistence
                                             or self.world.persistence is not self.persistence):
            raise ValueError('World, EventLog and Tasks must share one persistence authority')

    def refresh(self, task_id: str) -> Task:
        task = self.tasks[task_id]
        if self.persistence is not None:
            current = self.persistence.load_tasks().get(task_id)
            if current is None:
                raise StateConflict('task no longer exists')
            copy_record(task, current)
        return task

    def has_unsettled(self, task_id: str) -> bool:
        if self.persistence is not None:
            return bool(unsettled_rows(self.persistence, task_id))
        return any(o.task_id == task_id and o.state in UNSETTLED for o in self.operations.values())

    def _record(self, task: Task, event_type: str, actor: str = 'engine', payload: dict | None = None) -> None:
        if self.persistence is not None:
            try:
                persist_task(self.persistence, task, event_type, actor, payload)
            except BaseException:
                current = self.persistence.load_tasks().get(task.task_id)
                if current is not None:
                    copy_record(task, current)
                raise
        else:
            self.events.enqueue_outbox(event_type=event_type, actor=actor,
                workspace_id=task.workspace_id, task_id=task.task_id, payload=payload or {})

    def _touch(self, task: Task) -> None:
        task.revision += 1
        self.world.revisions.bump('tasks')

    def create(self, goal: str, success_criteria: tuple[str, ...], *, workspace_id: str = 'default',
               constraints: tuple[str, ...] = (), lane: str = 'background', priority: int = 50,
               budget: TaskBudget | None = None) -> Task:
        if lane not in {'interactive', 'background'}:
            raise ValueError('invalid task lane')
        task = Task(task_id=new_id('task'), workspace_id=workspace_id, goal=goal,
                    success_criteria=success_criteria, constraints=constraints, lane=lane,
                    priority=priority, budget=budget if budget is not None else TaskBudget())
        self._record(task, 'task.created', payload={'goal': goal, 'workspace_id': workspace_id})
        self.tasks[task.task_id] = task
        self.world.revisions.bump('tasks')
        return task

    def queue(self, task_id: str) -> Task:
        task = self.tasks[task_id]
        if task.state in TERMINAL_TASKS or self.has_unsettled(task_id):
            raise StateConflict('terminal or unresolved task cannot be queued')
        task.state = TaskState.QUEUED
        task.desired_state = TaskState.RUNNING
        task.queued_at = now()
        task.wait_reason = None
        self._touch(task)
        self._record(task, 'task.queued', payload={'lane': task.lane, 'priority': task.priority, 'queued_at': task.queued_at})
        return task

    def wait(self, task_id: str, reason: str, *, event_type: str = 'task.waiting') -> Task:
        task = self.tasks[task_id]
        if task.state in TERMINAL_TASKS:
            return task
        task.state = TaskState.WAITING
        task.wait_reason = reason
        self._touch(task)
        self._record(task, event_type, payload={'reason': reason, 'state': task.state.value})
        return task

    def update_runtime_state(self, task_id: str, *, state: TaskState | None = None,
                             phase: TaskPhase | None = None, event_type: str = 'task.state_changed',
                             actor: str = 'engine') -> Task:
        task = self.tasks[task_id]
        if task.state in TERMINAL_TASKS:
            return task
        changed = False
        if state is not None and state != task.state:
            task.state = state
            if state is TaskState.RUNNING:
                task.wait_reason = None
            changed = True
        if phase is not None and phase != task.phase:
            task.phase = phase
            changed = True
        if changed:
            self._touch(task)
            self._record(task, event_type, actor, {'state': task.state.value, 'phase': task.phase.value})
        return task

    def control(self, task_id: str, desired: TaskState) -> Task:
        task = self.tasks[task_id]
        if desired not in {TaskState.RUNNING, TaskState.PAUSED, TaskState.CANCELLED}:
            raise ValueError('unsupported control request')
        if task.state in TERMINAL_TASKS:
            return task
        pending = self.has_unsettled(task_id)
        task.desired_state = desired
        if desired is TaskState.CANCELLED:
            task.state = TaskState.CANCELLING if pending or task.state in {
                TaskState.RUNNING, TaskState.PAUSING, TaskState.CANCELLING} else TaskState.CANCELLED
        elif desired is TaskState.PAUSED:
            task.state = TaskState.PAUSING if pending or task.state is TaskState.RUNNING else TaskState.PAUSED
        elif not pending:
            task.state = TaskState.RUNNING
            task.wait_reason = None
        self._touch(task)
        self._record(task, 'task.control_requested', 'user', {'desired': desired.value, 'actual': task.state.value})
        return task

    def settle_control(self, task_id: str, *, safe_boundary: bool = False,
                       external_cancel_confirmed: bool = False) -> Task:
        task = self.tasks[task_id]
        if task.state in TERMINAL_TASKS or self.has_unsettled(task_id):
            return task
        before = task.state
        if task.desired_state is TaskState.PAUSED and safe_boundary:
            task.state = TaskState.PAUSED
        elif task.desired_state is TaskState.CANCELLED and (safe_boundary or external_cancel_confirmed):
            task.state = TaskState.CANCELLED
        if task.state != before:
            self._touch(task)
            self._record(task, 'task.control_settled', payload={'desired': task.desired_state.value, 'actual': task.state.value})
        return task

    def evaluate_goal(self, task_id: str, claims: dict[str, GoalClaim]) -> Task:
        task = self.tasks[task_id]
        if task.state in TERMINAL_TASKS:
            return task
        if not task.success_criteria:
            raise ValueError('a goal requires explicit success criteria')
        missing = [criterion for criterion in task.success_criteria
                   if criterion not in claims or claims[criterion].criterion != criterion
                   or not claims[criterion].passed or not claims[criterion].evidence_refs]
        if self.has_unsettled(task_id):
            missing.append('unsettled_operation')
        task.state = TaskState.RUNNING if missing else TaskState.COMPLETED
        if missing:
            task.phase = TaskPhase.REPLANNING
        self._touch(task)
        self._record(task, 'task.goal_evaluated', 'verifier', {'missing': missing, 'state': task.state.value})
        return task
