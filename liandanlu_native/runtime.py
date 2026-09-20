from __future__ import annotations

from dataclasses import dataclass, field
import heapq
from typing import Protocol, Any

from .capabilities import CapabilityRegistry
from .events import EventStore
from .models import (
    ActionProposal, GoalClaim, Operation, OperationState, Task, TaskBudget,
    TaskLease, TaskPhase, TaskState, new_id, now,
)
from .world import WorldModel


class Capability(Protocol):
    def execute(self, operation: Operation, locators: tuple[str, ...]) -> dict[str, Any]: ...
    def verify(self, operation: Operation, result: dict[str, Any]) -> list[dict[str, Any]]: ...
    def reconcile(self, operation: Operation, locators: tuple[str, ...]) -> tuple[bool, dict[str, Any]]: ...


class RuntimePersistence(Protocol):
    def save_task(self, task: Task) -> None: ...
    def save_task_with_outbox_event(self, task: Task, **event_kwargs) -> str: ...
    def save_operation(self, op: Operation) -> None: ...
    def save_operation_with_outbox_event(self, op: Operation, **event_kwargs) -> str: ...
    def save_task_and_operation_with_outbox_event(
        self, task: Task, op: Operation, **event_kwargs
    ) -> str: ...
    def claim_next_task(
        self, owner_id: str, *, lease_seconds: float, now_ts: float | None = None
    ) -> TaskLease | None: ...
    def renew_task_lease(
        self, lease: TaskLease, *, lease_seconds: float, now_ts: float | None = None
    ) -> TaskLease | None: ...
    def release_task_lease(self, lease: TaskLease) -> bool: ...


class BudgetExceeded(RuntimeError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class TaskScheduler:
    """Durable queue projection.

    TaskRuntime/SQLite remain authoritative. The heaps are rebuildable indexes,
    not another source of task truth.
    """
    tasks: "TaskRuntime"
    interactive: list[tuple[int, float, str]] = field(default_factory=list)
    background: list[tuple[int, float, str]] = field(default_factory=list)
    _leases: dict[str, TaskLease] = field(default_factory=dict)

    @classmethod
    def rebuild(cls, tasks: "TaskRuntime") -> "TaskScheduler":
        scheduler = cls(tasks)
        for task in tasks.tasks.values():
            if (
                task.state is TaskState.QUEUED
                and task.desired_state is TaskState.RUNNING
                and task.queued_at is not None
            ):
                scheduler._push(task)
        return scheduler

    def _push(self, task: Task) -> None:
        queued_at = task.queued_at if task.queued_at is not None else now()
        item = (-task.priority, queued_at, task.task_id)
        heapq.heappush(
            self.interactive if task.lane == "interactive" else self.background,
            item,
        )

    def submit(self, task_id: str) -> Task:
        task = self.tasks.queue(task_id)
        self._push(task)
        return task

    def _pop_valid(self, heap: list[tuple[int, float, str]]) -> str | None:
        while heap:
            _, _, task_id = heapq.heappop(heap)
            task = self.tasks.tasks.get(task_id)
            if (
                task is not None
                and task.state is TaskState.QUEUED
                and task.desired_state is TaskState.RUNNING
            ):
                return task_id
        return None

    def next_task(self) -> str | None:
        task_id = self._pop_valid(self.interactive)
        if task_id is not None:
            return task_id
        return self._pop_valid(self.background)


    def claim_next(
        self,
        owner_id: str,
        *,
        lease_seconds: float = 30.0,
        now_ts: float | None = None,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if self.tasks.persistence is not None:
            return self.tasks.persistence.claim_next_task(
                owner_id, lease_seconds=lease_seconds, now_ts=now_ts
            )
        task_id = self.next_task()
        if task_id is None:
            return None
        ts = now() if now_ts is None else now_ts
        previous = self._leases.get(task_id)
        generation = 1 if previous is None else previous.generation + 1
        lease = TaskLease(task_id, owner_id, generation, ts, ts + lease_seconds)
        self._leases[task_id] = lease
        return lease

    def renew(
        self,
        lease: TaskLease,
        *,
        lease_seconds: float = 30.0,
        now_ts: float | None = None,
    ) -> TaskLease | None:
        if self.tasks.persistence is not None:
            return self.tasks.persistence.renew_task_lease(
                lease, lease_seconds=lease_seconds, now_ts=now_ts
            )
        current = self._leases.get(lease.task_id)
        ts = now() if now_ts is None else now_ts
        if (
            current is None
            or current.owner_id != lease.owner_id
            or current.generation != lease.generation
            or current.lease_until <= ts
        ):
            return None
        renewed = TaskLease(
            lease.task_id, lease.owner_id, lease.generation,
            lease.claimed_at, ts + lease_seconds,
        )
        self._leases[lease.task_id] = renewed
        return renewed

    def release(self, lease: TaskLease) -> bool:
        if self.tasks.persistence is not None:
            return self.tasks.persistence.release_task_lease(lease)
        current = self._leases.get(lease.task_id)
        if current is None:
            return False
        if current.owner_id != lease.owner_id or current.generation != lease.generation:
            return False
        del self._leases[lease.task_id]
        return True


@dataclass
class OperationRuntime:
    world: WorldModel
    events: EventStore
    registry: CapabilityRegistry
    capabilities: dict[str, Capability] = field(default_factory=dict)
    operations: dict[str, Operation] = field(default_factory=dict)
    persistence: RuntimePersistence | None = None

    def __post_init__(self) -> None:
        if self.persistence is not None:
            if self.events.persistence is not self.persistence:
                raise ValueError("EventStore and OperationRuntime must share one persistence authority")
            if self.world.persistence is not self.persistence:
                raise ValueError("WorldModel and OperationRuntime must share one persistence authority")

    def _save(self, op: Operation) -> None:
        if self.persistence:
            self.persistence.save_operation(op)

    def _record(self, op: Operation, event_type: str, actor: str, payload: dict[str, Any] | None = None) -> None:
        if self.persistence:
            self.persistence.save_operation_with_outbox_event(
                op, event_type=event_type, actor=actor,
                workspace_id=op.workspace_id, payload=payload or {}
            )
        else:
            self.events.enqueue_outbox(
                event_type=event_type, actor=actor, workspace_id=op.workspace_id,
                task_id=op.task_id, operation_id=op.operation_id, object_refs=op.object_refs,
                payload=payload or {},
            )

    def prepare(self, task: Task, proposal: ActionProposal, *, expected_revisions: dict[str, int]) -> Operation:
        spec = self.registry.resolve(proposal)
        self.world.assert_revisions(
            expected_revisions, workspace_id=task.workspace_id
        )
        for ref in proposal.object_refs:
            self.world.assert_access(
                ref, spec.required_permission, workspace_id=task.workspace_id
            )
        op = Operation(
            operation_id=new_id("op"), task_id=task.task_id,
            workspace_id=task.workspace_id,
            capability=proposal.capability, action=proposal.action,
            object_refs=proposal.object_refs, arguments=proposal.arguments,
            risk_class=spec.risk_class,
            required_permission=spec.required_permission,
            idempotency_mode=spec.idempotency_mode.value,
            state=OperationState.PREPARED,
            expected_revisions=dict(expected_revisions),
        )
        reason = task.budget.block_reason(now())
        if reason is not None:
            raise BudgetExceeded(reason)
        task.budget.operations_started += 1
        task.revision += 1
        self.world.revisions.bump("tasks")
        if self.world.persistence:
            self.world.persistence.save_world_revisions(self.world.revisions)

        self.operations[op.operation_id] = op
        if self.persistence:
            self.persistence.save_task_and_operation_with_outbox_event(
                task, op, event_type="operation.prepared", actor="engine",
                workspace_id=op.workspace_id,
                payload={
                    "budget_operations_started": task.budget.operations_started,
                    "budget_max_operations": task.budget.max_operations,
                },
            )
        else:
            self.events.enqueue_outbox(
                event_type="operation.prepared", actor="engine",
                workspace_id=op.workspace_id, task_id=op.task_id,
                operation_id=op.operation_id, object_refs=op.object_refs,
                payload={
                    "budget_operations_started": task.budget.operations_started,
                    "budget_max_operations": task.budget.max_operations,
                },
            )
        return op

    def _verify(self, op: Operation, capability: Capability) -> Operation:
        if op.result is None:
            op.state = OperationState.UNKNOWN
            self._record(op, "operation.verify_missing_result", "verifier")
            return op
        op.state = OperationState.VERIFYING
        self._save(op)
        try:
            evidence = capability.verify(op, op.result)
        except Exception as exc:
            op.error = repr(exc)
            self._record(op, "operation.verify_error", "verifier", {"error": op.error})
            return op
        op.evidence.extend(evidence)
        if evidence and all(item.get("status") == "pass" for item in evidence):
            op.state = OperationState.VERIFIED
            event_type = "operation.verified"
        else:
            op.state = OperationState.FAILED
            event_type = "operation.verification_failed"
        self._record(op, event_type, "verifier", {"evidence": evidence})
        return op

    def execute(self, op: Operation) -> Operation:
        capability = self.capabilities[op.capability]
        self.world.assert_revisions(
            op.expected_revisions, workspace_id=op.workspace_id
        )
        locators = tuple(
            self.world.resolve_locator(
                ref, op.required_permission, workspace_id=op.workspace_id
            )
            for ref in op.object_refs
        )
        op.state = OperationState.RUNNING
        self._record(op, "operation.started", "engine")
        try:
            result = capability.execute(op, locators)
        except Exception as exc:
            op.state = OperationState.UNKNOWN
            op.error = repr(exc)
            self._record(op, "operation.unknown", "engine", {"error": op.error})
            return op
        op.result = result
        op.state = OperationState.OBSERVED
        self._save(op)
        return self._verify(op, capability)

    def recover_operation(self, op: Operation) -> Operation:
        if op.state is OperationState.PREPARED:
            op.state = OperationState.CANCELLED
            self._record(
                op, "operation.abandoned_prepared", "recovery",
                {"reason": "no external execution was recorded before restart"},
            )
            return op
        capability = self.capabilities[op.capability]
        if op.state in {OperationState.OBSERVED, OperationState.VERIFYING} and op.result is not None:
            return self._verify(op, capability)
        if op.state not in {
            OperationState.RUNNING, OperationState.UNKNOWN, OperationState.RECONCILING
        }:
            return op
        op.state = OperationState.RECONCILING
        self._record(op, "operation.reconciling", "recovery")
        try:
            locators = tuple(
                self.world.resolve_locator(
                    ref, op.required_permission, workspace_id=op.workspace_id
                )
                for ref in op.object_refs
            )
            verified, result = capability.reconcile(op, locators)
        except Exception as exc:
            op.state = OperationState.UNKNOWN
            op.error = repr(exc)
            self._record(op, "operation.reconcile_error", "recovery", {"error": op.error})
            return op
        op.result = result
        op.state = OperationState.VERIFIED if verified else OperationState.FAILED
        self._record(op, "operation.reconciled", "recovery", {"verified": verified})
        return op


@dataclass
class TaskRuntime:
    world: WorldModel
    events: EventStore
    tasks: dict[str, Task] = field(default_factory=dict)
    persistence: RuntimePersistence | None = None

    def __post_init__(self) -> None:
        if self.persistence is not None:
            if self.events.persistence is not self.persistence:
                raise ValueError("EventStore and TaskRuntime must share one persistence authority")
            if self.world.persistence is not self.persistence:
                raise ValueError("WorldModel and TaskRuntime must share one persistence authority")

    def _record(self, task: Task, event_type: str, actor: str, payload: dict[str, Any] | None = None) -> None:
        if self.persistence:
            self.persistence.save_task_with_outbox_event(
                task, event_type=event_type, actor=actor,
                workspace_id=task.workspace_id, payload=payload or {}
            )
        else:
            self.events.enqueue_outbox(
                event_type=event_type, actor=actor, workspace_id=task.workspace_id,
                task_id=task.task_id, payload=payload or {},
            )

    def _touch(self, task: Task) -> None:
        task.revision += 1
        self.world.revisions.bump("tasks")
        if self.world.persistence:
            self.world.persistence.save_world_revisions(self.world.revisions)

    def create(
        self,
        goal: str,
        success_criteria: tuple[str, ...],
        *,
        workspace_id: str = "default",
        constraints: tuple[str, ...] = (),
        lane: str = "background",
        priority: int = 50,
        budget: TaskBudget | None = None,
    ) -> Task:
        task = Task(
            task_id=new_id("task"), workspace_id=workspace_id, goal=goal,
            success_criteria=success_criteria, constraints=constraints,
            lane=lane, priority=priority,
            budget=budget if budget is not None else TaskBudget(),
        )
        self.tasks[task.task_id] = task
        self.world.revisions.bump("tasks")
        if self.world.persistence:
            self.world.persistence.save_world_revisions(self.world.revisions)
        self._record(task, "task.created", "engine", {"goal": goal, "workspace_id": workspace_id})
        return task

    def queue(self, task_id: str) -> Task:
        task = self.tasks[task_id]
        if task.state in {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED}:
            raise ValueError("terminal task cannot be queued")
        task.state = TaskState.QUEUED
        task.desired_state = TaskState.RUNNING
        task.queued_at = now()
        self._touch(task)
        self._record(
            task, "task.queued", "engine",
            {
                "lane": task.lane,
                "priority": task.priority,
                "queued_at": task.queued_at,
            },
        )
        return task

    def wait(self, task_id: str, reason: str, *, event_type: str = "task.waiting") -> Task:
        task = self.tasks[task_id]
        task.state = TaskState.WAITING
        task.wait_reason = reason
        self._touch(task)
        self._record(
            task, event_type, "engine",
            {"reason": reason, "state": task.state.value},
        )
        return task

    def update_runtime_state(self, task_id: str, *, state: TaskState | None = None, phase: TaskPhase | None = None, event_type: str = "task.state_changed", actor: str = "engine") -> Task:
        task = self.tasks[task_id]
        changed = False
        if state is not None and task.state != state:
            task.state = state
            changed = True
        if phase is not None and task.phase != phase:
            task.phase = phase
            changed = True
        if changed:
            self._touch(task)
            self._record(
                task, event_type, actor,
                {"state": task.state.value, "phase": task.phase.value},
            )
        return task

    def control(self, task_id: str, desired: TaskState) -> Task:
        task = self.tasks[task_id]
        if task.state in {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED}:
            return task
        task.desired_state = desired
        if desired == TaskState.PAUSED:
            if task.state == TaskState.RUNNING:
                task.state = TaskState.PAUSING
            elif task.state not in {TaskState.CANCELLING, TaskState.CANCELLED}:
                task.state = TaskState.PAUSED
        elif desired == TaskState.CANCELLED:
            if task.state in {TaskState.RUNNING, TaskState.PAUSING}:
                task.state = TaskState.CANCELLING
            else:
                task.state = TaskState.CANCELLED
        elif desired == TaskState.RUNNING:
            task.wait_reason = None
            if task.state in {
                TaskState.PAUSED, TaskState.PAUSING, TaskState.WAITING,
                TaskState.QUEUED, TaskState.CREATED,
            }:
                task.state = TaskState.RUNNING
        self._touch(task)
        self._record(
            task, "task.control_requested", "user",
            {"desired": desired.value, "actual": task.state.value},
        )
        return task

    def settle_control(self, task_id: str, *, safe_boundary: bool = False, external_cancel_confirmed: bool = False) -> Task:
        task = self.tasks[task_id]
        before = task.state
        if task.desired_state == TaskState.PAUSED and safe_boundary:
            task.state = TaskState.PAUSED
        elif (
            task.desired_state == TaskState.CANCELLED
            and (safe_boundary or external_cancel_confirmed)
        ):
            task.state = TaskState.CANCELLED
        if task.state != before:
            self._touch(task)
            self._record(
                task, "task.control_settled", "engine",
                {"desired": task.desired_state.value, "actual": task.state.value},
            )
        return task

    def evaluate_goal(self, task_id: str, claims: dict[str, GoalClaim]) -> Task:
        task = self.tasks[task_id]
        missing: list[str] = []
        for criterion in task.success_criteria:
            claim = claims.get(criterion)
            if claim is None or not claim.passed or not claim.evidence_refs:
                missing.append(criterion)
        task.state = TaskState.RUNNING if missing else TaskState.COMPLETED
        task.phase = TaskPhase.REPLANNING if missing else task.phase
        self._touch(task)
        self._record(
            task, "task.goal_evaluated", "verifier",
            {"missing": missing, "state": task.state.value},
        )
        return task
