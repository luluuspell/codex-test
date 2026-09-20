from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol, Any

from .events import EventStore
from .models import ActionProposal, Operation, OperationState, RiskClass, Task, TaskPhase, TaskState, new_id
from .world import WorldModel, StaleWorld


class Capability(Protocol):
    def execute(self, operation: Operation, locators: tuple[str, ...]) -> dict[str, Any]: ...
    def verify(self, operation: Operation, result: dict[str, Any]) -> list[dict[str, Any]]: ...
    def reconcile(self, operation: Operation, locators: tuple[str, ...]) -> tuple[bool, dict[str, Any]]: ...


@dataclass
class TaskScheduler:
    interactive: list[str] = field(default_factory=list)
    background: list[str] = field(default_factory=list)

    def submit(self, task: Task) -> None:
        task.state = TaskState.QUEUED
        task.desired_state = TaskState.RUNNING
        (self.interactive if task.lane == "interactive" else self.background).append(task.task_id)

    def next_task(self) -> str | None:
        if self.interactive:
            return self.interactive.pop(0)
        if self.background:
            return self.background.pop(0)
        return None


@dataclass
class OperationRuntime:
    world: WorldModel
    events: EventStore
    capabilities: dict[str, Capability] = field(default_factory=dict)
    operations: dict[str, Operation] = field(default_factory=dict)

    def prepare(self, task: Task, proposal: ActionProposal, *, risk: RiskClass, expected_revisions: dict[str, int]) -> Operation:
        self.world.assert_revisions(expected_revisions)
        op = Operation(operation_id=new_id("op"), task_id=task.task_id, capability=proposal.capability, action=proposal.action, object_refs=proposal.object_refs, arguments=proposal.arguments, risk_class=risk, state=OperationState.PREPARED, expected_revisions=dict(expected_revisions))
        self.operations[op.operation_id] = op
        self.events.enqueue_outbox(event_type="operation.prepared", actor="engine", task_id=task.task_id, operation_id=op.operation_id, object_refs=op.object_refs)
        return op

    def execute(self, op: Operation, *, permission: str = "read") -> Operation:
        capability = self.capabilities[op.capability]
        self.world.assert_revisions(op.expected_revisions)
        locators = tuple(self.world.resolve_locator(ref, permission) for ref in op.object_refs)
        op.state = OperationState.RUNNING
        try:
            result = capability.execute(op, locators)
        except Exception as exc:
            op.state = OperationState.UNKNOWN
            op.error = repr(exc)
            self.events.enqueue_outbox(event_type="operation.unknown", actor="engine", task_id=op.task_id, operation_id=op.operation_id, object_refs=op.object_refs, payload={"error": op.error})
            return op
        op.result = result
        op.state = OperationState.OBSERVED
        op.state = OperationState.VERIFYING
        evidence = capability.verify(op, result)
        op.evidence.extend(evidence)
        if evidence and all(x.get("status") == "pass" for x in evidence):
            op.state = OperationState.VERIFIED
            self.events.enqueue_outbox(event_type="operation.verified", actor="verifier", task_id=op.task_id, operation_id=op.operation_id, object_refs=op.object_refs, payload={"evidence": evidence})
        else:
            op.state = OperationState.FAILED
            self.events.enqueue_outbox(event_type="operation.verification_failed", actor="verifier", task_id=op.task_id, operation_id=op.operation_id, object_refs=op.object_refs, payload={"evidence": evidence})
        return op

    def recover_operation(self, op: Operation, *, permission: str = "read") -> Operation:
        if op.state not in {OperationState.RUNNING, OperationState.UNKNOWN, OperationState.PREPARED}:
            return op
        op.state = OperationState.RECONCILING
        capability = self.capabilities[op.capability]
        locators = tuple(self.world.resolve_locator(ref, permission) for ref in op.object_refs)
        verified, result = capability.reconcile(op, locators)
        op.result = result
        op.state = OperationState.VERIFIED if verified else OperationState.FAILED
        self.events.enqueue_outbox(event_type="operation.reconciled", actor="recovery", task_id=op.task_id, operation_id=op.operation_id, object_refs=op.object_refs, payload={"verified": verified})
        return op


@dataclass
class TaskRuntime:
    world: WorldModel
    events: EventStore
    tasks: dict[str, Task] = field(default_factory=dict)

    def create(self, goal: str, success_criteria: tuple[str, ...], *, constraints: tuple[str, ...] = (), lane: str = "background", priority: int = 50) -> Task:
        task = Task(task_id=new_id("task"), goal=goal, success_criteria=success_criteria, constraints=constraints, lane=lane, priority=priority)
        self.tasks[task.task_id] = task
        self.events.enqueue_outbox(event_type="task.created", actor="engine", task_id=task.task_id, payload={"goal": goal})
        return task

    def control(self, task_id: str, desired: TaskState) -> Task:
        task = self.tasks[task_id]
        task.desired_state = desired
        task.revision += 1
        if desired == TaskState.PAUSED and task.state not in {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED}:
            task.state = TaskState.PAUSED
        elif desired == TaskState.CANCELLED and task.state not in {TaskState.COMPLETED, TaskState.FAILED}:
            task.state = TaskState.CANCELLED
        elif desired == TaskState.RUNNING and task.state in {TaskState.PAUSED, TaskState.WAITING, TaskState.QUEUED, TaskState.CREATED}:
            task.state = TaskState.RUNNING
        self.world.revisions.bump("tasks")
        self.events.enqueue_outbox(event_type="task.controlled", actor="user", task_id=task_id, payload={"desired": desired.value, "actual": task.state.value})
        return task

    def evaluate_goal(self, task_id: str, claims: dict[str, bool]) -> Task:
        task = self.tasks[task_id]
        missing = [criterion for criterion in task.success_criteria if not claims.get(criterion, False)]
        if missing:
            task.state = TaskState.RUNNING
            task.phase = TaskPhase.REPLANNING
        else:
            task.state = TaskState.COMPLETED
        task.revision += 1
        self.world.revisions.bump("tasks")
        self.events.enqueue_outbox(event_type="task.goal_evaluated", actor="verifier", task_id=task_id, payload={"missing": missing, "state": task.state.value})
        return task
