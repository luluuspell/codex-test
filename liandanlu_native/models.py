from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import time
import uuid


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def now() -> float:
    return time.time()


class TaskState(str, Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskPhase(str, Enum):
    OBSERVING = "OBSERVING"
    THINKING = "THINKING"
    AUTHORIZING = "AUTHORIZING"
    EXECUTING = "EXECUTING"
    VERIFYING = "VERIFYING"
    REPLANNING = "REPLANNING"


class OperationState(str, Enum):
    PROPOSED = "PROPOSED"
    AUTHORIZED = "AUTHORIZED"
    PREPARED = "PREPARED"
    RUNNING = "RUNNING"
    OBSERVED = "OBSERVED"
    VERIFYING = "VERIFYING"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class RiskClass(str, Enum):
    READ = "READ"
    REVERSIBLE = "REVERSIBLE"
    MUTATING = "MUTATING"
    EXTERNAL = "EXTERNAL"
    DESTRUCTIVE = "DESTRUCTIVE"


@dataclass(slots=True)
class WorldRevisions:
    global_revision: int = 0
    desktop: int = 0
    workspace: int = 0
    browser: int = 0
    media: int = 0
    tasks: int = 0

    def bump(self, domain: str) -> None:
        if domain == "global_revision" or not hasattr(self, domain):
            raise KeyError(domain)
        setattr(self, domain, getattr(self, domain) + 1)
        self.global_revision += 1


@dataclass(slots=True)
class TaskBudget:
    max_operations: int = 64
    operations_started: int = 0
    deadline_at: float | None = None

    def block_reason(self, at: float) -> str | None:
        if self.max_operations >= 0 and self.operations_started >= self.max_operations:
            return "max_operations"
        if self.deadline_at is not None and at >= self.deadline_at:
            return "deadline"
        return None


@dataclass(frozen=True, slots=True)
class TaskLease:
    task_id: str
    owner_id: str
    generation: int
    claimed_at: float
    lease_until: float


@dataclass(slots=True)
class Entity:
    entity_id: str
    entity_type: str
    workspace_id: str
    locator: str
    version: int = 1
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    permissions: frozenset[str] = frozenset({"read"})


@dataclass(slots=True)
class Task:
    task_id: str
    workspace_id: str
    goal: str
    success_criteria: tuple[str, ...]
    constraints: tuple[str, ...] = ()
    state: TaskState = TaskState.CREATED
    desired_state: TaskState = TaskState.CREATED
    phase: TaskPhase = TaskPhase.OBSERVING
    priority: int = 50
    lane: str = "background"
    queued_at: float | None = None
    wait_reason: str | None = None
    budget: TaskBudget = field(default_factory=TaskBudget)
    revision: int = 1


@dataclass(slots=True)
class Operation:
    operation_id: str
    task_id: str
    workspace_id: str
    capability: str
    action: str
    object_refs: tuple[str, ...]
    arguments: dict[str, Any]
    risk_class: RiskClass
    required_permission: str = "read"
    idempotency_mode: str = "RECONCILABLE"
    state: OperationState = OperationState.PROPOSED
    expected_revisions: dict[str, int] = field(default_factory=dict)
    evidence: list[dict[str, Any]] = field(default_factory=list)
    result: dict[str, Any] | None = None
    error: str | None = None


@dataclass(slots=True)
class Event:
    sequence: int
    event_id: str
    workspace_id: str
    event_type: str
    actor: str
    task_id: str | None
    operation_id: str | None
    object_refs: tuple[str, ...]
    payload: dict[str, Any]
    causation_id: str | None
    correlation_id: str | None
    occurred_at: float
    observed_at: float
    learning_allowed: bool = True


@dataclass(slots=True)
class ActionProposal:
    proposal_id: str
    capability: str
    action: str
    object_refs: tuple[str, ...]
    arguments: dict[str, Any] = field(default_factory=dict)
    expected_outcome: str = ""
    confidence: float = 1.0


@dataclass(frozen=True, slots=True)
class GoalClaim:
    criterion: str
    passed: bool
    evidence_refs: tuple[str, ...]
