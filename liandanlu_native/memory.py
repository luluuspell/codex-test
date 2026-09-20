from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .models import new_id


class MemoryKind(str, Enum):
    FACT = "FACT"
    EPISODE = "EPISODE"
    STRATEGY = "STRATEGY"


class StrategyState(str, Enum):
    CANDIDATE = "CANDIDATE"
    SUPPORTED = "SUPPORTED"
    PROMOTED = "PROMOTED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class MemoryCandidate:
    kind: MemoryKind
    key: str
    value: Any
    scope: str
    source_event_ids: tuple[str, ...]
    confidence: float
    explicit_user_statement: bool = False


@dataclass(slots=True)
class MemoryRecord:
    memory_id: str
    kind: MemoryKind
    key: str
    value: Any
    scope: str
    source_event_ids: tuple[str, ...]
    confidence: float
    revision: int
    supersedes: str | None = None
    strategy_state: StrategyState | None = None
    support_count: int = 1


@dataclass
class MemoryStore:
    records: dict[str, MemoryRecord] = field(default_factory=dict)
    latest_by_key: dict[tuple[str, str, MemoryKind], str] = field(default_factory=dict)

    def commit(self, candidate: MemoryCandidate) -> MemoryRecord | None:
        if not candidate.source_event_ids:
            return None
        if candidate.scope == "global" and not candidate.explicit_user_statement:
            return None
        key = (candidate.scope, candidate.key, candidate.kind)
        previous_id = self.latest_by_key.get(key)
        previous = self.records.get(previous_id) if previous_id else None
        revision = 1 if previous is None else previous.revision + 1
        state = StrategyState.CANDIDATE if candidate.kind is MemoryKind.STRATEGY else None
        record = MemoryRecord(
            memory_id=new_id("mem"), kind=candidate.kind, key=candidate.key, value=candidate.value,
            scope=candidate.scope, source_event_ids=candidate.source_event_ids,
            confidence=candidate.confidence, revision=revision,
            supersedes=previous.memory_id if previous else None,
            strategy_state=state,
        )
        self.records[record.memory_id] = record
        self.latest_by_key[key] = record.memory_id
        return record

    def add_strategy_support(self, memory_id: str, *, independent_evidence: bool = True, user_confirmed: bool = False) -> MemoryRecord:
        record = self.records[memory_id]
        if record.kind is not MemoryKind.STRATEGY:
            raise ValueError("not a strategy memory")
        if independent_evidence:
            record.support_count += 1
        if user_confirmed or record.support_count >= 3:
            record.strategy_state = StrategyState.PROMOTED
        elif record.support_count >= 2:
            record.strategy_state = StrategyState.SUPPORTED
        return record

    def latest(self, scope: str, key: str, kind: MemoryKind) -> MemoryRecord | None:
        memory_id = self.latest_by_key.get((scope, key, kind))
        return self.records.get(memory_id) if memory_id else None
