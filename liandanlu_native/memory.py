from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from .models import Event, new_id


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


class MemoryPersistence(Protocol):
    def load_memory_records(self) -> list[MemoryRecord]: ...
    def process_memory_event(
        self, event: Event, candidate: MemoryCandidate | None
    ) -> MemoryRecord | None: ...
    def save_memory_record(self, record: MemoryRecord) -> None: ...


@dataclass
class MemoryStore:
    persistence: MemoryPersistence | None = None
    records: dict[str, MemoryRecord] = field(default_factory=dict)
    latest_by_key: dict[tuple[str, str, MemoryKind], str] = field(default_factory=dict)
    processed_event_ids: set[str] = field(default_factory=set)

    @classmethod
    def from_persistence(cls, persistence: MemoryPersistence) -> "MemoryStore":
        store = cls(persistence=persistence)
        for record in persistence.load_memory_records():
            store.records[record.memory_id] = record
            key = (record.scope, record.key, record.kind)
            current_id = store.latest_by_key.get(key)
            current = store.records.get(current_id) if current_id else None
            if current is None or record.revision > current.revision:
                store.latest_by_key[key] = record.memory_id
        return store

    def _validate(self, candidate: MemoryCandidate) -> bool:
        if not candidate.source_event_ids:
            return False
        if not 0.0 <= candidate.confidence <= 1.0:
            return False
        if candidate.scope == "global" and not candidate.explicit_user_statement:
            return False
        return True

    def commit(
        self,
        candidate: MemoryCandidate,
        *,
        source_event: Event | None = None,
    ) -> MemoryRecord | None:
        if not self._validate(candidate):
            return None
        if source_event is not None and self.persistence is not None:
            record = self.persistence.process_memory_event(source_event, candidate)
            if record is not None:
                self.records[record.memory_id] = record
                self.latest_by_key[(record.scope, record.key, record.kind)] = record.memory_id
            return record

        key = (candidate.scope, candidate.key, candidate.kind)
        previous_id = self.latest_by_key.get(key)
        previous = self.records.get(previous_id) if previous_id else None
        revision = 1 if previous is None else previous.revision + 1
        state = StrategyState.CANDIDATE if candidate.kind is MemoryKind.STRATEGY else None
        record = MemoryRecord(
            memory_id=new_id("mem"), kind=candidate.kind, key=candidate.key,
            value=candidate.value, scope=candidate.scope,
            source_event_ids=candidate.source_event_ids,
            confidence=candidate.confidence, revision=revision,
            supersedes=previous.memory_id if previous else None,
            strategy_state=state,
        )
        self.records[record.memory_id] = record
        self.latest_by_key[key] = record.memory_id
        return record

    def process_ignored_event(self, event: Event) -> None:
        if self.persistence is not None:
            self.persistence.process_memory_event(event, None)
        else:
            self.processed_event_ids.add(event.event_id)

    def add_strategy_support(
        self,
        memory_id: str,
        *,
        independent_evidence: bool = True,
        user_confirmed: bool = False,
    ) -> MemoryRecord:
        record = self.records[memory_id]
        if record.kind is not MemoryKind.STRATEGY:
            raise ValueError("not a strategy memory")
        if independent_evidence:
            record.support_count += 1
        if user_confirmed or record.support_count >= 3:
            record.strategy_state = StrategyState.PROMOTED
        elif record.support_count >= 2:
            record.strategy_state = StrategyState.SUPPORTED
        if self.persistence is not None:
            self.persistence.save_memory_record(record)
        return record

    def latest(self, scope: str, key: str, kind: MemoryKind) -> MemoryRecord | None:
        memory_id = self.latest_by_key.get((scope, key, kind))
        return self.records.get(memory_id) if memory_id else None


@dataclass
class MemoryPipeline:
    events: Any
    memory: MemoryStore
    consumer_id: str = "memory"

    def _candidate_from_event(self, event: Event) -> MemoryCandidate | None:
        if event.event_type != "memory.candidate" or not event.learning_allowed:
            return None
        payload = event.payload
        try:
            kind = MemoryKind(str(payload["kind"]))
            key = str(payload["key"])
            scope = str(payload["scope"])
            confidence = float(payload.get("confidence", 1.0))
        except (KeyError, TypeError, ValueError):
            return None
        if not key or not scope:
            return None
        return MemoryCandidate(
            kind=kind,
            key=key,
            value=payload.get("value"),
            scope=scope,
            source_event_ids=(event.event_id,),
            confidence=confidence,
            explicit_user_statement=bool(payload.get("explicit_user_statement", False)),
        )

    def run_once(self, *, limit: int = 100) -> dict[str, int]:
        scanned = committed = ignored = 0
        for event in self.events.read_after(self.consumer_id)[:limit]:
            scanned += 1
            candidate = self._candidate_from_event(event)
            if candidate is None:
                self.memory.process_ignored_event(event)
                ignored += 1
            else:
                record = self.memory.commit(candidate, source_event=event)
                if record is None:
                    self.memory.process_ignored_event(event)
                    ignored += 1
                else:
                    committed += 1
            self.events.ack(self.consumer_id, event.sequence)
        return {"scanned": scanned, "committed": committed, "ignored": ignored}
