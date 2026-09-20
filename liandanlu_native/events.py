from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .models import Event, new_id, now


class EventPersistence(Protocol):
    def enqueue_event_outbox(self, **kwargs) -> str: ...
    def dispatch_event_outbox(self, limit: int = 100) -> list[Event]: ...
    def read_events_after(self, consumer_id: str) -> list[Event]: ...
    def ack_event_consumer(self, consumer_id: str, sequence: int) -> None: ...


@dataclass
class EventStore:
    """Event facade.

    With a persistence backend, the durable outbox/event tables are the only authority.
    The in-memory lists exist only for isolated unit tests without persistence.
    """
    persistence: EventPersistence | None = None
    _events: list[Event] = field(default_factory=list)
    _outbox: list[dict[str, Any]] = field(default_factory=list)
    _cursors: dict[str, int] = field(default_factory=dict)

    def enqueue_outbox(self, *, event_type: str, actor: str, workspace_id: str = "system", task_id: str | None = None, operation_id: str | None = None, object_refs: tuple[str, ...] = (), payload: dict[str, Any] | None = None, causation_id: str | None = None, correlation_id: str | None = None, learning_allowed: bool = True) -> str:
        if self.persistence:
            return self.persistence.enqueue_event_outbox(
                event_type=event_type, actor=actor, workspace_id=workspace_id, task_id=task_id,
                operation_id=operation_id, object_refs=object_refs,
                payload=payload or {}, causation_id=causation_id,
                correlation_id=correlation_id, learning_allowed=learning_allowed,
            )
        outbox_id = new_id("outbox")
        self._outbox.append({
            "outbox_id": outbox_id, "event_type": event_type, "actor": actor,
            "workspace_id": workspace_id, "task_id": task_id, "operation_id": operation_id,
            "object_refs": object_refs, "payload": payload or {},
            "causation_id": causation_id, "correlation_id": correlation_id,
            "learning_allowed": learning_allowed,
        })
        return outbox_id

    def flush_outbox(self) -> list[Event]:
        if self.persistence:
            return self.persistence.dispatch_event_outbox()
        emitted: list[Event] = []
        pending, self._outbox = self._outbox, []
        for item in pending:
            ts = now()
            ev = Event(
                sequence=len(self._events) + 1, event_id=new_id("evt"),
                workspace_id=item["workspace_id"],
                event_type=item["event_type"], actor=item["actor"],
                task_id=item["task_id"], operation_id=item["operation_id"],
                object_refs=tuple(item["object_refs"]), payload=dict(item["payload"]),
                causation_id=item["causation_id"], correlation_id=item["correlation_id"],
                occurred_at=ts, observed_at=ts,
                learning_allowed=item["learning_allowed"],
            )
            self._events.append(ev)
            emitted.append(ev)
        return emitted

    def read_after(self, consumer_id: str) -> list[Event]:
        if self.persistence:
            return self.persistence.read_events_after(consumer_id)
        seq = self._cursors.get(consumer_id, 0)
        return [e for e in self._events if e.sequence > seq]

    def ack(self, consumer_id: str, sequence: int) -> None:
        if self.persistence:
            self.persistence.ack_event_consumer(consumer_id, sequence)
            return
        current = self._cursors.get(consumer_id, 0)
        if sequence >= current:
            self._cursors[consumer_id] = sequence

    def all_events(self) -> list[Event]:
        if self.persistence:
            return self.persistence.read_events_after("__audit_no_ack__")
        return list(self._events)
