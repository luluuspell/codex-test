from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import Event, new_id, now


@dataclass
class EventStore:
    events: list[Event] = field(default_factory=list)
    outbox: list[dict[str, Any]] = field(default_factory=list)
    cursors: dict[str, int] = field(default_factory=dict)

    def enqueue_outbox(self, *, event_type: str, actor: str, task_id: str | None = None, operation_id: str | None = None, object_refs: tuple[str, ...] = (), payload: dict[str, Any] | None = None, causation_id: str | None = None, correlation_id: str | None = None, learning_allowed: bool = True) -> str:
        outbox_id = new_id("outbox")
        self.outbox.append({"outbox_id": outbox_id, "event_type": event_type, "actor": actor, "task_id": task_id, "operation_id": operation_id, "object_refs": object_refs, "payload": payload or {}, "causation_id": causation_id, "correlation_id": correlation_id, "learning_allowed": learning_allowed})
        return outbox_id

    def flush_outbox(self) -> list[Event]:
        emitted: list[Event] = []
        pending, self.outbox = self.outbox, []
        for item in pending:
            ev = Event(sequence=len(self.events) + 1, event_id=new_id("evt"), event_type=item["event_type"], actor=item["actor"], task_id=item["task_id"], operation_id=item["operation_id"], object_refs=tuple(item["object_refs"]), payload=dict(item["payload"]), causation_id=item["causation_id"], correlation_id=item["correlation_id"], occurred_at=now(), learning_allowed=item["learning_allowed"])
            self.events.append(ev)
            emitted.append(ev)
        return emitted

    def read_after(self, consumer_id: str) -> list[Event]:
        seq = self.cursors.get(consumer_id, 0)
        return [e for e in self.events if e.sequence > seq]

    def ack(self, consumer_id: str, sequence: int) -> None:
        current = self.cursors.get(consumer_id, 0)
        if sequence < current:
            return
        self.cursors[consumer_id] = sequence
