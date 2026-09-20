from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BrainSnapshot:
    snapshot_id: str
    brain_version: str
    encoder_version: str
    processed_event_seq: int
    next_event_predictions: tuple[tuple[str, float], ...] = ()
    novelty: float = 0.0
    valid_until_seq: int | None = None

    def usable(self, engine_event_seq: int, *, max_lag: int = 32) -> bool:
        lag = engine_event_seq - self.processed_event_seq
        if lag < 0 or lag > max_lag:
            return False
        if self.valid_until_seq is not None and engine_event_seq > self.valid_until_seq:
            return False
        return True


@dataclass
class FlyConsumerState:
    brain_version: str
    last_received_sequence: int = 0
    last_processed_sequence: int = 0
    last_checkpoint_sequence: int = 0
    state: str = "OFFLINE"

    def lag(self, latest_allowed_sequence: int) -> int:
        return max(0, latest_allowed_sequence - self.last_processed_sequence)

    def receive(self, sequence: int) -> None:
        self.last_received_sequence = max(self.last_received_sequence, sequence)

    def processed(self, sequence: int) -> None:
        self.last_processed_sequence = max(self.last_processed_sequence, sequence)
        if self.last_processed_sequence >= self.last_received_sequence:
            self.state = "LIVE"
        else:
            self.state = "LAGGING"
