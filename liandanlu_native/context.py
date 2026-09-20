from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .models import Task, new_id
from .world import WorldModel


@dataclass(slots=True)
class Referent:
    object_ref: str
    source: str
    score: float


@dataclass
class ReferentStack:
    items: list[Referent] = field(default_factory=list)

    def push(self, ref: str, source: str, score: float = 1.0) -> None:
        self.items = [x for x in self.items if x.object_ref != ref]
        self.items.insert(0, Referent(ref, source, score))
        del self.items[32:]

    def resolve(self, *, minimum: float = 0.85, ambiguity_gap: float = 0.12) -> str | None:
        if not self.items:
            return None
        ranked = sorted(self.items, key=lambda x: x.score, reverse=True)
        first = ranked[0]
        second = ranked[1] if len(ranked) > 1 else None
        if first.score < minimum:
            return None
        if second and first.score - second.score < ambiguity_gap:
            return None
        return first.object_ref


@dataclass(frozen=True, slots=True)
class ContextManifest:
    manifest_id: str
    task_id: str
    workspace_id: str
    world_revisions: dict[str, int]
    focus_object_refs: tuple[str, ...]
    referent_refs: tuple[str, ...]
    fact_memory_refs: tuple[str, ...]
    episode_memory_refs: tuple[str, ...]
    strategy_memory_refs: tuple[str, ...]
    recent_event_from: int | None
    recent_event_to: int | None
    brain_snapshot_ref: str | None
    constraints: tuple[str, ...]
    success_criteria: tuple[str, ...]
    excluded: tuple[tuple[str, str], ...]


def build_manifest(task: Task, world: WorldModel, referents: ReferentStack, *, focus: Iterable[str] = (), fact_refs: Iterable[str] = (), episode_refs: Iterable[str] = (), strategy_refs: Iterable[str] = (), event_range: tuple[int | None, int | None] = (None, None), brain_snapshot_ref: str | None = None, excluded: Iterable[tuple[str, str]] = ()) -> ContextManifest:
    revisions = world.revisions
    return ContextManifest(
        manifest_id=new_id("ctx"), task_id=task.task_id,
        workspace_id=task.workspace_id,
        world_revisions={"global_revision": revisions.global_revision, "desktop": revisions.desktop, "workspace": world.workspace_revision(task.workspace_id), "browser": revisions.browser, "media": revisions.media, "tasks": revisions.tasks},
        focus_object_refs=tuple(focus),
        referent_refs=tuple(x.object_ref for x in referents.items[:8]),
        fact_memory_refs=tuple(fact_refs), episode_memory_refs=tuple(episode_refs), strategy_memory_refs=tuple(strategy_refs),
        recent_event_from=event_range[0], recent_event_to=event_range[1], brain_snapshot_ref=brain_snapshot_ref,
        constraints=task.constraints, success_criteria=task.success_criteria, excluded=tuple(excluded),
    )
