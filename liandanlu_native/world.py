from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .models import Entity, WorldRevisions


class UnknownObject(KeyError):
    pass


class EntityVersionConflict(RuntimeError):
    pass


class StaleWorld(RuntimeError):
    pass


class WorldPersistence(Protocol):
    def save_world_entity(self, entity: Entity) -> None: ...
    def save_world_revisions(self, revisions: WorldRevisions) -> None: ...
    def save_world_relation(self, source: str, relation: str, target: str) -> None: ...


@dataclass
class WorldModel:
    revisions: WorldRevisions = field(default_factory=WorldRevisions)
    entities: dict[str, Entity] = field(default_factory=dict)
    relations: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    persistence: WorldPersistence | None = None

    def _persist_revisions(self) -> None:
        if self.persistence:
            self.persistence.save_world_revisions(self.revisions)

    def register(self, entity: Entity) -> Entity:
        self.entities[entity.entity_id] = entity
        self.revisions.bump("workspace")
        if self.persistence:
            self.persistence.save_world_entity(entity)
        self._persist_revisions()
        return entity

    def get(self, ref: str, *, expected_version: int | None = None) -> Entity:
        try:
            entity = self.entities[ref]
        except KeyError as exc:
            raise UnknownObject(ref) from exc
        if entity.status != "active":
            raise UnknownObject(f"{ref} is {entity.status}")
        if expected_version is not None and entity.version != expected_version:
            raise EntityVersionConflict(f"{ref}: expected {expected_version}, actual {entity.version}")
        return entity

    def resolve_locator(
        self,
        ref: str,
        permission: str,
        *,
        workspace_id: str | None = None,
    ) -> str:
        entity = self.get(ref)
        if workspace_id is not None and entity.workspace_id != workspace_id:
            raise PermissionError(
                f"{ref} belongs to workspace {entity.workspace_id}, not {workspace_id}"
            )
        if permission not in entity.permissions:
            raise PermissionError(f"{ref} lacks {permission}")
        return entity.locator

    def assert_revisions(self, expected: dict[str, int]) -> None:
        for domain, value in expected.items():
            if not hasattr(self.revisions, domain):
                raise KeyError(domain)
            actual = getattr(self.revisions, domain)
            if actual != value:
                raise StaleWorld(f"{domain}: expected {value}, actual {actual}")

    def observe(self, source: str, observation_type: str, value: dict[str, Any], domain: str) -> None:
        self.observations.append({"source": source, "type": observation_type, "value": value})
        self.revisions.bump(domain)
        self._persist_revisions()

    def relate(self, source: str, relation: str, target: str) -> None:
        self.get(source)
        self.get(target)
        edge = (relation, target)
        bucket = self.relations.setdefault(source, [])
        if edge not in bucket:
            bucket.append(edge)
            if self.persistence:
                self.persistence.save_world_relation(source, relation, target)
            self.revisions.bump("workspace")
            self._persist_revisions()
