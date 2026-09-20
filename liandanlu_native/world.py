from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import Entity, WorldRevisions


class UnknownObject(KeyError):
    pass


class EntityVersionConflict(RuntimeError):
    pass


class StaleWorld(RuntimeError):
    pass


@dataclass
class WorldModel:
    revisions: WorldRevisions = field(default_factory=WorldRevisions)
    entities: dict[str, Entity] = field(default_factory=dict)
    relations: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)

    def register(self, entity: Entity) -> Entity:
        self.entities[entity.entity_id] = entity
        self.revisions.bump("workspace")
        return entity

    def get(self, ref: str, *, expected_version: int | None = None) -> Entity:
        try:
            entity = self.entities[ref]
        except KeyError as exc:
            raise UnknownObject(ref) from exc
        if expected_version is not None and entity.version != expected_version:
            raise EntityVersionConflict(f"{ref}: expected {expected_version}, actual {entity.version}")
        return entity

    def resolve_locator(self, ref: str, permission: str) -> str:
        entity = self.get(ref)
        if permission not in entity.permissions:
            raise PermissionError(f"{ref} lacks {permission}")
        return entity.locator

    def assert_revisions(self, expected: dict[str, int]) -> None:
        for domain, value in expected.items():
            actual = getattr(self.revisions, domain)
            if actual != value:
                raise StaleWorld(f"{domain}: expected {value}, actual {actual}")

    def observe(self, source: str, observation_type: str, value: dict[str, Any], domain: str) -> None:
        self.observations.append({"source": source, "type": observation_type, "value": value})
        self.revisions.bump(domain)

    def relate(self, source: str, relation: str, target: str) -> None:
        self.get(source)
        self.get(target)
        self.relations.setdefault(source, []).append((relation, target))
        self.revisions.bump("workspace")
