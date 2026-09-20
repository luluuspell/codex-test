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
    def save_workspace_revision(self, workspace_id: str, revision: int) -> None: ...
    def save_world_relation(self, source: str, relation: str, target: str) -> None: ...


@dataclass
class WorldModel:
    revisions: WorldRevisions = field(default_factory=WorldRevisions)
    entities: dict[str, Entity] = field(default_factory=dict)
    relations: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    workspace_revisions: dict[str, int] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)
    persistence: WorldPersistence | None = None

    def _persist_revisions(self) -> None:
        if self.persistence:
            self.persistence.save_world_revisions(self.revisions)

    def workspace_revision(self, workspace_id: str) -> int:
        return self.workspace_revisions.get(workspace_id, 0)

    def _bump_workspace(self, workspace_id: str) -> None:
        revision = self.workspace_revision(workspace_id) + 1
        self.workspace_revisions[workspace_id] = revision
        self.revisions.workspace += 1
        self.revisions.global_revision += 1
        if self.persistence:
            self.persistence.save_workspace_revision(workspace_id, revision)
        self._persist_revisions()

    def register(self, entity: Entity) -> Entity:
        self.entities[entity.entity_id] = entity
        if self.persistence:
            self.persistence.save_world_entity(entity)
        self._bump_workspace(entity.workspace_id)
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

    def assert_access(
        self,
        ref: str,
        permission: str,
        *,
        workspace_id: str,
    ) -> Entity:
        entity = self.get(ref)
        if entity.workspace_id != workspace_id:
            raise PermissionError(
                f"{ref} belongs to workspace {entity.workspace_id}, not {workspace_id}"
            )
        if permission not in entity.permissions:
            raise PermissionError(f"{ref} lacks {permission}")
        return entity

    def resolve_locator(
        self,
        ref: str,
        permission: str,
        *,
        workspace_id: str | None = None,
    ) -> str:
        if workspace_id is None:
            entity = self.get(ref)
            if permission not in entity.permissions:
                raise PermissionError(f"{ref} lacks {permission}")
        else:
            entity = self.assert_access(ref, permission, workspace_id=workspace_id)
        return entity.locator

    def assert_revisions(
        self,
        expected: dict[str, int],
        *,
        workspace_id: str | None = None,
    ) -> None:
        for domain, value in expected.items():
            if domain == "workspace":
                if workspace_id is None:
                    raise ValueError("workspace_id is required for workspace revision checks")
                actual = self.workspace_revision(workspace_id)
            else:
                if not hasattr(self.revisions, domain):
                    raise KeyError(domain)
                actual = getattr(self.revisions, domain)
            if actual != value:
                raise StaleWorld(f"{domain}: expected {value}, actual {actual}")

    def observe(
        self,
        source: str,
        observation_type: str,
        value: dict[str, Any],
        domain: str,
        *,
        workspace_id: str | None = None,
    ) -> None:
        self.observations.append({
            "source": source, "type": observation_type,
            "value": value, "workspace_id": workspace_id,
        })
        if domain == "workspace":
            if workspace_id is None:
                raise ValueError("workspace_id is required for workspace observations")
            self._bump_workspace(workspace_id)
        else:
            self.revisions.bump(domain)
            self._persist_revisions()

    def relate(self, source: str, relation: str, target: str) -> None:
        source_entity = self.get(source)
        target_entity = self.get(target)
        if source_entity.workspace_id != target_entity.workspace_id:
            raise PermissionError("cross-workspace object relations require an explicit bridge")
        edge = (relation, target)
        bucket = self.relations.setdefault(source, [])
        if edge not in bucket:
            bucket.append(edge)
            if self.persistence:
                self.persistence.save_world_relation(source, relation, target)
            self._bump_workspace(source_entity.workspace_id)
