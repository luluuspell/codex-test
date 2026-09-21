from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .models import ActionProposal, ResourceRequest, RiskClass


class IdempotencyMode(str, Enum):
    SAFE_REPEAT = "SAFE_REPEAT"
    KEYED = "KEYED"
    RECONCILABLE = "RECONCILABLE"
    NON_RECONCILABLE = "NON_RECONCILABLE"


class InvalidAction(ValueError):
    pass


def _find_forbidden_keys(value: Any, forbidden: frozenset[str], prefix: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            normalized = key.lower()
            location = f"{prefix}.{key}" if prefix else key
            if normalized in forbidden:
                found.append(location)
            found.extend(_find_forbidden_keys(child, forbidden, location))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            location = f"{prefix}[{index}]"
            found.extend(_find_forbidden_keys(child, forbidden, location))
    return found


@dataclass(frozen=True, slots=True)
class ActionSpec:
    capability: str
    action: str
    risk_class: RiskClass
    required_permission: str
    allowed_arguments: frozenset[str] = frozenset()
    required_arguments: frozenset[str] = frozenset()
    revision_domains: frozenset[str] = frozenset()
    resource_request: ResourceRequest = ResourceRequest()
    idempotency_mode: IdempotencyMode = IdempotencyMode.RECONCILABLE
    forbidden_arguments: frozenset[str] = frozenset({
        "path", "file_path", "absolute_path", "locator", "filesystem_path",
        "source_path", "target_path", "shell_command"
    })

    def validate(self, proposal: ActionProposal) -> None:
        if proposal.capability != self.capability or proposal.action != self.action:
            raise InvalidAction("proposal does not match action spec")
        keys = set(proposal.arguments)
        unknown = keys - set(self.allowed_arguments)
        if unknown:
            raise InvalidAction(f"unknown arguments: {sorted(unknown)}")
        missing = set(self.required_arguments) - keys
        if missing:
            raise InvalidAction(f"missing required arguments: {sorted(missing)}")
        forbidden = _find_forbidden_keys(proposal.arguments, self.forbidden_arguments)
        if forbidden:
            raise InvalidAction(f"forbidden locator-like arguments: {sorted(forbidden)}")


@dataclass
class CapabilityRegistry:
    actions: dict[tuple[str, str], ActionSpec] = field(default_factory=dict)

    def register(self, spec: ActionSpec) -> None:
        key = (spec.capability, spec.action)
        if key in self.actions:
            raise ValueError(f"duplicate action spec: {key}")
        self.actions[key] = spec

    def resolve(self, proposal: ActionProposal) -> ActionSpec:
        key = (proposal.capability, proposal.action)
        try:
            spec = self.actions[key]
        except KeyError as exc:
            raise InvalidAction(f"unregistered action: {proposal.capability}.{proposal.action}") from exc
        spec.validate(proposal)
        return spec
