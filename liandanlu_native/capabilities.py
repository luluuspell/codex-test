from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .models import ActionProposal, RiskClass


class IdempotencyMode(str, Enum):
    SAFE_REPEAT = "SAFE_REPEAT"
    KEYED = "KEYED"
    RECONCILABLE = "RECONCILABLE"
    NON_RECONCILABLE = "NON_RECONCILABLE"


class InvalidAction(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ActionSpec:
    capability: str
    action: str
    risk_class: RiskClass
    required_permission: str
    allowed_arguments: frozenset[str] = frozenset()
    required_arguments: frozenset[str] = frozenset()
    revision_domains: frozenset[str] = frozenset()
    idempotency_mode: IdempotencyMode = IdempotencyMode.RECONCILABLE
    forbidden_arguments: frozenset[str] = frozenset({
        "path", "absolute_path", "locator", "filesystem_path", "shell_command"
    })

    def validate(self, proposal: ActionProposal) -> None:
        if proposal.capability != self.capability or proposal.action != self.action:
            raise InvalidAction("proposal does not match action spec")
        keys = set(proposal.arguments)
        forbidden = keys & set(self.forbidden_arguments)
        if forbidden:
            raise InvalidAction(f"forbidden locator-like arguments: {sorted(forbidden)}")
        unknown = keys - set(self.allowed_arguments)
        if unknown:
            raise InvalidAction(f"unknown arguments: {sorted(unknown)}")
        missing = set(self.required_arguments) - keys
        if missing:
            raise InvalidAction(f"missing required arguments: {sorted(missing)}")


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
