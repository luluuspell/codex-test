from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .models import RiskClass


class PolicyDecision(str, Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    REQUIRE_CONFIRMATION = "REQUIRE_CONFIRMATION"


@dataclass
class WorkspacePolicy:
    allowed_capabilities: frozenset[str] = frozenset()
    denied_actions: frozenset[str] = frozenset()
    confirm_risks: frozenset[RiskClass] = frozenset({
        RiskClass.MUTATING, RiskClass.EXTERNAL, RiskClass.DESTRUCTIVE
    })
    allow_risks: frozenset[RiskClass] = frozenset({
        RiskClass.READ, RiskClass.REVERSIBLE
    })


@dataclass
class PolicyEngine:
    policies: dict[str, WorkspacePolicy] = field(default_factory=dict)

    def evaluate(self, workspace_id: str, capability: str, action: str, risk: RiskClass) -> PolicyDecision:
        policy = self.policies.get(workspace_id)
        if policy is None:
            return PolicyDecision.DENY
        if action in policy.denied_actions:
            return PolicyDecision.DENY
        if capability not in policy.allowed_capabilities:
            return PolicyDecision.DENY
        if risk in policy.confirm_risks:
            return PolicyDecision.REQUIRE_CONFIRMATION
        if risk in policy.allow_risks:
            return PolicyDecision.ALLOW
        return PolicyDecision.DENY
