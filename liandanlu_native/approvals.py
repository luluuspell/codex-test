"""Durable, request-bound human approval; never exposed as a model tool.

The host authenticates the human BEFORE decide/revoke. Principal strings here
are trusted host context, not a replacement for UI authentication. Approvals,
budgets, Operations and events all share one SQLiteStore transaction authority.
"""
from __future__ import annotations

from dataclasses import asdict
from enum import Enum
import hashlib
import hmac
import json
import math
from typing import Any

from .integrity import StateConflict, _lease, _task_allowed, _world_preflight, copy_record, immediate, unsettled_rows
from .models import ActionProposal, Operation, new_id, now
from .policy import PolicyDecision


class ApprovalError(StateConflict):
    pass


def canonical(value: Any) -> str:
    def normalize(item: Any) -> Any:
        if isinstance(item, Enum):
            return item.value
        if isinstance(item, dict):
            if not all(isinstance(k, str) for k in item):
                raise ApprovalError('JSON object keys must be strings')
            return {k: normalize(v) for k, v in item.items()}
        if isinstance(item, (set, frozenset)):
            return sorted((normalize(v) for v in item), key=lambda v: json.dumps(v, sort_keys=True))
        if isinstance(item, (list, tuple)):
            return [normalize(v) for v in item]
        return item
    return json.dumps(normalize(value), sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


class ApprovalService:
    def __init__(self, store: Any, registry: Any, policy: Any, *,
                 human_principals: frozenset[str] = frozenset(), ttl_seconds: float = 600):
        if not math.isfinite(ttl_seconds) or not 0 < ttl_seconds <= 86400:
            raise ValueError('approval TTL must be finite, positive and at most 24 hours')
        if any(not isinstance(p, str) or not p for p in human_principals):
            raise ValueError('human principal IDs must be nonempty strings')
        self.store, self.registry, self.policy = store, registry, policy
        self.human_principals, self.ttl_seconds = frozenset(human_principals), ttl_seconds
        with immediate(store):
            store.conn.execute('''CREATE TABLE IF NOT EXISTS approval_requests(
                approval_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                workspace_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                snapshot_json TEXT NOT NULL, request_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('PENDING','APPROVED','DENIED','REVOKED','CONSUMED')),
                created_at REAL NOT NULL, expires_at REAL NOT NULL, decided_at REAL, decided_by TEXT,
                UNIQUE(task_id,proposal_id), FOREIGN KEY(task_id) REFERENCES tasks(task_id))''')
            store.conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS approval_one_open_task
                ON approval_requests(task_id) WHERE state IN ('PENDING','APPROVED')''')
            store.conn.execute('''CREATE TABLE IF NOT EXISTS approval_operations(
                operation_id TEXT PRIMARY KEY, approval_id TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
                FOREIGN KEY(operation_id) REFERENCES operations(operation_id),
                FOREIGN KEY(approval_id) REFERENCES approval_requests(approval_id))''')

    def _row(self, approval_id: str) -> Any:
        row = self.store.conn.execute('SELECT * FROM approval_requests WHERE approval_id=?', (approval_id,)).fetchone()
        if row is None:
            raise ApprovalError('approval not found')
        return row

    def _event(self, row: Any, event_type: str, actor: str, **extra: Any) -> None:
        self.store._insert_outbox(event_type=event_type, actor=actor,
            workspace_id=row['workspace_id'], task_id=row['task_id'],
            payload={'approval_id': row['approval_id'], 'request_digest': row['request_digest'], **extra})

    def inspect(self, approval_id: str, *, workspace_id: str) -> dict:
        row = self._row(approval_id)
        if row['workspace_id'] != workspace_id:
            raise PermissionError('approval belongs to another workspace')
        value = dict(row)
        value['snapshot'] = json.loads(value.pop('snapshot_json'))
        value['effective_state'] = ('EXPIRED' if value['state'] in {'PENDING', 'APPROVED'}
                                    and value['expires_at'] <= now() else value['state'])
        return value

    def outstanding(self, task_id: str) -> dict | None:
        row = self.store.conn.execute('''SELECT * FROM approval_requests WHERE task_id=?
            AND state IN ('PENDING','APPROVED') ORDER BY created_at DESC LIMIT 1''', (task_id,)).fetchone()
        return self.inspect(row['approval_id'], workspace_id=row['workspace_id']) if row else None

    def _snapshot(self, op: Operation) -> dict:
        proposal = ActionProposal('approval-validation', op.capability, op.action, tuple(op.object_refs), op.arguments)
        spec = self.registry.resolve(proposal)
        if (op.risk_class != spec.risk_class or op.required_permission != spec.required_permission
                or op.idempotency_mode != spec.idempotency_mode.value):
            raise ApprovalError('action manifest changed')
        if set(op.expected_revisions) != set(spec.revision_domains):
            raise ApprovalError('approval must bind all action revision domains')
        task = self.store.conn.execute('SELECT * FROM tasks WHERE task_id=?', (op.task_id,)).fetchone()
        _task_allowed(task)
        if task['workspace_id'] != op.workspace_id:
            raise ApprovalError('task workspace changed')
        policy = self.policy.policies.get(op.workspace_id)
        if policy is None or self.policy.evaluate(op.workspace_id, op.capability, op.action,
                                                 spec.risk_class) is PolicyDecision.DENY:
            raise ApprovalError('current policy denies action')
        _world_preflight(self.store, op)
        entities = []
        for ref in op.object_refs:
            entity = self.store.conn.execute('SELECT * FROM world_entities WHERE entity_id=?', (ref,)).fetchone()
            entities.append({'ref': ref, 'fingerprint': digest(dict(entity))})
        budget = json.loads(task['budget_json'] or '{}')
        return {
            'request': {'capability': op.capability, 'action': op.action, 'object_refs': list(op.object_refs),
                        'arguments': op.arguments, 'expected_revisions': op.expected_revisions},
            'task': {'task_id': op.task_id, 'workspace_id': op.workspace_id, 'goal': task['goal'],
                     'constraints': json.loads(task['constraints_json']),
                     'success_criteria': json.loads(task['success_criteria_json']),
                     'max_operations': budget.get('max_operations', 64), 'deadline_at': budget.get('deadline_at')},
            'action_spec': asdict(spec), 'policy': asdict(policy), 'entities': entities}

    def _operation(self, task_id: str, workspace_id: str, proposal: ActionProposal, expected: dict) -> Operation:
        spec = self.registry.resolve(proposal)
        return Operation(operation_id='approval-preview', task_id=task_id, workspace_id=workspace_id,
            capability=proposal.capability, action=proposal.action, object_refs=tuple(proposal.object_refs),
            arguments=json.loads(canonical(proposal.arguments)), risk_class=spec.risk_class,
            required_permission=spec.required_permission, idempotency_mode=spec.idempotency_mode.value,
            expected_revisions=dict(expected))

    def _fresh(self, row: Any, op: Operation | None = None) -> None:
        if row['expires_at'] <= now():
            raise ApprovalError('approval expired; fresh confirmation is required')
        saved = json.loads(row['snapshot_json'])
        if not hmac.compare_digest(digest(saved), row['request_digest']):
            raise ApprovalError('stored approval digest mismatch')
        if op is None:
            req = saved['request']
            proposal = ActionProposal(row['proposal_id'], req['capability'], req['action'],
                                      tuple(req['object_refs']), req['arguments'])
            op = self._operation(row['task_id'], row['workspace_id'], proposal, req['expected_revisions'])
        if op.task_id != row['task_id'] or op.workspace_id != row['workspace_id']:
            raise ApprovalError('approval task/workspace mismatch')
        if not hmac.compare_digest(digest(self._snapshot(op)), row['request_digest']):
            raise ApprovalError('approved request, policy, task or objects changed')

    def request(self, task: Any, proposal: ActionProposal, expected: dict, *, task_lease: Any = None) -> str:
        preview = self._operation(task.task_id, task.workspace_id, proposal, expected)
        with immediate(self.store):
            if task_lease is not None:
                if task_lease.task_id != task.task_id:
                    raise ApprovalError('lease task mismatch')
                _lease(self.store.conn, 'task_leases', task.task_id, task_lease.owner_id, task_lease.generation)
            if unsettled_rows(self.store, task.task_id):
                raise ApprovalError('unresolved operation prevents a new approval')
            snapshot = self._snapshot(preview)
            snapshot_json = canonical(snapshot)
            if len(snapshot_json.encode('utf-8')) > 262144:
                raise ApprovalError('approval snapshot exceeds 256 KiB; use a reviewed artifact reference')
            request_digest = digest(snapshot)
            old = self.store.conn.execute('SELECT * FROM approval_requests WHERE task_id=? AND proposal_id=?',
                                          (task.task_id, proposal.proposal_id)).fetchone()
            if old:
                if old['request_digest'] != request_digest:
                    raise ApprovalError('proposal ID reused with a different request')
                if old['state'] not in {'PENDING', 'APPROVED'}:
                    raise ApprovalError('proposal was already decided or consumed; do not reuse its ID')
                self._fresh(old)
                return old['approval_id']
            if self.outstanding(task.task_id) is not None:
                raise ApprovalError('resolve or revoke the outstanding approval first')
            approval_id, ts = new_id('approval'), now()
            self.store.conn.execute('''INSERT INTO approval_requests
                (approval_id,task_id,workspace_id,proposal_id,snapshot_json,request_digest,state,created_at,expires_at)
                VALUES(?,?,?,?,?,?,'PENDING',?,?)''', (approval_id, task.task_id, task.workspace_id,
                proposal.proposal_id, snapshot_json, request_digest, ts, ts + self.ttl_seconds))
            self.store.conn.execute('UPDATE tasks SET state=?,wait_reason=?,phase=?,revision=revision+1 WHERE task_id=?',
                                   ('WAITING', f'approval:{approval_id}', 'AUTHORIZING', task.task_id))
            self._event(self._row(approval_id), 'approval.requested', 'policy')
        copy_record(task, self.store.load_tasks()[task.task_id])
        return approval_id

    def _human(self, row: Any, principal_id: str, workspace_id: str, request_digest: str) -> None:
        if principal_id not in self.human_principals:
            raise PermissionError('authenticated human principal is required')
        if workspace_id != row['workspace_id']:
            raise PermissionError('wrong approval workspace')
        if not hmac.compare_digest(row['request_digest'], request_digest):
            raise ApprovalError('human must acknowledge the exact displayed request digest')

    def decide(self, approval_id: str, *, approve: bool, principal_id: str,
               workspace_id: str, request_digest: str) -> dict:
        if type(approve) is not bool:
            raise ValueError('approve must be a boolean')
        with immediate(self.store):
            row = self._row(approval_id)
            self._human(row, principal_id, workspace_id, request_digest)
            desired = 'APPROVED' if approve else 'DENIED'
            if row['state'] != 'PENDING':
                if row['decided_by'] == principal_id and (row['state'] == desired or
                        (approve and row['state'] == 'CONSUMED')):
                    return self.inspect(approval_id, workspace_id=workspace_id)
                raise ApprovalError('approval is not pending')
            self._fresh(row)
            self.store.conn.execute("UPDATE approval_requests SET state=?,decided_at=?,decided_by=? WHERE approval_id=? AND state='PENDING'",
                                    (desired, now(), principal_id, approval_id))
            self.store.conn.execute('UPDATE task_leases SET lease_until=0 WHERE task_id=?', (row['task_id'],))
            self.store.conn.execute('''UPDATE tasks SET state=?,desired_state=?,wait_reason=?,
                queued_at=?,revision=revision+1 WHERE task_id=?''', ('QUEUED' if approve else 'BLOCKED',
                'RUNNING', f'approval:{approval_id}' if approve else 'approval:denied', now(), row['task_id']))
            self._event(row, 'approval.approved' if approve else 'approval.denied', 'user', principal_id=principal_id)
        return self.inspect(approval_id, workspace_id=workspace_id)

    def revoke(self, approval_id: str, *, principal_id: str, workspace_id: str, request_digest: str) -> dict:
        with immediate(self.store):
            row = self._row(approval_id)
            self._human(row, principal_id, workspace_id, request_digest)
            if row['state'] == 'REVOKED':
                return self.inspect(approval_id, workspace_id=workspace_id)
            if row['state'] not in {'PENDING', 'APPROVED', 'CONSUMED'}:
                raise ApprovalError('approval is not revocable')
            linked = self.store.conn.execute('''SELECT o.state FROM operations o
                JOIN approval_operations a ON a.operation_id=o.operation_id WHERE a.approval_id=?''',
                (approval_id,)).fetchone()
            if linked is not None and linked['state'] != 'PREPARED':
                raise ApprovalError('operation was dispatched; revocation cannot undo a side effect')
            self.store.conn.execute("UPDATE approval_requests SET state='REVOKED' WHERE approval_id=?", (approval_id,))
            # Revocation must stop scheduling too, not silently trigger another model proposal.
            self.store.conn.execute("""UPDATE tasks SET state='BLOCKED',wait_reason='approval:revoked',
                revision=revision+1 WHERE task_id=? AND state NOT IN ('COMPLETED','FAILED','CANCELLED')
                AND desired_state NOT IN ('PAUSED','CANCELLED')""", (row['task_id'],))
            self._event(row, 'approval.revoked', 'user', principal_id=principal_id)
        return self.inspect(approval_id, workspace_id=workspace_id)

    def approved_action(self, approval_id: str) -> tuple[ActionProposal, dict]:
        row = self._row(approval_id)
        if row['state'] != 'APPROVED':
            raise ApprovalError('approval is not approved')
        self._fresh(row)
        req = json.loads(row['snapshot_json'])['request']
        return (ActionProposal(row['proposal_id'], req['capability'], req['action'],
                               tuple(req['object_refs']), req['arguments']), req['expected_revisions'])

    def bind_in_transaction(self, op: Operation, approval_id: str | None) -> None:
        """After operation INSERT, before that SAME reservation transaction commits."""
        if not self.store.conn.in_transaction:
            raise ApprovalError('approval binding requires the operation transaction')
        decision = self.policy.evaluate(op.workspace_id, op.capability, op.action, op.risk_class)
        if decision is PolicyDecision.DENY:
            raise ApprovalError('current policy denies operation')
        if approval_id is None:
            if decision is not PolicyDecision.ALLOW:
                raise ApprovalError('operation requires a request-bound human approval')
            return
        row = self._row(approval_id)
        if row['state'] != 'APPROVED':
            raise ApprovalError('approval was consumed, rejected or never approved')
        self._fresh(row, op)
        self.store.conn.execute('INSERT INTO approval_operations(operation_id,approval_id,request_digest) VALUES(?,?,?)',
                               (op.operation_id, approval_id, row['request_digest']))
        self.store.conn.execute("UPDATE approval_requests SET state='CONSUMED' WHERE approval_id=?", (approval_id,))
        self._event(row, 'approval.consumed', 'engine', operation_id=op.operation_id)

    def check_dispatch_in_transaction(self, op: Operation) -> None:
        link = self.store.conn.execute('SELECT * FROM approval_operations WHERE operation_id=?', (op.operation_id,)).fetchone()
        if link is None:
            if self.policy.evaluate(op.workspace_id, op.capability, op.action, op.risk_class) is not PolicyDecision.ALLOW:
                raise ApprovalError('unapproved or newly forbidden dispatch')
            return
        row = self._row(link['approval_id'])
        if row['state'] != 'CONSUMED' or row['request_digest'] != link['request_digest']:
            raise ApprovalError('dispatch authorization was revoked or modified')
        self._fresh(row, op)


def check_dispatch_approval(store: Any, op: Operation, service: ApprovalService | None) -> None:
    if service is not None:
        if service.store is not store:
            raise ApprovalError('approval service must share the operation store')
        service.check_dispatch_in_transaction(op)
        return
    table = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='approval_operations'").fetchone()
    if table and store.conn.execute('SELECT 1 FROM approval_operations WHERE operation_id=?', (op.operation_id,)).fetchone():
        raise ApprovalError('approved operation requires its configured authorization service')
