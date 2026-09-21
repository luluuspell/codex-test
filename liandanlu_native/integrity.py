"""SQLite integrity transactions. No network or OS side effects run under a DB lock.

These helpers share SQLiteStore's connection and tables; they do not introduce
another store. External resources still need their own fencing/idempotency.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import fields
import json
from typing import Any, Iterator

from .models import OperationState, TaskBudget, TaskState, now
from .world import StaleWorld


UNSETTLED = frozenset({
    OperationState.RUNNING, OperationState.UNKNOWN, OperationState.RECONCILING,
    OperationState.OBSERVED, OperationState.VERIFYING,
})
TERMINAL_TASKS = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED})


class LeaseLost(RuntimeError):
    pass


class OperationConflict(RuntimeError):
    pass


class StateConflict(RuntimeError):
    pass


class BudgetExceeded(RuntimeError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def copy_record(target: Any, source: Any) -> None:
    for item in fields(source):
        setattr(target, item.name, deepcopy(getattr(source, item.name)))


@contextmanager
def immediate(store: Any) -> Iterator[None]:
    """Own exactly one short transaction; never roll back a caller's transaction."""
    conn = store.conn
    if conn.in_transaction:
        raise StateConflict('nested write transaction is not supported here')
    conn.execute('BEGIN IMMEDIATE')
    try:
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def _lease(conn: Any, table: str, task_id: str, owner: str | None,
           generation: int | None) -> None:
    if owner is None and generation is None:
        return  # Unleased mode remains available only for explicit local/test callers.
    if not owner or type(generation) is not int or generation < 1:
        raise LeaseLost('invalid lease identity')
    if table not in {'task_leases', 'resource_leases'}:
        raise ValueError('unknown lease table')
    row = conn.execute(
        f'SELECT owner_id,generation,lease_until FROM {table} WHERE task_id=?',
        (task_id,),
    ).fetchone()
    if (row is None or row['owner_id'] != owner or row['generation'] != generation
            or row['lease_until'] <= now()):
        raise LeaseLost(f'{table}: stale or expired lease')


def validate_fences(store: Any, op: Any) -> None:
    _lease(store.conn, 'task_leases', op.task_id,
           op.task_lease_owner_id, op.task_lease_generation)
    _lease(store.conn, 'resource_leases', op.task_id,
           op.resource_lease_owner_id, op.resource_lease_generation)


def unsettled_rows(store: Any, task_id: str) -> list[Any]:
    states = tuple(s.value for s in UNSETTLED)
    marks = ','.join('?' for _ in states)
    return list(store.conn.execute(
        f'SELECT * FROM operations WHERE task_id=? AND state IN ({marks})',
        (task_id, *states),
    ))


def _task_allowed(row: Any) -> None:
    if row is None:
        raise StateConflict('task is not persisted')
    if TaskState(row['state']) in TERMINAL_TASKS:
        raise StateConflict('terminal task cannot execute')
    if row['desired_state'] in {TaskState.PAUSED.value, TaskState.CANCELLED.value}:
        raise StateConflict('task control forbids new operations')


def _world_preflight(store: Any, op: Any) -> tuple[str, ...]:
    """Re-read authoritative revisions, permissions and locators inside the lock."""
    conn = store.conn
    for domain, expected in op.expected_revisions.items():
        if domain == 'workspace':
            row = conn.execute('SELECT revision FROM workspace_revisions WHERE workspace_id=?',
                               (op.workspace_id,)).fetchone()
            actual = row['revision'] if row else 0
        else:
            if domain not in {'global_revision', 'desktop', 'browser', 'media', 'tasks'}:
                raise StaleWorld(f'unsupported revision domain: {domain}')
            row = conn.execute('SELECT * FROM world_revisions WHERE singleton=1').fetchone()
            actual = row[domain] if row else 0
        if actual != expected:
            raise StaleWorld(f'{domain}: expected {expected}, actual {actual}')
    locators = []
    for ref in op.object_refs:
        row = conn.execute('SELECT * FROM world_entities WHERE entity_id=?', (ref,)).fetchone()
        if (row is None or row['status'] != 'active'
                or row['workspace_id'] != op.workspace_id
                or op.required_permission not in json.loads(row['permissions_json'])):
            raise PermissionError(f'object access denied: {ref}')
        locators.append(row['locator'])
    return tuple(locators)


def _same_request(row: Any, op: Any) -> None:
    if row is None:
        raise OperationConflict('operation does not exist')
    scalar = ('task_id', 'workspace_id', 'capability', 'action', 'required_permission',
              'idempotency_mode', 'task_lease_owner_id', 'task_lease_generation',
              'resource_lease_owner_id', 'resource_lease_generation')
    if any(row[key] != getattr(op, key) for key in scalar):
        raise OperationConflict('immutable operation identity was modified')
    if (row['risk_class'] != op.risk_class.value
            or tuple(json.loads(row['object_refs_json'])) != tuple(op.object_refs)
            or json.loads(row['arguments_json']) != op.arguments
            or json.loads(row['expected_revisions_json']) != op.expected_revisions):
        raise OperationConflict('immutable operation request was modified')


def _event(store: Any, op: Any, name: str, actor: str, payload: dict | None = None) -> None:
    store._insert_outbox(event_type=name, actor=actor, workspace_id=op.workspace_id,
                        task_id=op.task_id, operation_id=op.operation_id,
                        object_refs=op.object_refs, payload=payload or {})


def reserve_operation(store: Any, task: Any, op: Any) -> None:
    """Budget decision, reservation, operation and outbox are one transaction."""
    with immediate(store):
        row = store.conn.execute('SELECT * FROM tasks WHERE task_id=?', (task.task_id,)).fetchone()
        _task_allowed(row)
        if row['workspace_id'] != op.workspace_id:
            raise PermissionError('task workspace mismatch')
        budget = TaskBudget(**json.loads(row['budget_json'] or '{}'))
        reason = budget.block_reason(now())
        if reason:
            raise BudgetExceeded(reason)
        if unsettled_rows(store, task.task_id):
            raise OperationConflict('previous operation must be reconciled before replanning')
        validate_fences(store, op)
        _world_preflight(store, op)
        budget.operations_started += 1
        store.conn.execute(
            'UPDATE tasks SET budget_json=?, revision=revision+1 WHERE task_id=?',
            (json.dumps({'max_operations': budget.max_operations,
                         'operations_started': budget.operations_started,
                         'deadline_at': budget.deadline_at}), task.task_id),
        )
        store._upsert_operation(op)
        _event(store, op, 'operation.prepared', 'engine', {
            'budget_operations_started': budget.operations_started,
            'budget_max_operations': budget.max_operations,
        })
    copy_record(task, store.load_tasks()[task.task_id])


def start_operation(store: Any, op: Any) -> tuple[str, ...]:
    """Only the winner of PREPARED -> RUNNING may invoke an adapter."""
    with immediate(store):
        row = store.conn.execute('SELECT * FROM operations WHERE operation_id=?',
                                 (op.operation_id,)).fetchone()
        _same_request(row, op)
        if row['state'] != OperationState.PREPARED.value:
            raise OperationConflict(f'operation already dispatched or terminal: {row["state"]}')
        task = store.conn.execute('SELECT * FROM tasks WHERE task_id=?', (op.task_id,)).fetchone()
        _task_allowed(task)
        deadline = json.loads(task['budget_json'] or '{}').get('deadline_at')
        if deadline is not None and now() >= deadline:
            raise BudgetExceeded('deadline')
        validate_fences(store, op)
        locators = _world_preflight(store, op)
        store.conn.execute('UPDATE operations SET state=? WHERE operation_id=? AND state=?',
                           (OperationState.RUNNING.value, op.operation_id,
                            OperationState.PREPARED.value))
        _event(store, op, 'operation.started', 'engine')
    op.state = OperationState.RUNNING
    return locators


def persist_operation(store: Any, op: Any, expected: OperationState,
                      event_type: str, actor: str, payload: dict | None = None) -> None:
    """CAS prevents a late callback from overwriting a newer recovery result."""
    with immediate(store):
        row = store.conn.execute('SELECT * FROM operations WHERE operation_id=?',
                                 (op.operation_id,)).fetchone()
        _same_request(row, op)
        if row['state'] != expected.value:
            raise OperationConflict(f'operation changed: expected {expected.value}, got {row["state"]}')
        store._upsert_operation(op)
        _event(store, op, event_type, actor, payload)


def persist_task(store: Any, task: Any, event_type: str, actor: str,
                 payload: dict | None = None) -> None:
    with immediate(store):
        row = store.conn.execute('SELECT revision FROM tasks WHERE task_id=?',
                                 (task.task_id,)).fetchone()
        if row is not None and row['revision'] != task.revision - 1:
            raise StateConflict('stale task revision cannot overwrite newer task state')
        store._upsert_task(task)
        store._insert_outbox(event_type=event_type, actor=actor, workspace_id=task.workspace_id,
                             task_id=task.task_id, payload=payload or {})


def resource_is_pinned(store: Any, row: Any) -> bool:
    return any(op['resource_lease_owner_id'] == row['owner_id']
               and op['resource_lease_generation'] == row['generation']
               for op in unsettled_rows(store, row['task_id']))
