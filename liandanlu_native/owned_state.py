"""Fenced task-state writes used by a worker, distinct from user control writes."""
from __future__ import annotations

from typing import Any

from .integrity import LeaseLost, TERMINAL_TASKS, _lease, copy_record, immediate
from .models import TaskPhase, TaskState, now


def check_owner(tasks: Any, task_id: str, lease: Any) -> None:
    if lease is not None and lease.task_id != task_id:
        raise LeaseLost('task lease belongs to another task')
    if tasks.persistence is not None:
        conn = tasks.persistence.conn
        if lease is None:
            row = conn.execute('SELECT lease_until FROM task_leases WHERE task_id=?', (task_id,)).fetchone()
            if row is not None and row['lease_until'] > now():
                raise LeaseLost('another worker owns this task')
        else:
            _lease(conn, 'task_leases', task_id, lease.owner_id, lease.generation)
    elif lease is not None and lease.lease_until <= now():
        raise LeaseLost('task lease expired')


def update_owned(tasks: Any, task: Any, lease: Any, *, state: TaskState | None = None,
                 phase: TaskPhase | None = None, reason: str | None = None,
                 event_type: str = 'task.state_changed', actor: str = 'engine') -> None:
    store = tasks.persistence
    if store is None:
        check_owner(tasks, task.task_id, lease)
        if reason is not None:
            tasks.wait(task.task_id, reason, event_type=event_type)
        else:
            tasks.update_runtime_state(task.task_id, state=state, phase=phase,
                                       event_type=event_type, actor=actor)
        return
    with immediate(store):
        check_owner(tasks, task.task_id, lease)
        current = store.load_tasks()[task.task_id]
        if current.state in TERMINAL_TASKS:
            copy_record(task, current)
            return
        # A worker must not erase a concurrently committed pause/cancel request.
        if current.desired_state in {TaskState.PAUSED, TaskState.CANCELLED}:
            copy_record(task, current)
            return
        if state is not None:
            current.state = state
        if phase is not None:
            current.phase = phase
        current.wait_reason = reason
        current.revision += 1
        store._upsert_task(current)
        store._insert_outbox(event_type=event_type, actor=actor,
            workspace_id=current.workspace_id, task_id=current.task_id,
            payload={'state': current.state.value, 'phase': current.phase.value, 'reason': reason})
    copy_record(task, current)
