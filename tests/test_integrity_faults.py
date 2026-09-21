"""Fault injection beyond the original a8 regression reproductions."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import sqlite3

import pytest

from test_recovery_integrity import Counter, Model, core, task_and_op
from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.models import (ActionProposal, OperationState, ResourceCapacity,
                                     ResourceRequest, TaskState, now)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.resources import ResourceBroker
from liandanlu_native.runtime import TaskScheduler


@pytest.fixture(scope='module')
def crash_results(tmp_path_factory):
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'companion_smoke.py'
    spec = importlib.util.spec_from_file_location('companion_crash_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.run_scenarios(tmp_path_factory.mktemp('real-crashes'))
    return {item['phase']: item for item in report['cases']}


@pytest.mark.parametrize('phase', [
    'after_prepare', 'after_dispatch_before_effect', 'after_effect_before_result',
    'after_result_before_verify', 'after_verify_before_dispatch',
])
def test_real_process_death_does_not_replay_side_effect(phase, crash_results):
    assert crash_results[phase]['injected_exit'] == 73
    assert crash_results[phase]['passed'], crash_results[phase]


def test_budget_and_operation_rollback_if_outbox_insert_fails(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task = tasks.create('fault', ('ok',), workspace_id='ws')
        tasks.control(task.task_id, TaskState.RUNNING)
        db.conn.execute('''CREATE TRIGGER abort_prepare BEFORE INSERT ON event_outbox
            WHEN NEW.event_type='operation.prepared' BEGIN SELECT RAISE(ABORT, 'injected'); END''')
        db.conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            ops.prepare(task, ActionProposal('p', 'files', 'run', ('file_A',)), expected_revisions={})
        assert task.budget.operations_started == 0
        assert db.load_tasks()[task.task_id].budget.operations_started == 0
        assert ops.operations == {} and db.load_operations() == {}
    finally:
        db.close()


def test_stale_task_snapshot_cannot_erase_new_control(tmp_path):
    db, world, events, tasks, _ = core(tmp_path / 'db')
    try:
        task = tasks.create('control', ('ok',), workspace_id='ws')
        from liandanlu_native.runtime import TaskRuntime
        stale_runtime = TaskRuntime(world, events, persistence=db)
        stale_runtime.tasks = {task.task_id: deepcopy(task)}
        tasks.control(task.task_id, TaskState.PAUSED)
        with pytest.raises(RuntimeError):
            stale_runtime.control(task.task_id, TaskState.RUNNING)
        assert db.load_tasks()[task.task_id].desired_state is TaskState.PAUSED
    finally:
        db.close()


def test_prepared_arguments_cannot_be_changed_before_dispatch(tmp_path):
    cap = Counter()
    db, _, _, tasks, ops = core(tmp_path / 'db', cap)
    try:
        _, op = task_and_op(tasks, ops)
        op.arguments['surprise'] = '/not-an-authorized-locator'
        with pytest.raises(RuntimeError):
            ops.execute(op)
        assert cap.calls == 0
        assert db.load_operations()[op.operation_id].arguments == {}
    finally:
        db.close()


def test_revoked_permission_is_rechecked_at_atomic_dispatch(tmp_path):
    cap = Counter()
    db, _, _, tasks, ops = core(tmp_path / 'db', cap)
    try:
        _, op = task_and_op(tasks, ops)
        db.conn.execute("UPDATE world_entities SET permissions_json='[]' WHERE entity_id='file_A'")
        db.conn.commit()
        with pytest.raises(PermissionError):
            ops.execute(op)
        assert cap.calls == 0
        assert db.load_operations()[op.operation_id].state is OperationState.PREPARED
    finally:
        db.close()


def test_losing_lease_during_model_call_does_not_erase_new_owner_state(tmp_path):
    db, world, _, tasks, ops = core(tmp_path / 'db')
    try:
        scheduler = TaskScheduler(tasks)
        task = tasks.create('work', ('ok',), workspace_id='ws')
        scheduler.submit(task.task_id)
        lease = scheduler.claim_next('old', lease_seconds=30)
        snapshot = []
        class TransferModel(Model):
            def next_action(self, manifest):
                db.conn.execute("UPDATE task_leases SET owner_id='new',generation=generation+1 WHERE task_id=?", (task.task_id,))
                db.conn.execute("UPDATE tasks SET state='WAITING', wait_reason='new-owner', revision=revision+1 WHERE task_id=?", (task.task_id,))
                db.conn.commit()
                snapshot.append(deepcopy(db.load_tasks()[task.task_id]))
                return super().next_action(manifest)
        runner = NativeAgentRunner(tasks, ops, TransferModel(), PolicyEngine({
            'ws': WorkspacePolicy(allowed_capabilities=frozenset({'files'}))}))
        with pytest.raises(RuntimeError):
            runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
        assert db.load_tasks()[task.task_id] == snapshot[0]
        assert ops.operations == {}
    finally:
        db.close()


def test_verified_recovery_releases_previously_pinned_resources(tmp_path):
    request = ResourceRequest(memory_mb=800)
    cap = Counter(fail=True, applied=True)
    db, world, _, tasks, ops = core(tmp_path / 'db', cap, request)
    try:
        task = tasks.create('heavy', ('ok',), workspace_id='ws')
        tasks.control(task.task_id, TaskState.RUNNING)
        broker = ResourceBroker(ResourceCapacity(100, 1000, 100), persistence=db)
        lease = broker.acquire(task.task_id, 'worker', request, lease_seconds=30)
        op = ops.prepare(task, ActionProposal('p', 'files', 'run', ('file_A',)),
                         expected_revisions={}, resource_lease=lease)
        ops.execute(op)
        assert not broker.release(lease)
        report = RecoveryCoordinator(tasks, ops).recover()
        assert op.state is OperationState.VERIFIED
        assert cap.calls == 1
        assert report['resources_released'] == 1
        assert broker.acquire('other', 'other', request) is not None
    finally:
        db.close()


def test_live_owner_prepared_operation_is_not_abandoned(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        scheduler = TaskScheduler(tasks)
        task = tasks.create('live', ('ok',), workspace_id='ws')
        scheduler.submit(task.task_id)
        lease = scheduler.claim_next('live', lease_seconds=30)
        op = ops.prepare(task, ActionProposal('p', 'files', 'run', ('file_A',)),
                         expected_revisions={}, task_lease=lease)
        RecoveryCoordinator(tasks, ops).recover()
        assert op.state is OperationState.PREPARED
    finally:
        db.close()


@pytest.mark.parametrize('amount', [-1, float('nan'), float('inf'), True])
def test_invalid_resource_quantities_fail_closed(amount):
    with pytest.raises(ValueError):
        broker = ResourceBroker(ResourceCapacity(100, 1000, 100))
        broker.acquire('task', 'owner', ResourceRequest(memory_mb=amount))


def test_failed_result_commit_keeps_durable_unknown_recovery_path(tmp_path):
    cap = Counter()
    db, _, _, tasks, ops = core(tmp_path / 'db', cap)
    try:
        _, op = task_and_op(tasks, ops)
        db.conn.execute('''CREATE TRIGGER abort_observed BEFORE UPDATE ON operations
            WHEN NEW.state='OBSERVED' BEGIN SELECT RAISE(ABORT, 'injected'); END''')
        db.conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            ops.execute(op)
        assert cap.calls == 1
        assert db.load_operations()[op.operation_id].state is OperationState.RUNNING
        db.conn.execute('DROP TRIGGER abort_observed')
        db.conn.commit()
        ops.recover_operation(op)
        assert cap.calls == 1
        assert op.state is OperationState.VERIFIED
    finally:
        db.close()
