"""Regression cases discovered by reviewing the a8 execution/recovery paths."""
from copy import deepcopy
import threading

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import (
    ActionProposal, Entity, OperationState, ResourceCapacity, ResourceRequest,
    RiskClass, TaskBudget, TaskState, now,
)
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.resources import ResourceBroker
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


class Counter:
    def __init__(self, *, fail=False, applied=True):
        self.calls = 0
        self.fail = fail
        self.applied = applied

    def execute(self, op, locators):
        self.calls += 1
        if self.fail:
            raise TimeoutError('reply lost; effect may still be active')
        return {'ok': True}

    def verify(self, op, result):
        return [{'claim': 'ok', 'status': 'pass' if result.get('ok') else 'fail'}]

    def reconcile(self, op, locators):
        return self.applied, {'ok': self.applied}


class Model:
    def __init__(self):
        self.calls = 0

    def next_action(self, manifest):
        self.calls += 1
        return ActionProposal('p', 'files', 'run', ('file_A',))


def registry(request=None):
    reg = CapabilityRegistry()
    reg.register(ActionSpec(
        'files', 'run', RiskClass.READ, 'read',
        revision_domains=frozenset({'workspace'}),
        resource_request=request or ResourceRequest(),
    ))
    return reg


def core(path, cap=None, request=None, restore=False):
    db = SQLiteStore(path)
    world = db.load_world_model() if restore else WorldModel(persistence=db)
    if not restore:
        world.register(Entity('file_A', 'file', 'ws', '/fixture/a.txt'))
    events = EventStore(persistence=db)
    tasks = TaskRuntime(world, events, persistence=db)
    tasks.tasks = db.load_tasks()
    ops = OperationRuntime(world, events, registry(request),
                           capabilities={'files': cap or Counter()}, persistence=db)
    ops.operations = db.load_operations()
    return db, world, events, tasks, ops


def task_and_op(tasks, ops, *, budget=None, resource=None):
    task = tasks.create('work', ('ok',), workspace_id='ws', budget=budget)
    tasks.control(task.task_id, TaskState.RUNNING)
    op = ops.prepare(task, ActionProposal('p', 'files', 'run', ('file_A',)),
                     expected_revisions={'workspace': ops.world.workspace_revision('ws')},
                     resource_lease=resource)
    return task, op


def test_verified_operation_cannot_execute_twice(tmp_path):
    cap = Counter()
    db, _, _, tasks, ops = core(tmp_path / 'db', cap)
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        with pytest.raises(RuntimeError):
            ops.execute(op)
        assert cap.calls == 1
        assert db.load_operations()[op.operation_id].state is OperationState.VERIFIED
    finally:
        db.close()


def test_ambiguous_reconcile_is_not_a_definite_failure(tmp_path):
    cap = Counter(fail=True, applied=False)
    db, _, _, tasks, ops = core(tmp_path / 'db', cap)
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        ops.recover_operation(op)
        assert op.state is OperationState.UNKNOWN
        assert cap.calls == 1
    finally:
        db.close()


def test_unknown_operation_prevents_fake_cancel_in_recovery(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db', Counter(fail=True, applied=False))
    try:
        task, op = task_and_op(tasks, ops)
        ops.execute(op)
        tasks.wait(task.task_id, 'operation:unknown')
        tasks.control(task.task_id, TaskState.CANCELLED)
        RecoveryCoordinator(tasks, ops).recover()
        assert op.state is OperationState.UNKNOWN
        assert task.desired_state is TaskState.CANCELLED
        assert task.state is TaskState.CANCELLING
    finally:
        db.close()


def test_missing_provider_does_not_abort_other_recovery(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db', Counter(fail=True))
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        ops.capabilities.clear()
        RecoveryCoordinator(tasks, ops).recover()
        assert op.state is OperationState.UNKNOWN
        assert 'unavailable' in (op.error or '').lower()
    finally:
        db.close()


def test_reconcile_applied_still_requires_verifier(tmp_path):
    class LyingReceipt(Counter):
        def reconcile(self, op, locators):
            return True, {'ok': False}
    db, _, _, tasks, ops = core(tmp_path / 'db', LyingReceipt(fail=True))
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        ops.recover_operation(op)
        assert op.state is not OperationState.VERIFIED
    finally:
        db.close()


def test_stale_runner_cannot_call_model_or_change_task(tmp_path):
    db, world, _, tasks, ops = core(tmp_path / 'db')
    try:
        scheduler = TaskScheduler(tasks)
        task = tasks.create('work', ('ok',), workspace_id='ws')
        scheduler.submit(task.task_id)
        old = scheduler.claim_next('old', lease_seconds=1)
        assert old is not None
        new = scheduler.claim_next('new', lease_seconds=30, now_ts=now() + 2)
        assert new is not None
        before = deepcopy(db.load_tasks()[task.task_id])
        model = Model()
        runner = NativeAgentRunner(tasks, ops, model, PolicyEngine({
            'ws': WorkspacePolicy(allowed_capabilities=frozenset({'files'}))
        }))
        with pytest.raises(RuntimeError):
            runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=old)
        assert model.calls == 0
        assert db.load_tasks()[task.task_id] == before
    finally:
        db.close()


def test_budget_uses_durable_counter_not_stale_python_copy(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task = tasks.create('one operation', ('ok',), workspace_id='ws',
                            budget=TaskBudget(max_operations=1))
        tasks.control(task.task_id, TaskState.RUNNING)
        stale = deepcopy(task)
        kwargs = {'expected_revisions': {'workspace': ops.world.workspace_revision('ws')}}
        op = ops.prepare(task, ActionProposal('p1', 'files', 'run', ('file_A',)), **kwargs)
        ops.execute(op)
        with pytest.raises(RuntimeError):
            ops.prepare(stale, ActionProposal('p2', 'files', 'run', ('file_A',)), **kwargs)
        assert len(db.load_operations()) == 1
        assert db.load_tasks()[task.task_id].budget.operations_started == 1
    finally:
        db.close()


def test_uncertain_job_pins_resources_even_after_lease_expiry(tmp_path):
    request = ResourceRequest(memory_mb=800)
    db, world, _, tasks, ops = core(tmp_path / 'db', Counter(fail=True), request)
    try:
        task = tasks.create('heavy', ('ok',), workspace_id='ws')
        tasks.control(task.task_id, TaskState.RUNNING)
        broker = ResourceBroker(ResourceCapacity(100, 1000, 100), persistence=db)
        lease = broker.acquire(task.task_id, 'worker', request, lease_seconds=1)
        assert lease is not None
        op = ops.prepare(task, ActionProposal('p', 'files', 'run', ('file_A',)),
                         expected_revisions={'workspace': world.workspace_revision('ws')},
                         resource_lease=lease)
        ops.execute(op)
        assert op.state is OperationState.UNKNOWN
        assert broker.release(lease) is False
        assert broker.acquire('other', 'other', request, now_ts=now() + 60) is None
    finally:
        db.close()


def test_context_from_another_task_is_rejected(tmp_path):
    db, world, _, tasks, ops = core(tmp_path / 'db')
    try:
        a = tasks.create('a', ('ok',), workspace_id='ws')
        b = tasks.create('b', ('ok',), workspace_id='ws')
        model = Model()
        runner = NativeAgentRunner(tasks, ops, model, PolicyEngine(), require_task_lease=False)
        with pytest.raises(ValueError):
            runner.step(a, build_manifest(b, world, ReferentStack()))
        assert model.calls == 0
    finally:
        db.close()


def test_terminal_task_cannot_be_reactivated_by_agent(tmp_path):
    db, world, _, tasks, ops = core(tmp_path / 'db')
    try:
        task = tasks.create('done', ('ok',), workspace_id='ws')
        tasks.control(task.task_id, TaskState.CANCELLED)
        model = Model()
        runner = NativeAgentRunner(tasks, ops, model, PolicyEngine(), require_task_lease=False)
        runner.step(task, build_manifest(task, world, ReferentStack()))
        assert task.state is TaskState.CANCELLED
        assert model.calls == 0
    finally:
        db.close()


def test_two_connections_cannot_execute_same_operation(tmp_path):
    path = tmp_path / 'db'
    counter = Counter()
    db, _, _, tasks, ops = core(path, counter)
    _, op = task_and_op(tasks, ops)
    op_id = op.operation_id
    db.close()
    barrier = threading.Barrier(2)
    unexpected = []

    def worker():
        local, _, _, _, runtime = core(path, counter, restore=True)
        try:
            barrier.wait(timeout=10)
            runtime.execute(runtime.operations[op_id])
        except RuntimeError:
            pass  # Losing the compare-and-set is the expected loser result.
        except BaseException as exc:
            unexpected.append(repr(exc))
        finally:
            local.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
    assert not any(thread.is_alive() for thread in threads)
    assert unexpected == []
    assert counter.calls == 1
