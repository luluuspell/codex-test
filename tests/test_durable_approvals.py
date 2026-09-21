"""Approval tests exercise the real SQLite/NativeAgentRunner path, not UI mocks."""
from copy import deepcopy
from dataclasses import replace
import json
import threading

import pytest

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.approvals import ApprovalError, ApprovalService, canonical
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import ActionProposal, Entity, OperationState, RiskClass, TaskState, now
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


class Adapter:
    def __init__(self, fail=False):
        self.calls = 0
        self.values = []
        self.fail = fail
        self.lock = threading.Lock()

    def execute(self, op, locators):
        with self.lock:
            self.calls += 1
            self.values.append(op.arguments['content'])
        if self.fail:
            raise TimeoutError('response lost after effect')
        return {'ok': True, 'content': op.arguments['content']}

    def verify(self, op, result):
        return [{'claim': 'content', 'status': 'pass' if result.get('ok') else 'fail'}]

    def reconcile(self, op, locators):
        return True, {'ok': True, 'content': op.arguments['content']}


class Model:
    def __init__(self):
        self.calls = 0

    def next_action(self, manifest):
        self.calls += 1
        return ActionProposal(f'p{self.calls}', 'files', 'edit', ('file_A',),
                              {'content': 'approved content' if self.calls == 1 else 'UNAPPROVED'})


def core(path, *, restore=False, adapter=None):
    store = SQLiteStore(path)
    world = store.load_world_model() if restore else WorldModel(persistence=store)
    if not restore:
        world.register(Entity('file_A', 'file', 'ws', '/test/draft.txt',
                              permissions=frozenset({'read', 'write'})))
    events = EventStore(persistence=store)
    tasks = TaskRuntime(world, events, persistence=store)
    if restore:
        tasks.tasks = store.load_tasks()
    registry = CapabilityRegistry()
    registry.register(ActionSpec('files', 'edit', RiskClass.MUTATING, 'write',
        allowed_arguments=frozenset({'content'}), required_arguments=frozenset({'content'}),
        revision_domains=frozenset({'workspace'})))
    adapter = adapter or Adapter()
    ops = OperationRuntime(world, events, registry, capabilities={'files': adapter}, persistence=store)
    if restore:
        ops.operations = store.load_operations()
    policy = PolicyEngine({'ws': WorkspacePolicy(allowed_capabilities=frozenset({'files'}))})
    approvals = ApprovalService(store, registry, policy, human_principals=frozenset({'human-owner'}))
    model = Model()
    runner = NativeAgentRunner(tasks, ops, model, policy, approvals=approvals)
    return store, world, tasks, ops, runner, approvals, adapter, model


def request(core):
    db, world, tasks, ops, runner, approvals, adapter, model = core
    task = tasks.create('edit a draft', ('content_matches',), workspace_id='ws',
                        constraints=('only approved content',))
    scheduler = TaskScheduler(tasks)
    scheduler.submit(task.task_id)
    lease = scheduler.claim_next('planner')
    runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
    item = approvals.outstanding(task.task_id)
    assert item is not None, task.wait_reason
    return task, item, lease


def decide(service, item, approve=True, **overrides):
    kwargs = dict(approve=approve, principal_id='human-owner', workspace_id='ws',
                  request_digest=item['request_digest'])
    kwargs.update(overrides)
    return service.decide(item['approval_id'], **kwargs)


def prepare(core, task, item):
    db, world, tasks, ops, runner, approvals, adapter, model = core
    tasks.refresh(task.task_id)
    lease = TaskScheduler(tasks).claim_next('executor')
    assert lease is not None
    proposal, expected = approvals.approved_action(item['approval_id'])
    op = ops.prepare(task, proposal, expected_revisions=expected, task_lease=lease,
                     approval_id=item['approval_id'])
    return op, lease


def test_request_is_durable_and_pending_does_not_call_model_again(tmp_path):
    c = core(tmp_path / 'db')
    db, world, tasks, ops, runner, service, adapter, model = c
    try:
        task, item, lease = request(c)
        assert item['state'] == 'PENDING'
        assert task.state is TaskState.WAITING
        assert task.budget.operations_started == 0 and adapter.calls == 0
        runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
        assert model.calls == 1 and ops.operations == {}
        assert db.conn.execute('SELECT COUNT(*) FROM approval_requests').fetchone()[0] == 1
        assert '/test/draft.txt' not in canonical(item['snapshot'])
    finally:
        db.close()


def test_approve_resume_uses_frozen_action_without_second_model_call(tmp_path):
    c = core(tmp_path / 'db')
    db, world, tasks, ops, runner, service, adapter, model = c
    try:
        task, item, old = request(c)
        decide(service, item)
        lease = TaskScheduler(tasks).claim_next('executor')
        assert lease is not None and lease.generation > old.generation
        runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
        assert model.calls == 1
        assert adapter.values == ['approved content']
        assert next(iter(ops.operations.values())).state is OperationState.VERIFIED
        assert service.inspect(item['approval_id'], workspace_id='ws')['state'] == 'CONSUMED'
        assert task.budget.operations_started == 1
    finally:
        db.close()


def test_restart_after_approval_preserves_action_and_receipt(tmp_path):
    path = tmp_path / 'db'
    c = core(path)
    task, item, _ = request(c)
    decide(c[5], item)
    c[0].close()
    restored = core(path, restore=True)
    db, world, tasks, ops, runner, service, adapter, model = restored
    try:
        task = tasks.tasks[task.task_id]
        lease = TaskScheduler(tasks).claim_next('after-restart')
        runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
        assert model.calls == 0
        assert adapter.values == ['approved content']
        assert next(iter(ops.operations.values())).state is OperationState.VERIFIED
        assert db.conn.execute('SELECT COUNT(*) FROM approval_operations').fetchone()[0] == 1
    finally:
        db.close()


@pytest.mark.parametrize('overrides,error', [
    ({'principal_id': 'model'}, PermissionError),
    ({'workspace_id': 'other'}, PermissionError),
    ({'request_digest': '0' * 64}, ApprovalError),
    ({'approve': 'true'}, ValueError),
])
def test_human_decision_is_scoped_and_bound_to_displayed_digest(tmp_path, overrides, error):
    c = core(tmp_path / 'db')
    try:
        _, item, _ = request(c)
        with pytest.raises(error):
            decide(c[5], item, **overrides)
        assert c[5].inspect(item['approval_id'], workspace_id='ws')['state'] == 'PENDING'
        assert c[6].calls == 0
    finally:
        c[0].close()


def test_duplicate_approval_is_idempotent_not_another_task(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        first = decide(c[5], item)
        revision = c[0].load_tasks()[task.task_id].revision
        second = decide(c[5], item)
        assert first['state'] == second['state'] == 'APPROVED'
        assert c[0].load_tasks()[task.task_id].revision == revision
        assert len(c[0].load_tasks()) == 1
        with pytest.raises(ApprovalError):
            decide(c[5], item, approve=False)
    finally:
        c[0].close()


def test_rejection_does_not_execute_or_replan(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item, approve=False)
        c[2].refresh(task.task_id)
        c[4].step(task, build_manifest(task, c[1], ReferentStack()))
        assert task.state is TaskState.BLOCKED
        assert c[6].calls == 0 and c[7].calls == 1
    finally:
        c[0].close()


@pytest.mark.parametrize('change', ['argument', 'object', 'constraint', 'policy', 'spec', 'expiry'])
def test_changed_conditions_invalidate_approval_without_budget_consumption(tmp_path, change):
    c = core(tmp_path / 'db')
    db, world, tasks, ops, runner, service, adapter, model = c
    try:
        task, item, _ = request(c)
        decide(service, item)
        tasks.refresh(task.task_id)
        lease = TaskScheduler(tasks).claim_next('executor')
        proposal, expected = service.approved_action(item['approval_id'])
        if change == 'argument':
            proposal.arguments['content'] = 'changed'
        elif change == 'object':
            with db.conn:
                db.conn.execute("UPDATE world_entities SET version=version+1 WHERE entity_id='file_A'")
        elif change == 'constraint':
            with db.conn:
                db.conn.execute("UPDATE tasks SET constraints_json='[\"different\"]' WHERE task_id=?", (task.task_id,))
        elif change == 'policy':
            service.policy.policies['ws'] = WorkspacePolicy(allowed_capabilities=frozenset({'files','other'}))
        elif change == 'spec':
            spec = ops.registry.actions[('files','edit')]
            ops.registry.actions[('files','edit')] = replace(spec, allowed_arguments=frozenset({'content','other'}))
        else:
            with db.conn:
                db.conn.execute('UPDATE approval_requests SET expires_at=0 WHERE approval_id=?', (item['approval_id'],))
        with pytest.raises((ApprovalError, PermissionError)):
            ops.prepare(task, proposal, expected_revisions=expected, task_lease=lease, approval_id=item['approval_id'])
        assert db.load_tasks()[task.task_id].budget.operations_started == 0
        assert ops.operations == {} and db.load_operations() == {}
        assert adapter.calls == 0
        assert service.inspect(item['approval_id'], workspace_id='ws')['state'] == 'APPROVED'
    finally:
        db.close()


def test_single_use_approval_cannot_authorize_a_second_operation(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, lease = prepare(c, task, item)
        c[3].execute(op)
        proposal = ActionProposal('new-proposal', 'files', 'edit', ('file_A',), {'content': 'approved content'})
        with pytest.raises(ApprovalError):
            c[3].prepare(task, proposal, expected_revisions=op.expected_revisions,
                         task_lease=lease, approval_id=item['approval_id'])
        assert c[6].calls == 1
        assert c[0].load_tasks()[task.task_id].budget.operations_started == 1
    finally:
        c[0].close()


@pytest.mark.parametrize('when', ['pending', 'approved', 'prepared'])
def test_revocation_blocks_before_dispatch(tmp_path, when):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        op = None
        if when != 'pending':
            decide(c[5], item)
        if when == 'prepared':
            op, _ = prepare(c, task, item)
        c[5].revoke(item['approval_id'], principal_id='human-owner', workspace_id='ws',
                    request_digest=item['request_digest'])
        if op:
            with pytest.raises(ApprovalError):
                c[3].execute(op)
            assert op.state is OperationState.PREPARED
        else:
            with pytest.raises(ApprovalError):
                c[5].approved_action(item['approval_id'])
        assert c[6].calls == 0
    finally:
        c[0].close()


def test_revocation_does_not_claim_to_undo_an_already_dispatched_operation(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, _ = prepare(c, task, item)
        c[3].execute(op)
        with pytest.raises(ApprovalError, match='cannot undo'):
            c[5].revoke(item['approval_id'], principal_id='human-owner', workspace_id='ws',
                        request_digest=item['request_digest'])
        assert c[6].calls == 1
    finally:
        c[0].close()


def test_policy_revocation_after_prepare_is_checked_again_before_effect(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, _ = prepare(c, task, item)
        c[5].policy.policies['ws'] = WorkspacePolicy()
        with pytest.raises(ApprovalError):
            c[3].execute(op)
        assert c[6].calls == 0 and op.state is OperationState.PREPARED
    finally:
        c[0].close()


def test_linked_operation_cannot_be_dispatched_without_approval_service(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, _ = prepare(c, task, item)
        c[3].approval_service = None
        with pytest.raises(ApprovalError):
            c[3].execute(op)
        assert c[6].calls == 0
    finally:
        c[0].close()


def test_binding_failure_rolls_back_approval_budget_operation_and_events(tmp_path):
    c = core(tmp_path / 'db')
    db = c[0]
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        c[2].refresh(task.task_id)
        lease = TaskScheduler(c[2]).claim_next('executor')
        proposal, expected = c[5].approved_action(item['approval_id'])
        count = len(db.pending_outbox())
        with db.conn:
            db.conn.execute("""CREATE TRIGGER fail_prepare BEFORE INSERT ON event_outbox
                WHEN NEW.event_type='operation.prepared' BEGIN SELECT RAISE(ABORT,'injected'); END""")
        with pytest.raises(Exception, match='injected'):
            c[3].prepare(task, proposal, expected_revisions=expected, task_lease=lease, approval_id=item['approval_id'])
        assert db.load_tasks()[task.task_id].budget.operations_started == 0
        assert db.load_operations() == {} and c[3].operations == {}
        assert db.conn.execute('SELECT COUNT(*) FROM approval_operations').fetchone()[0] == 0
        assert c[5].inspect(item['approval_id'], workspace_id='ws')['state'] == 'APPROVED'
        assert len(db.pending_outbox()) == count
    finally:
        db.close()


def test_parallel_consumers_can_only_bind_one_operation(tmp_path):
    path = tmp_path / 'db'
    c = core(path)
    task, item, _ = request(c)
    decide(c[5], item)
    c[0].close()
    barrier = threading.Barrier(2)
    successes, failures = [], []

    def worker():
        local = core(path, restore=True)
        try:
            task = local[2].tasks[item['task_id']]
            proposal, expected = local[5].approved_action(item['approval_id'])
            barrier.wait(timeout=10)
            op = local[3].prepare(task, proposal, expected_revisions=expected, approval_id=item['approval_id'])
            successes.append(op.operation_id)
        except ApprovalError:
            failures.append('consumed')
        except BaseException as exc:
            failures.append(repr(exc))
        finally:
            local[0].close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
    assert not any(t.is_alive() for t in threads)
    assert len(successes) == 1 and failures == ['consumed']
    check = SQLiteStore(path)
    try:
        assert len(check.load_operations()) == 1
        assert check.load_tasks()[task.task_id].budget.operations_started == 1
    finally:
        check.close()


def test_unknown_result_retains_consumed_grant_and_does_not_replay(tmp_path):
    c = core(tmp_path / 'db', adapter=Adapter(fail=True))
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, lease = prepare(c, task, item)
        c[3].execute(op)
        assert op.state is OperationState.UNKNOWN
        assert c[5].inspect(item['approval_id'], workspace_id='ws')['state'] == 'CONSUMED'
        c[4].step(task, build_manifest(task, c[1], ReferentStack()), task_lease=lease)
        assert c[6].calls == 1 and c[7].calls == 1
        c[3].recover_operation(op)
        assert op.state is OperationState.VERIFIED and c[6].calls == 1
    finally:
        c[0].close()


def test_approval_after_user_cancel_is_rejected(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        c[2].control(task.task_id, TaskState.CANCELLED)
        with pytest.raises(RuntimeError):
            decide(c[5], item)
        assert c[6].calls == 0
    finally:
        c[0].close()


@pytest.mark.parametrize('ttl', [0, -1, float('nan'), float('inf'), 86401])
def test_bad_approval_lifetime_is_rejected(tmp_path, ttl):
    c = core(tmp_path / 'db')
    try:
        with pytest.raises(ValueError):
            ApprovalService(c[0], c[3].registry, c[4].policy, ttl_seconds=ttl)
    finally:
        c[0].close()
