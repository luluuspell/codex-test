import json
from pathlib import Path
import subprocess
import sys

import pytest

from liandanlu_native.approvals import ApprovalError
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.models import ActionProposal, TaskState
from liandanlu_native.runtime import TaskScheduler
from test_durable_approvals import core, request, decide, prepare


def test_real_process_crash_approval_smoke():
    script = Path(__file__).resolve().parents[1] / 'scripts' / 'approval_smoke.py'
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report['all_passed'] and report['api_calls'] == 0
    assert len(report['cases']) == 3
    assert all(case['model_calls'] == 1 for case in report['cases'])
    assert [case['effects'] for case in report['cases']] == [1, 1, 0]


@pytest.mark.parametrize('approved', [False, True])
def test_revoke_stops_queue_and_replanning(tmp_path, approved):
    c = core(tmp_path / 'db')
    try:
        task, item, lease = request(c)
        if approved:
            decide(c[5], item)
        c[5].revoke(item['approval_id'], principal_id='human-owner', workspace_id='ws',
                    request_digest=item['request_digest'])
        c[2].refresh(task.task_id)
        assert task.state is TaskState.BLOCKED
        assert TaskScheduler(c[2]).claim_next('after-revoke') is None
        c[4].step(task, build_manifest(task, c[1], ReferentStack()))
        assert c[7].calls == 1 and c[6].calls == 0
    finally:
        c[0].close()


def test_expired_approval_never_replans_in_agent(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, lease = request(c)
        with c[0].conn:
            c[0].conn.execute('UPDATE approval_requests SET expires_at=0 WHERE approval_id=?', (item['approval_id'],))
        c[4].step(task, build_manifest(task, c[1], ReferentStack()), task_lease=lease)
        assert task.state is TaskState.BLOCKED
        assert c[7].calls == 1 and c[6].calls == 0
        # Explicit revoke allows a later fresh reviewed request; history is retained.
        c[5].revoke(item['approval_id'], principal_id='human-owner', workspace_id='ws',
                    request_digest=item['request_digest'])
        assert c[5].outstanding(task.task_id) is None
    finally:
        c[0].close()


def test_same_proposal_id_cannot_be_reused_after_consumption(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        decide(c[5], item)
        op, _ = prepare(c, task, item)
        c[3].execute(op)
        c[2].refresh(task.task_id)
        req = item['snapshot']['request']
        proposal = ActionProposal(item['proposal_id'], req['capability'], req['action'],
                                  tuple(req['object_refs']), req['arguments'])
        with pytest.raises(ApprovalError, match='already decided'):
            c[5].request(task, proposal, req['expected_revisions'])
        assert c[6].calls == 1
    finally:
        c[0].close()


def test_inspection_returns_a_copy_not_the_approved_source_of_truth(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, _ = request(c)
        copy = c[5].inspect(item['approval_id'], workspace_id='ws')
        copy['snapshot']['request']['arguments']['content'] = 'changed display'
        assert c[5].inspect(item['approval_id'], workspace_id='ws')['snapshot']['request']['arguments']['content'] == 'approved content'
        with pytest.raises(PermissionError):
            c[5].inspect(item['approval_id'], workspace_id='other')
    finally:
        c[0].close()


def test_pending_approval_cannot_bind_and_does_not_consume_budget(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, lease = request(c)
        req = item['snapshot']['request']
        proposal = ActionProposal('attempt', req['capability'], req['action'], tuple(req['object_refs']), req['arguments'])
        with pytest.raises(ApprovalError):
            c[3].prepare(task, proposal, expected_revisions=req['expected_revisions'], task_lease=lease,
                         approval_id=item['approval_id'])
        assert c[0].load_tasks()[task.task_id].budget.operations_started == 0
        assert c[0].load_operations() == {}
    finally:
        c[0].close()


def test_registered_policy_cannot_be_bypassed_by_omitting_approval_id(tmp_path):
    c = core(tmp_path / 'db')
    try:
        task, item, lease = request(c)
        req = item['snapshot']['request']
        proposal = ActionProposal('attempt', req['capability'], req['action'], tuple(req['object_refs']), req['arguments'])
        with pytest.raises(ApprovalError):
            c[3].prepare(task, proposal, expected_revisions=req['expected_revisions'], task_lease=lease)
        assert c[0].load_operations() == {} and c[6].calls == 0
    finally:
        c[0].close()
