"""Black-box regressions: a nonempty evidence string is not completion proof."""
from hashlib import sha256
import json

import pytest

from liandanlu_native.models import GoalClaim, TaskState
from test_recovery_integrity import core, task_and_op


def receipt_ref(op):
    encoded = json.dumps(op.evidence[0], sort_keys=True, separators=(',', ':'),
                         ensure_ascii=False, allow_nan=False).encode('utf-8')
    return 'evidence:v1:' + op.operation_id + ':' + sha256(encoded).hexdigest()


def test_invented_reference_cannot_complete_a_task(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task, op = task_and_op(tasks, ops)
        ops.execute(op)
        tasks.evaluate_goal(task.task_id, {'ok': GoalClaim('ok', True, ('invented',))})
        assert task.state is not TaskState.COMPLETED
    finally:
        db.close()


def test_another_tasks_verified_receipt_cannot_complete_this_task(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        target = tasks.create('different target', ('ok',), workspace_id='ws')
        tasks.control(target.task_id, TaskState.RUNNING)
        tasks.evaluate_goal(target.task_id, {'ok': GoalClaim('ok', True, (receipt_ref(op),))})
        assert target.state is not TaskState.COMPLETED
    finally:
        db.close()


def test_another_workspaces_verified_receipt_is_rejected(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        _, op = task_and_op(tasks, ops)
        ops.execute(op)
        target = tasks.create('other workspace', ('ok',), workspace_id='other')
        tasks.control(target.task_id, TaskState.RUNNING)
        tasks.evaluate_goal(target.task_id, {'ok': GoalClaim('ok', True, (receipt_ref(op),))})
        assert target.state is not TaskState.COMPLETED
    finally:
        db.close()


@pytest.mark.parametrize('desired', [TaskState.PAUSED, TaskState.CANCELLED])
def test_goal_verdict_cannot_override_user_control(tmp_path, desired):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task, op = task_and_op(tasks, ops)
        ops.execute(op)
        tasks.control(task.task_id, desired)
        before = db.load_tasks()[task.task_id]
        tasks.evaluate_goal(task.task_id, {'ok': GoalClaim('ok', True, (receipt_ref(op),))})
        after = db.load_tasks()[task.task_id]
        assert after.state is before.state
        assert after.desired_state is desired
        assert after.revision == before.revision
    finally:
        db.close()


def test_prepared_operation_prevents_goal_completion(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task, _ = task_and_op(tasks, ops)
        tasks.evaluate_goal(task.task_id, {'ok': GoalClaim('ok', True, ('not-executed',))})
        assert task.state is not TaskState.COMPLETED
    finally:
        db.close()


def test_truthy_non_boolean_pass_is_not_proof(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task, op = task_and_op(tasks, ops)
        ops.execute(op)
        tasks.evaluate_goal(task.task_id, {'ok': GoalClaim('ok', 'yes', (receipt_ref(op),))})
        assert task.state is not TaskState.COMPLETED
    finally:
        db.close()


def test_valid_but_different_claim_is_not_proof(tmp_path):
    db, _, _, tasks, ops = core(tmp_path / 'db')
    try:
        task, op = task_and_op(tasks, ops)
        ops.execute(op)
        # Trusted host changes acceptance criteria, not the recorded verifier claim.
        task.success_criteria = ('published',)
        task.revision += 1
        tasks._record(task, 'task.criteria_changed')
        tasks.evaluate_goal(task.task_id, {
            'published': GoalClaim('published', True, (receipt_ref(op),))})
        assert task.state is not TaskState.COMPLETED
    finally:
        db.close()
