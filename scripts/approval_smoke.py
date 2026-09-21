#!/usr/bin/env python3
"""Offline approval/restart smoke with real subprocess death and fsync file effects.

Only a TemporaryDirectory is modified. A scripted model is deliberately used:
this verifies authorization/execution, not live LLM or macOS UI integration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.approvals import ApprovalService
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import ActionProposal, Entity, OperationState, RiskClass
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime, TaskScheduler
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel


def append(path, value):
    with Path(path).open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + '\n')
        handle.flush()
        os.fsync(handle.fileno())


class Model:
    def __init__(self, folder):
        self.folder = folder

    def next_action(self, manifest):
        append(self.folder / 'model_calls.jsonl', {'task': manifest.task_id})
        return ActionProposal('reviewed-proposal', 'draft', 'append', ('draft_A',),
                              {'content': '只写入本次批准的内容'})


class FileAdapter:
    def __init__(self, folder, crash=False):
        self.folder, self.crash = folder, crash

    def execute(self, op, locators):
        append(locators[0], {'operation_id': op.operation_id, 'content': op.arguments['content']})
        if self.crash:
            os._exit(73)
        return {'ok': True}

    def verify(self, op, result):
        path = self.folder / 'effects.jsonl'
        rows = [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines()] if path.exists() else []
        matches = [r for r in rows if r['operation_id'] == op.operation_id and r['content'] == op.arguments['content']]
        return [{'claim': 'exact_approved_content_once', 'status': 'pass' if len(matches) == 1 else 'fail',
                 'source': 'direct_file_read', 'matching_rows': len(matches)}]

    def reconcile(self, op, locators):
        return self.verify(op, {})[0]['status'] == 'pass', {'ok': True}


def core(folder, *, initial=False, crash=False):
    db = SQLiteStore(folder / 'engine.db')
    world = WorldModel(persistence=db) if initial else db.load_world_model()
    if initial:
        world.register(Entity('draft_A', 'file', 'workspace_A', str(folder / 'effects.jsonl'),
                              permissions=frozenset({'write'})))
    events = EventStore(persistence=db)
    tasks = TaskRuntime(world, events, persistence=db)
    tasks.tasks = db.load_tasks()
    registry = CapabilityRegistry()
    registry.register(ActionSpec('draft', 'append', RiskClass.MUTATING, 'write',
        allowed_arguments=frozenset({'content'}), required_arguments=frozenset({'content'}),
        revision_domains=frozenset({'workspace'})))
    adapter = FileAdapter(folder, crash)
    ops = OperationRuntime(world, events, registry, capabilities={'draft': adapter}, persistence=db)
    ops.operations = db.load_operations()
    policy = PolicyEngine({'workspace_A': WorkspacePolicy(allowed_capabilities=frozenset({'draft'}))})
    service = ApprovalService(db, registry, policy, human_principals=frozenset({'test-human'}))
    runner = NativeAgentRunner(tasks, ops, Model(folder), policy, approvals=service)
    return db, world, tasks, ops, service, runner


def stage(folder, action):
    db, world, tasks, ops, service, runner = core(folder, initial=action == 'plan', crash=action == 'effect-crash')
    try:
        if action == 'plan':
            task = tasks.create('append approved draft', ('exact_approved_content_once',), workspace_id='workspace_A')
            scheduler = TaskScheduler(tasks)
            scheduler.submit(task.task_id)
            lease = scheduler.claim_next('planner')
            runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
            item = service.outstanding(task.task_id)
            assert item and item['state'] == 'PENDING'
            (folder / 'request.json').write_text(json.dumps(item), encoding='utf-8')
            os._exit(73)
        item = json.loads((folder / 'request.json').read_text(encoding='utf-8'))
        if action == 'approve':
            service.decide(item['approval_id'], approve=True, principal_id='test-human',
                           workspace_id='workspace_A', request_digest=item['request_digest'])
            os._exit(73)
        task = tasks.tasks[item['task_id']]
        lease = TaskScheduler(tasks).claim_next('executor')
        assert lease is not None
        if action == 'prepare-crash':
            proposal, expected = service.approved_action(item['approval_id'])
            ops.prepare(task, proposal, expected_revisions=expected, task_lease=lease, approval_id=item['approval_id'])
            os._exit(73)
        runner.step(task, build_manifest(task, world, ReferentStack()), task_lease=lease)
    finally:
        db.close()


def run_case(root, case):
    folder = root / case
    folder.mkdir()
    source = folder / 'original.txt'
    source.write_text('保留原文件，不覆盖。', encoding='utf-8')
    original_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    actions = ['plan', 'approve', {'normal': 'execute', 'after_effect': 'effect-crash',
                                   'after_prepare': 'prepare-crash'}[case]]
    exits = []
    for action in actions:
        child = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker', action,
                                '--folder', str(folder)], capture_output=True, text=True, timeout=20)
        expected_exit = 0 if action == 'execute' else 73
        if child.returncode != expected_exit:
            raise RuntimeError(f'{action}: {child.returncode}\n{child.stdout}\n{child.stderr}')
        exits.append(child.returncode)
    db, world, tasks, ops, service, runner = core(folder)
    try:
        # Test harness has reaped the worker; emulate supervisor-confirmed owner death.
        # A production engine must not expire arbitrary live workers this way.
        with db.conn:
            db.conn.execute('UPDATE task_leases SET lease_until=0')
        recovery = RecoveryCoordinator(tasks, ops)
        recovery.recover()
        op = next(iter(ops.operations.values()))
        effects = (folder / 'effects.jsonl')
        rows = effects.read_text(encoding='utf-8').splitlines() if effects.exists() else []
        models = (folder / 'model_calls.jsonl').read_text(encoding='utf-8').splitlines()
        count = 0 if case == 'after_prepare' else 1
        expected_state = OperationState.CANCELLED if case == 'after_prepare' else OperationState.VERIFIED
        assert len(rows) == count and len(models) == 1 and op.state is expected_state
        assert hashlib.sha256(source.read_bytes()).hexdigest() == original_hash
        assert db.conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert db.conn.execute("SELECT COUNT(*) FROM approval_requests WHERE state='CONSUMED'").fetchone()[0] == 1
        ops.events.flush_outbox()
        return {'case': case, 'child_exit_codes': exits, 'model_calls': len(models),
                'effects': len(rows), 'operation_state': op.state.value,
                'original_preserved': True, 'sqlite_integrity': 'ok', 'passed': True}
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker')
    parser.add_argument('--folder', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.worker:
        stage(args.folder, args.worker)
        return
    with tempfile.TemporaryDirectory(prefix='liandanlu-approval-') as tmp:
        cases = [run_case(Path(tmp), case) for case in ['normal', 'after_effect', 'after_prepare']]
    report = {'all_passed': all(c['passed'] for c in cases), 'model': 'scripted_offline',
              'api_calls': 0, 'filesystem': 'temporary', 'cases': cases}
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + '\n', encoding='utf-8')
    print(text)


if __name__ == '__main__':
    main()
