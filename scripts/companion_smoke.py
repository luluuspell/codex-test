#!/usr/bin/env python3
"""Real subprocess/SQLite/filesystem crash tests, using only temporary fixtures.

No model API, external provider, GUI automation or user document is involved.
Run: python3 scripts/companion_smoke.py
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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from liandanlu_native.agent import NativeAgentRunner
from liandanlu_native.capabilities import ActionSpec, CapabilityRegistry
from liandanlu_native.context import ReferentStack, build_manifest
from liandanlu_native.events import EventStore
from liandanlu_native.models import ActionProposal, Entity, RiskClass, TaskState
from liandanlu_native.policy import PolicyEngine, WorkspacePolicy
from liandanlu_native.recovery import RecoveryCoordinator
from liandanlu_native.runtime import OperationRuntime, TaskRuntime
from liandanlu_native.storage import SQLiteStore
from liandanlu_native.world import WorldModel

PHASES = {
    'after_prepare': ('CANCELLED', 0),
    'after_dispatch_before_effect': ('UNKNOWN', 0),
    'after_effect_before_result': ('VERIFIED', 1),
    'after_result_before_verify': ('VERIFIED', 1),
    'after_verify_before_dispatch': ('VERIFIED', 1),
}
EXIT_FAULT = 73
ORIGINAL_TEXT = 'This original must not be modified.\n'


def registry():
    reg = CapabilityRegistry()
    reg.register(ActionSpec('fixture', 'append_marker', RiskClass.MUTATING, 'read',
                            revision_domains=frozenset({'workspace'})))
    return reg


def durable_text(path: Path, text: str, *, append=False):
    with path.open('a' if append else 'w', encoding='utf-8') as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())


class FileEffect:
    def __init__(self, directory: Path, fault: str | None = None):
        self.directory = directory
        self.fault = fault

    def execute(self, operation, locators):
        if self.fault == 'after_dispatch_before_effect':
            os._exit(EXIT_FAULT)
        # Deliberately non-idempotent: a replay would append a second line.
        durable_text(self.directory / 'effects.log', operation.operation_id + '\n', append=True)
        if self.fault == 'after_effect_before_result':
            os._exit(EXIT_FAULT)
        return {'ok': True, 'operation_id': operation.operation_id}

    def verify(self, operation, result):
        if self.fault == 'after_result_before_verify':
            os._exit(EXIT_FAULT)
        count = self.count(operation.operation_id)
        return [{'claim': 'marker_written_once', 'status': 'pass' if count == 1 else 'fail',
                 'count': count, 'source': 'filesystem'}]

    def count(self, operation_id):
        path = self.directory / 'effects.log'
        return path.read_text(encoding='utf-8').splitlines().count(operation_id) if path.exists() else 0

    def reconcile(self, operation, locators):
        return self.count(operation.operation_id) == 1, {'ok': self.count(operation.operation_id) == 1}


class FixtureModel:
    def next_action(self, manifest):
        return ActionProposal('fixture-proposal', 'fixture', 'append_marker', ('fixture-file',))


def worker(directory: Path, phase: str):
    source = directory / 'original.txt'
    durable_text(source, ORIGINAL_TEXT)
    db = SQLiteStore(directory / 'runtime.sqlite')
    world = WorldModel(persistence=db)
    world.register(Entity('fixture-file', 'file', 'fixture-ws', str(source)))
    events = EventStore(persistence=db)
    tasks = TaskRuntime(world, events, persistence=db)

    class FaultRuntime(OperationRuntime):
        def prepare(self, *args, **kwargs):
            op = super().prepare(*args, **kwargs)
            durable_text(directory / 'ids.json', json.dumps({'task_id': op.task_id, 'operation_id': op.operation_id}))
            if phase == 'after_prepare':
                os._exit(EXIT_FAULT)
            return op

        def execute(self, op):
            result = super().execute(op)
            if phase == 'after_verify_before_dispatch':
                os._exit(EXIT_FAULT)
            return result

    ops = FaultRuntime(world, events, registry(), capabilities={'fixture': FileEffect(directory, phase)}, persistence=db)
    task = tasks.create('Append one marker without changing the original', ('marker_written_once',),
                        workspace_id='fixture-ws')
    tasks.control(task.task_id, TaskState.RUNNING)
    policy = PolicyEngine({'fixture-ws': WorkspacePolicy(
        allowed_capabilities=frozenset({'fixture'}), confirm_risks=frozenset(),
        allow_risks=frozenset({RiskClass.MUTATING}))})
    try:
        NativeAgentRunner(tasks, ops, FixtureModel(), policy, require_task_lease=False).step(
            task, build_manifest(task, world, ReferentStack()))
    finally:
        db.close()
    raise RuntimeError('fault point was not reached')


def run_scenarios(base: Path):
    results = []
    expected_hash = hashlib.sha256(ORIGINAL_TEXT.encode('utf-8')).hexdigest()
    for phase, (expected_state, expected_count) in PHASES.items():
        directory = base / phase
        directory.mkdir(parents=True, exist_ok=False)
        proc = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker',
                               '--directory', str(directory), '--phase', phase],
                              capture_output=True, text=True, timeout=20)
        if proc.returncode != EXIT_FAULT:
            raise RuntimeError(f'{phase}: expected injected exit {EXIT_FAULT}, got {proc.returncode}: {proc.stderr}')
        ids = json.loads((directory / 'ids.json').read_text(encoding='utf-8'))
        source = directory / 'original.txt'
        before_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        cap = FileEffect(directory)
        before_count = cap.count(ids['operation_id'])
        db = SQLiteStore(directory / 'runtime.sqlite')
        try:
            coordinator = RecoveryCoordinator.from_store(db, registry=registry(), capabilities={'fixture': cap})
            recovery = coordinator.recover()
            op = coordinator.operations.operations[ids['operation_id']]
            after_count = cap.count(op.operation_id)
            all_events = coordinator.operations.events.all_events()
            second_flush = coordinator.operations.events.flush_outbox()
            integrity = db.conn.execute('PRAGMA integrity_check').fetchone()[0]
            preserved = expected_hash == before_hash == hashlib.sha256(source.read_bytes()).hexdigest()
            passed = (op.state.value == expected_state and before_count == after_count == expected_count
                      and preserved and not second_flush and not db.pending_outbox() and integrity == 'ok'
                      and len({ev.event_id for ev in all_events}) == len(all_events))
            results.append({'phase': phase, 'injected_exit': proc.returncode,
                            'operation_state': op.state.value, 'effects_before': before_count,
                            'effects_after': after_count, 'original_preserved': preserved,
                            'sqlite_integrity': integrity, 'recovery_status': recovery['status'],
                            'passed': passed})
        finally:
            db.close()
    return {'kind': 'real_subprocess_sqlite_filesystem', 'api_calls': 0,
            'all_passed': all(item['passed'] for item in results), 'cases': results}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--phase', choices=tuple(PHASES))
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.worker:
        if args.directory is None or args.phase is None:
            parser.error('worker requires directory and phase')
        worker(args.directory, args.phase)
        return
    with tempfile.TemporaryDirectory(prefix='liandanlu-smoke-') as directory:
        result = run_scenarios(Path(directory))
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + '\n', encoding='utf-8')
    raise SystemExit(0 if result['all_passed'] else 1)


if __name__ == '__main__':
    main()
