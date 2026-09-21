#!/usr/bin/env python3
"""Bounded, logged source-tree verification; failure is never reported as success."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def stop_group(proc: subprocess.Popen) -> None:
    try:
        if os.name == 'posix':
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            proc.kill()
        proc.wait(timeout=2)
    # Also clean children which outlived an already exited group leader.
    if os.name == 'posix':
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_check(name: str, command: list[str], timeout: float, directory: Path) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()
    timer = time.monotonic()
    stdout = directory / f'{name}.stdout.log'
    stderr = directory / f'{name}.stderr.log'
    proc = None
    status = 'INFRASTRUCTURE_ERROR'
    error = None
    code = None
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    # Disable unrelated auto-loaded pytest plugins in an existing user Python install.
    env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    try:
        with stdout.open('wb') as out, stderr.open('wb') as err:
            proc = subprocess.Popen(command, cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                    stdout=out, stderr=err, start_new_session=(os.name == 'posix'))
            try:
                code = proc.wait(timeout=timeout)
                status = 'PASS' if code == 0 else 'TEST_FAILURE'
            except subprocess.TimeoutExpired:
                status = 'TIMEOUT'
                stop_group(proc)
                code = proc.returncode
    except (OSError, subprocess.SubprocessError) as exc:
        error = f'{type(exc).__name__}: {exc}'
        if proc is not None:
            stop_group(proc)
    result = {'check': name, 'command': command, 'status': status,
              'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
              'duration_seconds': round(time.monotonic() - timer, 3), 'exit_code': code,
              'stdout_file': stdout.name, 'stderr_file': stderr.name, 'error': error}
    (directory / f'{name}.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'{name}: {status} ({result["duration_seconds"]}s)', flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=ROOT / 'verification')
    args = parser.parse_args()
    output = args.output.resolve()
    py = sys.executable
    pytest = [py, '-m', 'pytest', '-p', 'pytest_cov.plugin', '-q', '-W', 'error']
    groups = [
        ('00_compile', [py, '-m', 'compileall', '-q', 'liandanlu_native', 'scripts'], 20),
        ('10_core', pytest + ['tests/test_native_core.py', 'tests/test_native_extensions.py'], 30),
        ('20_persistence', pytest + ['tests/test_durable_memory_scheduler.py',
             'tests/test_persistence_and_policy.py', 'tests/test_runtime_integrity.py'], 40),
        ('30_execution', pytest + ['tests/test_task_leases_budgets.py',
             'tests/test_execution_fencing_resources.py', 'tests/test_recovery_integrity.py'], 45),
        ('40_bridge', pytest + ['tests/test_desktop_bridge_protocol.py'], 40),
        ('50_faults', pytest + ['tests/test_integrity_faults.py'], 90),
        ('60_regression', pytest + ['--cov=liandanlu_native', '--cov-report=term-missing',
             f'--cov-report=json:{output / "coverage.json"}', '--cov-fail-under=75',
             f'--junitxml={output / "junit.xml"}'], 120),
        ('70_real_crash_smoke', [py, 'scripts/companion_smoke.py',
             '--report', str(output / 'smoke.json')], 110),
    ]
    checks = []
    for name, command, timeout in groups:
        result = run_check(name, command, timeout, output)
        checks.append(result)
        if result['status'] != 'PASS':
            break
    report = {'all_passed': len(checks) == len(groups) and all(x['status'] == 'PASS' for x in checks),
              'python': sys.version, 'checks': checks,
              'not_run': [item[0] for item in groups[len(checks):]]}
    (output / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    raise SystemExit(0 if report['all_passed'] else 1)


if __name__ == '__main__':
    main()
