#!/usr/bin/env python3
"""Build and re-open a verified Native Core source snapshot after verification.

Only tracked repository files and explicitly generated test reports are included.
There is no upload, credential access, deployment or model invocation here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import tomllib
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def git(*args: str) -> bytes:
    return subprocess.check_output(['git', *args], cwd=ROOT, timeout=15)


def safe_file(relative: str) -> Path:
    posix = PurePosixPath(relative)
    if posix.is_absolute() or '..' in posix.parts:
        raise ValueError(f'unsafe archive entry: {relative}')
    path = ROOT / relative
    if path.is_symlink() or not path.resolve().is_relative_to(ROOT):
        raise ValueError(f'symlink or out-of-tree file: {relative}')
    if path.name == '.env' or path.suffix in {'.sqlite', '.db', '.pem', '.key'} or '.git' in path.parts:
        raise ValueError(f'runtime/credential file must not be distributed: {relative}')
    return path


def main():
    evidence = ROOT / 'verification'
    report = json.loads((evidence / 'verification.json').read_text(encoding='utf-8'))
    smoke = json.loads((evidence / 'smoke.json').read_text(encoding='utf-8'))
    if not report.get('all_passed') or not smoke.get('all_passed'):
        raise RuntimeError('refusing to package a failed or incomplete verification')
    version = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))['project']['version']
    commit = git('rev-parse', 'HEAD').decode().strip()
    tracked = [name for name in git('ls-files', '-z').decode().split('\0') if name]
    payload = {name: safe_file(name).read_bytes() for name in tracked}
    for path in sorted(evidence.rglob('*')):
        if path.is_file() and not path.is_symlink():
            relative = path.relative_to(ROOT).as_posix()
            payload[relative] = safe_file(relative).read_bytes()
    meta = {'project': 'Liandanlu Native Companion Core', 'version': version,
            'source_commit': commit, 'created_at': datetime.now(timezone.utc).isoformat(),
            'scope': 'Complete tracked Native Core repository, not the original full frontend/video application',
            'ci_run_id': os.getenv('GITHUB_RUN_ID'), 'source_files': len(tracked),
            'api_calls_in_smoke': 0}
    payload['PACKAGE_META.json'] = (json.dumps(meta, ensure_ascii=False, indent=2) + '\n').encode()
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in sorted(payload.items())}
    payload['MANIFEST.sha256'] = ''.join(f'{digest}  {name}\n' for name, digest in hashes.items()).encode()
    out = ROOT / 'dist'
    out.mkdir(exist_ok=True)
    folder = f'Liandanlu_Native_Core_{version}'
    destination = out / f'{folder}_Full_Source.zip'
    with zipfile.ZipFile(destination, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, content in sorted(payload.items()):
            archive.writestr(f'{folder}/{name}', content)
    with zipfile.ZipFile(destination) as archive:
        if archive.testzip() is not None:
            raise RuntimeError('ZIP CRC check failed')
        for name, digest in hashes.items():
            if hashlib.sha256(archive.read(f'{folder}/{name}')).hexdigest() != digest:
                raise RuntimeError(f'archive hash mismatch: {name}')
        with tempfile.TemporaryDirectory(prefix='liandanlu-package-check-') as tmp:
            archive.extractall(tmp)
            unpacked = Path(tmp) / folder
            command = [sys.executable, str(unpacked / 'scripts' / 'companion_smoke.py')]
            result = subprocess.run(command, cwd=unpacked, capture_output=True, text=True, timeout=120)
            if result.returncode != 0:
                raise RuntimeError(f'unpacked source failed smoke: {result.stdout}\n{result.stderr}')
            unpacked_smoke = json.loads(result.stdout)
            if not unpacked_smoke['all_passed']:
                raise RuntimeError('unpacked source smoke incomplete')
    junit = ET.parse(evidence / 'junit.xml')
    suites = list(junit.iter('testsuite'))
    coverage = json.loads((evidence / 'coverage.json').read_text(encoding='utf-8'))
    summary = {**meta, 'archive': destination.name,
               'sha256': hashlib.sha256(destination.read_bytes()).hexdigest(),
               'bytes': destination.stat().st_size, 'manifest_files_checked': len(hashes),
               'zip_crc_valid': True, 'unpacked_smoke_passed': True,
               'tests': sum(int(item.get('tests', 0)) for item in suites),
               'failures': sum(int(item.get('failures', 0)) + int(item.get('errors', 0)) for item in suites),
               'skipped': sum(int(item.get('skipped', 0)) for item in suites),
               'statement_coverage_percent': coverage['totals']['percent_covered'],
               'crash_cases': smoke['cases']}
    (out / 'PACKAGE_REPORT.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (out / f'{destination.name}.sha256').write_text(f'{summary["sha256"]}  {destination.name}\n', encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
