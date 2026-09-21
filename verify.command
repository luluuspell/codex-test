#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else "Python 3.11 or newer is required")'
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python scripts/verify_groups.py
printf '\nVerification finished. Results: verification/verification.json\n'
