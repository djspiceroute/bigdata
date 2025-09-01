#!/usr/bin/env bash
set -euo pipefail
python scripts/dq_hive.py --source hive --db demo --table customers --pk id
