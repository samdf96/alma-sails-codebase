#!/usr/bin/env bash
# headless_split.sh
# -----------------
# CANFAR headless session entry point for the split step.
# Runs inside the CASA pipeline container.
#
# Positional args (passed via session.create args=):
#   $1  mous_id      e.g. uid://A001/X.../X...
#   $2  db_path      Canonical path to the pipeline SQLite database
#   $3  spw_override (optional) comma-separated SPW IDs, e.g. "21,23,25,27,29"

set -euo pipefail

MOUS_ID="$1"
DB_PATH="$2"
SPW_OVERRIDE="${3:-}"

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-split] mous_id      : $MOUS_ID"
echo "[headless-split] db_path      : $DB_PATH"
echo "[headless-split] spw_override : ${SPW_OVERRIDE:-<from alminer>}"

SPW_ARG=""
if [ -n "$SPW_OVERRIDE" ]; then
    SPW_ARG="--spw ${SPW_OVERRIDE}"
fi

casa --nologger --nologfile \
    -c "${CODEBASE_DIR}/scripts/manual_split.py" \
    --mous-id "${MOUS_ID}" \
    --db-path "${DB_PATH}" \
    ${SPW_ARG}
