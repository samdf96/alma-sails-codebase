#!/usr/bin/env bash
# headless_admit_lineid.sh
# --------------------------
# CANFAR headless session entry point for ADMIT line identification.
# Runs inside a CASA pipeline (c-casa) container.
#
# Positional args (passed via session.create args=):
#   $1  mous_id   e.g. uid://A001/X.../X...
#   $2  source    Target/field name within this MOUS
#   $3  spw       spw index of the line_triage_unit to run
#   $4  db_path   Canonical path to the pipeline SQLite database
#   $5  force     (optional) "1" to re-run a unit already 'complete'

set -euo pipefail

MOUS_ID="$1"
SOURCE="$2"
SPW="$3"
DB_PATH="$4"
FORCE="${5:-}"

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-admit-lineid] mous_id  : $MOUS_ID"
echo "[headless-admit-lineid] source   : $SOURCE"
echo "[headless-admit-lineid] spw      : $SPW"
echo "[headless-admit-lineid] db_path  : $DB_PATH"
echo "[headless-admit-lineid] force    : ${FORCE:-0}"

EXTRA_ARGS=()
if [ "$FORCE" = "1" ]; then
    EXTRA_ARGS+=(--force)
fi

# cd into a container-local temp dir so CASA's makemask temp directories
# (__tmp_frominmask_*) are isolated per-session and don't collide on /arc
cd "$(mktemp -d)"

casa --nologger --nogui \
    -c "${CODEBASE_DIR}/scripts/manual_admit_lineid.py" \
    --mous-id "${MOUS_ID}" \
    --source "${SOURCE}" \
    --spw "${SPW}" \
    --db-path "${DB_PATH}" \
    "${EXTRA_ARGS[@]}"
