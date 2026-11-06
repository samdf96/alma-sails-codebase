#!/usr/bin/env bash
# headless_scouse.sh
# -------------------
# CANFAR headless session entry point for one SCOUSE stage (2, 3, 4) or ACORNS of
# one unit, submitted by batch_scouse.py. Runs in the CASA image (config.CASA_IMAGE),
# whose system Python 3.10 is what the structure_id_env venv was built on.
#
# Args (passed via session.create args=):
#   $1  stage        2 | 3 | 4 | acorns
#   $2  unit_id      scouse_unit_id
#   $3  db_path      Canonical path to the pipeline SQLite database
#   $4...            Options passed through to the stage script, e.g. --njobs 4
#
# The stage script is called with --batch-job: it takes over the 'submitted' job
# batch_scouse.py recorded for the unit, and records success or failure itself.
# scousepy builds matplotlib figures even when nothing is shown, and forces the Qt
# backend on import; the stage scripts switch back to Agg (scouse_utils.import_scouse).
# MPLBACKEND=Agg and QT_QPA_PLATFORM=offscreen are a backstop: Qt without a display
# aborts the process instead of raising, which would leave the job looking stale.

set -euo pipefail

STAGE="$1"
UNIT_ID="$2"
DB_PATH="$3"
shift 3

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"
export MPLBACKEND=Agg QT_QPA_PLATFORM=offscreen

echo "[headless-scouse] stage    : $STAGE"
echo "[headless-scouse] unit_id  : $UNIT_ID"
echo "[headless-scouse] db_path  : $DB_PATH"
echo "[headless-scouse] options  : ${*:-<none>}"
echo "[headless-scouse] host     : $(hostname), $(nproc) CPUs visible"

cd /arc/projects/ALMA-SAILS
exec "${CODEBASE_DIR}/scripts/scouse_run.sh" --stage "$STAGE" \
    --unit-id "$UNIT_ID" --db-path "$DB_PATH" --batch-job "$@"
