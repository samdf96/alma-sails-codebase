#!/usr/bin/env bash
# headless_line_imaging.sh
# --------------------------
# CANFAR headless session entry point for deep (niter>0) line imaging of one
# confirmed ADMIT detection. Runs inside a CASA pipeline (c-casa) container.
#
# Positional args (passed via session.create args=):
#   $1  detection_id  line_admit_detection_id
#   $2  db_path       Canonical path to the pipeline SQLite database
#   $3  force         (optional) "1" to re-run a unit already 'complete'
#   $4  ncores        (optional) mpicasa cores; 0/unset = flexible/single-threaded
#   $5  robust        (optional) Briggs robust weighting; unset = script default (0.5).
#                     Output filenames carry the robust value (_r05 / _r20), so the
#                     same detection can be imaged at several weightings side by side.
#
# Unlike ADMIT (no MPI awareness at all), deep line imaging genuinely threads
# parallel= through to tclean, so this branches on ncores the same way
# headless_imaging.sh does for continuum.

set -euo pipefail

DETECTION_ID="$1"
DB_PATH="$2"
FORCE="${3:-}"
NCORES="${4:-0}"
ROBUST="${5:-}"

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-line-imaging] detection_id : $DETECTION_ID"
echo "[headless-line-imaging] db_path      : $DB_PATH"
echo "[headless-line-imaging] force        : ${FORCE:-0}"
echo "[headless-line-imaging] ncores       : $NCORES"
echo "[headless-line-imaging] robust       : ${ROBUST:-default}"

EXTRA_ARGS=()
if [ "$FORCE" = "1" ]; then
    EXTRA_ARGS+=(--force)
fi
if [ -n "$ROBUST" ]; then
    EXTRA_ARGS+=(--robust "$ROBUST")
fi

if [ "$NCORES" -gt 1 ]; then
    mpicasa -n "$NCORES" casa --nologger --nogui \
        -c "${CODEBASE_DIR}/scripts/manual_line_imaging.py" \
        --detection-id "${DETECTION_ID}" \
        --db-path "${DB_PATH}" \
        --ncores "${NCORES}" \
        "${EXTRA_ARGS[@]}"
else
    casa --nologger --nogui \
        -c "${CODEBASE_DIR}/scripts/manual_line_imaging.py" \
        --detection-id "${DETECTION_ID}" \
        --db-path "${DB_PATH}" \
        "${EXTRA_ARGS[@]}"
fi
