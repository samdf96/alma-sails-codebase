#!/usr/bin/env bash
# headless_line_imaging.sh
# --------------------------
# CANFAR headless session entry point for deep (niter>0) line imaging of one
# confirmed ADMIT detection. Runs inside a CASA pipeline (c-casa) container.
#
# Args (passed via session.create args=):
#   $1  detection_id  line_admit_detection_id
#   $2  db_path       Canonical path to the pipeline SQLite database
#   $3...             Options passed through unchanged to manual_line_imaging.py,
#                     e.g. --force --ncores 4 --robust 2.0
#
# Options are named, matching manual_line_imaging.py's own CLI, because session
# args arrive as one whitespace-split string: an empty optional positional
# vanishes and shifts everything after it. (The earlier positional form lost
# --robust and ran ncores>1 jobs single-threaded whenever force was off.)
#
# Unlike ADMIT (no MPI awareness at all), deep line imaging genuinely threads
# parallel= through to tclean, so with --ncores N > 1 this runs under
# mpicasa -n N, the same way headless_imaging.sh does for continuum.
#
# A CASA rc file can be passed by setting CASARCFILES in the session
# environment (the flow's casarc=); CASA reads it itself. See
# docs/reference/casa_rc.md.

set -euo pipefail

DETECTION_ID="$1"
DB_PATH="$2"
shift 2

NCORES=0
prev=""
for arg in "$@"; do
    [ "$prev" = "--ncores" ] && NCORES="$arg"
    prev="$arg"
done

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-line-imaging] detection_id : $DETECTION_ID"
echo "[headless-line-imaging] db_path      : $DB_PATH"
echo "[headless-line-imaging] options      : ${*:-<none>}"
echo "[headless-line-imaging] CASARCFILES  : ${CASARCFILES:-<unset>}"

if [ "$NCORES" -gt 1 ]; then
    mpicasa -n "$NCORES" casa --nologger --nogui \
        -c "${CODEBASE_DIR}/scripts/manual_line_imaging.py" \
        --detection-id "${DETECTION_ID}" \
        --db-path "${DB_PATH}" \
        "$@"
else
    casa --nologger --nogui \
        -c "${CODEBASE_DIR}/scripts/manual_line_imaging.py" \
        --detection-id "${DETECTION_ID}" \
        --db-path "${DB_PATH}" \
        "$@"
fi
