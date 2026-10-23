#!/usr/bin/env bash
# headless_line_triage_imaging.sh
# --------------------------------
# CANFAR headless session entry point for ADMIT-bound line-triage imaging.
# Runs inside a CASA pipeline (c-casa) container.
#
# Args (passed via session.create args=):
#   $1  mous_id    e.g. uid://A001/X.../X...
#   $2  source     Target/field name within this MOUS
#   $3  db_path    Canonical path to the pipeline SQLite database
#   $4...          Options passed through unchanged to
#                  manual_line_triage_imaging.py, e.g.
#                    --linewidth-floor-kms 8.0 --spw 0 --force --taper-arcsec 0.15
#
# Options are named, matching manual_line_triage_imaging.py's own CLI, because
# session args arrive as one whitespace-split string: an empty optional
# positional would vanish and shift everything after it.
#
# A CASA rc file can be passed by setting CASARCFILES in the session
# environment (the flow's casarc= / a launcher's --casarc). CASA reads it
# itself; this script only reports it. Without one, tclean sizes its cube
# chunks from the host's RAM rather than the session's -- see
# docs/reference/casa_rc.md and the presets in casarc/.

set -euo pipefail

MOUS_ID="$1"
SOURCE="$2"
DB_PATH="$3"
shift 3

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-line-triage] mous_id     : $MOUS_ID"
echo "[headless-line-triage] source      : $SOURCE"
echo "[headless-line-triage] db_path     : $DB_PATH"
echo "[headless-line-triage] options     : ${*:-<none>}"
if [ -n "${CASARCFILES:-}" ]; then
    echo "[headless-line-triage] CASARCFILES : $CASARCFILES"
else
    echo "[headless-line-triage] CASARCFILES : <unset> -- WARNING: tclean will size cube chunks" \
         "from the host's RAM, not this session's; large cubes may be killed (docs/reference/casa_rc.md)"
fi

casa --nologger --nogui \
    -c "${CODEBASE_DIR}/scripts/manual_line_triage_imaging.py" \
    --mous-id "${MOUS_ID}" \
    --source "${SOURCE}" \
    --db-path "${DB_PATH}" \
    "$@"
