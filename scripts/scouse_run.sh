#!/bin/bash
# scouse_run.sh
# -------------
# Entry point for all SCOUSE + ACORNS pipeline stages.  Sets the required
# LD_LIBRARY_PATH for PyQt5 (the GUIs) and invokes the correct stage script via
# the structure_id_env venv python.  Every stage script takes --unit-id N, or
# --source with --line-label.
#
# Run from the project root, from inside a CASA container terminal.  Stage 1 is
# always a GUI (Desktop session).  Stages 2 and 4 are headless unless given
# --interactive (then they need a Desktop session too); stage 3, acorns and pdf
# are always headless.  batch_scouse.py runs stages 2-4 and acorns as CANFAR
# headless sessions (via headless_scouse.sh), so by hand you normally only run
# stage 1:
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage 1 --source S68Nc --line-label "HC3N(10-9)"
#
#   alma-sails-codebase/scripts/scouse_run.sh --stage 2 --unit-id 2 [--interactive]
#   alma-sails-codebase/scripts/scouse_run.sh --stage 3 --unit-id 2 [--njobs 4]
#   alma-sails-codebase/scripts/scouse_run.sh --stage 4 --unit-id 2 [--interactive]
#   alma-sails-codebase/scripts/scouse_run.sh --stage acorns --unit-id 2
#
#   # summary PDF of a complete unit (ACORNS already writes it)
#   alma-sails-codebase/scripts/scouse_run.sh --stage pdf --unit-id 2

set -euo pipefail

export LD_LIBRARY_PATH=/arc/projects/ALMA-SAILS/tools/xcb-libs/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

VENV=/arc/projects/ALMA-SAILS/structure_id_env/.venv/bin/python3
SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Extract --stage; pass everything else straight through to the stage script
STAGE=""
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage) STAGE="$2"; shift 2 ;;
        *)       PASSTHROUGH+=("$1"); shift ;;
    esac
done

if [[ -z "$STAGE" ]]; then
    echo "[ERROR] --stage is required (1|2|3|4|acorns|pdf)" >&2
    exit 1
fi

case "$STAGE" in
    1)      exec "$VENV" "$SCRIPTS/manual_scouse_stage1.py" "${PASSTHROUGH[@]}" ;;
    2)      exec "$VENV" "$SCRIPTS/manual_scouse_stage2.py" "${PASSTHROUGH[@]}" ;;
    3)      exec "$VENV" "$SCRIPTS/manual_scouse_stage3.py" "${PASSTHROUGH[@]}" ;;
    4)      exec "$VENV" "$SCRIPTS/manual_scouse_stage4.py" "${PASSTHROUGH[@]}" ;;
    acorns) exec "$VENV" "$SCRIPTS/manual_acorns_run.py"   "${PASSTHROUGH[@]}" ;;
    pdf)    exec "$VENV" "$SCRIPTS/scouse_summary_pdf.py"  "${PASSTHROUGH[@]}" ;;
    *)
        echo "[ERROR] Unknown stage '$STAGE'. Must be one of: 1 2 3 4 acorns pdf" >&2
        exit 1
        ;;
esac
