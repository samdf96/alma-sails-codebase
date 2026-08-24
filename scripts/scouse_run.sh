#!/bin/bash
# scouse_run.sh
# -------------
# Entry point for all SCOUSE + ACORNS pipeline stages.  Sets the required
# LD_LIBRARY_PATH for PyQt5 (stages 1, 2, 4) and invokes the correct stage
# script via the structure_id_env venv python.
#
# Run from the project root, from inside a CASA container terminal launched
# via the CANFAR Science Portal desktop session (stages 1, 2, 4 require a
# live display; stages 3 and acorns are headless and can run in any terminal):
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage 1 --source S68Nc --line-label "HC3N(10-9)"
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage 2 --source S68Nc --line-label "HC3N(10-9)"
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage 3 --source S68Nc --line-label "HC3N(10-9)"
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage 4 --source S68Nc --line-label "HC3N(10-9)"
#
#   alma-sails-codebase/scripts/scouse_run.sh \
#       --stage acorns --source S68Nc --line-label "HC3N(10-9)"

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
    echo "[ERROR] --stage is required (1|2|3|4|acorns)" >&2
    exit 1
fi

case "$STAGE" in
    1)      exec "$VENV" "$SCRIPTS/manual_scouse_stage1.py" "${PASSTHROUGH[@]}" ;;
    2)      exec "$VENV" "$SCRIPTS/manual_scouse_stage2.py" "${PASSTHROUGH[@]}" ;;
    3)      exec "$VENV" "$SCRIPTS/manual_scouse_stage3.py" "${PASSTHROUGH[@]}" ;;
    4)      exec "$VENV" "$SCRIPTS/manual_scouse_stage4.py" "${PASSTHROUGH[@]}" ;;
    acorns) exec "$VENV" "$SCRIPTS/manual_acorns_run.py"   "${PASSTHROUGH[@]}" ;;
    *)
        echo "[ERROR] Unknown stage '$STAGE'. Must be one of: 1 2 3 4 acorns" >&2
        exit 1
        ;;
esac
