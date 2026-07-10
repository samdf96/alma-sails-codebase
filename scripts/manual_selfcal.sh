#!/usr/bin/env bash
# manual_selfcal.sh
# -----------------
# Run autoselfcal and post-selfcal cleanup from inside a CASA pipeline container.
#
# Workflow:
#   1. Pre-flight: validate split_complete state and split products in DB
#   2. Mark selfcal_running
#   3. Write a minimal auto_selfcal entry script with explicit vislist
#   4. Run mpicasa, or plain casa -c in flexible mode (handles findcont,
#      selfcal loops, applycal, uvcontsub)
#   5. Extract CORRECTED_DATA → *_targets.corrected.ms (with MD5 validation)
#   6. Record selfcal_complete with product paths
#
# After this script completes the MOUS is imaging-ready:
#   selfcal_products     → auto_selfcal/*_targets.corrected.ms
#   selfcal_products_sub → auto_selfcal/*_targets.contsub.ms
#
# Usage (inside a CASA pipeline container):
#   bash alma-sails-codebase/scripts/manual_selfcal.sh --mous-id uid://A001/X.../X...
#
# Optional:
#   --ncores 4      (default: 4; passed to mpicasa -n, e.g. mpicasa -n 4 casa -c ...)
#                   pass 0 for flexible mode: plain casa -c, no mpicasa --
#                   auto_selfcal's own MPIEnvironment check then correctly
#                   self-selects parallel=False.
#   --db-path PATH  (default: from alma_ops.config)

set -euo pipefail

MOUS_ID=""
NCORES=4
DB_PATH_ARG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mous-id)  MOUS_ID="$2";      shift 2 ;;
        --ncores)   NCORES="$2";       shift 2 ;;
        --db-path)  DB_PATH_ARG="$2";  shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$MOUS_ID" ]]; then
    echo "Usage: $0 --mous-id uid://A001/X.../X... [--ncores N] [--db-path PATH]" >&2
    exit 1
fi

CODEBASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# ---------------------------------------------------------------------------
# Pre-flight: validate state, resolve DB path and split product paths
# ---------------------------------------------------------------------------
read -r DB_PATH MOUS_DIR STATE SPLIT_JSON < <(python3 -c "
import sys, json
sys.path.insert(0, '$CODEBASE_DIR')
from alma_ops import db
from alma_ops.config import DB_PATH as DEFAULT_DB

db_path = '$DB_PATH_ARG' if '$DB_PATH_ARG' else str(DEFAULT_DB)
conn = db.get_connection(db_path)
row = db.get_pipeline_state(conn, '$MOUS_ID')
if row is None:
    print('ERROR MOUS not found', file=__import__('sys').stderr); __import__('sys').exit(1)
state = row['current_state']
# split_complete: normal manual invocation. selfcal_running: launched via
# selfcal_flow.py, which already advances the state itself before submitting
# the headless session (so an OOM-killed session leaves a recoverable marker
# instead of a stuck state) -- this script must accept both, or every
# flow-launched run fails this check deterministically (see state_transitions
# note for uid://A001/X335e/Xfb, 2026-06-26).
if state not in ('split_complete', 'selfcal_running'):
    print(f'ERROR Expected split_complete (or selfcal_running, if launched via the Prefect flow) got {state!r}', file=__import__('sys').stderr)
    __import__('sys').exit(1)
mous_dir = db.get_mous_directory(conn, '$MOUS_ID')
if mous_dir is None:
    print('ERROR mous_directory not set', file=__import__('sys').stderr); __import__('sys').exit(1)
splits = db.get_split_products(conn, '$MOUS_ID')
if not splits:
    print('ERROR no split_products in database', file=__import__('sys').stderr); __import__('sys').exit(1)
print(db_path, mous_dir, state, json.dumps([str(p) for p in splits]))
")

echo "[selfcal] mous_id  : $MOUS_ID"
if [[ "$NCORES" == "0" ]]; then
    echo "[selfcal] ncores   : flexible (no mpicasa)"
else
    echo "[selfcal] ncores   : $NCORES"
fi
echo "[selfcal] db_path  : $DB_PATH"
echo "[selfcal] mous_dir : $MOUS_DIR"
echo "[selfcal] splits:"
python3 -c "import json,sys; [print(f'    {p}') for p in json.loads(sys.argv[1])]" "$SPLIT_JSON"

# ---------------------------------------------------------------------------
# Set up auto_selfcal working directory and entry script
# ---------------------------------------------------------------------------
AUTOSELFCAL_DIR="$MOUS_DIR/auto_selfcal"
mkdir -p "$AUTOSELFCAL_DIR"

DIR_MOUS_ID="$(python3 -c "
import sys; sys.path.insert(0, '$CODEBASE_DIR')
from alma_ops.utils import to_dir_mous_id; print(to_dir_mous_id('$MOUS_ID'))
")"
TERMINAL_LOG="$MOUS_DIR/${DIR_MOUS_ID}_autoselfcal_terminal-$(date +%Y%m%d-%H%M%S).log"
ENTRY_SCRIPT="$AUTOSELFCAL_DIR/run_autoselfcal_entry.py"

python3 -c "
import json, os, shutil, sys
vislist = json.loads(sys.argv[1])
# autoselfcal builds output paths via plain string concatenation on vis,
# so it only works with bare filenames in the working directory.
# Move each split MS into auto_selfcal/ and pass just the basename.
# Moving is safe: autoselfcal reads DATA and writes CORRECTED_DATA as a
# separate column; a mid-run kill leaves DATA intact.
basenames = []
for v in vislist:
    bn = os.path.basename(v)
    dest = os.path.join('$AUTOSELFCAL_DIR', bn)
    if not os.path.exists(dest):
        shutil.move(v, dest)
        print('[selfcal] moved:', bn)
    else:
        print('[selfcal] already present (skipping move):', bn)
    basenames.append(bn)
script = '''import sys
from datetime import datetime
sys.path.append('/arc/projects/ALMA-SAILS/auto_selfcal')
from auto_selfcal import auto_selfcal
try:
    from casampi.MPIEnvironment import MPIEnvironment
    parallel = MPIEnvironment.is_mpi_enabled
except Exception:
    parallel = False
log_path = '$MOUS_DIR/casa-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '-selfcal.log'
casalog.setlogfile(log_path)
print('[selfcal] CASA log:', log_path)
vislist = ''' + repr(basenames) + '''
auto_selfcal(
    vislist,
    apply_to_target_ms=True,
    uvcontsub_target_ms=True,
    run_findcont=True,
    parallel=parallel,
)
'''
with open('$ENTRY_SCRIPT', 'w') as f:
    f.write(script)
print('[selfcal] entry script written:', '$ENTRY_SCRIPT')
print('[selfcal] vislist:', basenames)
" "$SPLIT_JSON"

# ---------------------------------------------------------------------------
# Mark as selfcal_running (already done by selfcal_flow.py before submitting
# the headless session -- only transition here for a direct manual invocation)
# ---------------------------------------------------------------------------
if [[ "$STATE" == "selfcal_running" ]]; then
    echo "[selfcal] state already selfcal_running (launched via Prefect flow)"
else
    python3 -c "
import sys; sys.path.insert(0, '$CODEBASE_DIR')
from alma_ops.db import get_connection, record_transition
conn = get_connection('$DB_PATH')
record_transition(conn, mous_id='$MOUS_ID', from_state='$STATE',
                  to_state='selfcal_running', step='selfcal')
conn.close()
print('[selfcal] state → selfcal_running')
"
fi
echo "[selfcal] To reset after abort:"
echo "  sqlite3 '$DB_PATH' \"UPDATE pipeline_state SET current_state='split_complete' WHERE mous_id='$MOUS_ID';\""

# ---------------------------------------------------------------------------
# Run mpicasa (fixed mode), or plain casa -c (flexible mode, NCORES=0) --
# auto_selfcal's own MPIEnvironment check self-selects parallel=False when
# not actually launched under MPI, so no other change is needed here.
# ---------------------------------------------------------------------------
echo "[selfcal] Terminal log: $TERMINAL_LOG"

if [[ "$NCORES" == "0" ]]; then
    echo "[selfcal] Starting casa (flexible mode, no mpicasa) ($(date))..."
    (cd "$AUTOSELFCAL_DIR" && casa --nologger --nologfile -c "$ENTRY_SCRIPT") 2>&1 | tee "$TERMINAL_LOG"
else
    echo "[selfcal] Starting mpicasa ($(date))..."
    (cd "$AUTOSELFCAL_DIR" && mpicasa -n "$NCORES" casa --nologger --nologfile -c "$ENTRY_SCRIPT") 2>&1 | tee "$TERMINAL_LOG"
fi
CASA_EXIT=${PIPESTATUS[0]}
CASA_RUNNER="mpicasa"; [[ "$NCORES" == "0" ]] && CASA_RUNNER="casa"

if [[ "$CASA_EXIT" -ne 0 ]]; then
    echo "[ERROR] $CASA_RUNNER exited with code $CASA_EXIT" >&2
    # Record failure via plain Python (no casatools needed)
    python3 -c "
import sys; sys.path.insert(0, '$CODEBASE_DIR')
from alma_ops.db import get_connection, record_failure
conn = get_connection('$DB_PATH')
record_failure(conn, mous_id='$MOUS_ID', from_state='selfcal_running',
               error='$CASA_RUNNER exited with code $CASA_EXIT', step='selfcal')
conn.close()
print('[selfcal] state → failed')
"
    exit 1
fi
echo "[selfcal] $CASA_RUNNER complete ($(date))"

# ---------------------------------------------------------------------------
# Post-selfcal: extract CORRECTED_DATA, validate, record selfcal_complete
# Must run via casa -c because casatasks/casatools require CASA's Python 3.8
# ---------------------------------------------------------------------------
POST_SELFCAL_SCRIPT="$CODEBASE_DIR/scripts/post_selfcal.py"
echo "[selfcal] Running post-selfcal (casa -c)..."
casa --nologger -c "$POST_SELFCAL_SCRIPT" --mous-id "$MOUS_ID" --db-path "$DB_PATH"
POST_EXIT=$?

if [[ "$POST_EXIT" -ne 0 ]]; then
    echo "[ERROR] post-selfcal script exited with code $POST_EXIT" >&2
    exit 1
fi
