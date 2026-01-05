#!/bin/bash
# run_autoselfcal_fixed.sh
# Usage: run_autoselfcal.sh <mous_id> <db_path> <terminal_logfile_path> \
# <autoselfcal_entry_script_path> <autoselfcal_directory_path> <NCORES> <MEM_GB>

set -uo pipefail

MOUS_ID="$1"
DB_PATH="$2"
TERMINAL_LOGFILE_PATH_PREFIX="$3"
AUTOSELFCAL_ENTRY_SCRIPT_PATH="$4"
AUTOSELFCAL_DIRECTORY_PATH="$5"
NCORES="$6"
MEM_GB="$7"

# create timestamped terminal logfile path
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
TERMINAL_LOGFILE_PATH="${TERMINAL_LOGFILE_PATH_PREFIX}-$TIMESTAMP.log"

# redirect all output (stdout and stderr) to terminal logfile AND console
exec > >(tee -a "$TERMINAL_LOGFILE_PATH") 2>&1

echo "[INFO] Starting autoselfcal for MOUS ID: $MOUS_ID"
echo "[INFO] Logging to: $TERMINAL_LOGFILE_PATH"

echo "[INFO] Using fixed resource session with NCORES=$NCORES and MEM_GB=$MEM_GB"

# Move into the autoselfcal working directory
cd "$AUTOSELFCAL_DIRECTORY_PATH"

echo "[INFO] Changed working directory to: $(pwd)"

# running casa with auto_selfcal.py
mpicasa -n "$NCORES" casa --pipeline --nologger --nogui -c "$AUTOSELFCAL_ENTRY_SCRIPT_PATH"
