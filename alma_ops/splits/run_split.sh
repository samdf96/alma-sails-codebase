#!/bin/bash
# run_split.sh
# Usage: run_split.sh <mous_id> <db_path> <terminal_logfile_path> \
# <casa_driver_script_path> <casa_logfile_path> <json_payload_path>

set -uo pipefail

# -------------------------
# Positional arguments
# -------------------------
MOUS_ID="$1"
DB_PATH="$2"
TERMINAL_LOGFILE_PATH_PREFIX="$3"
CASA_DRIVER_SCRIPT_PATH="$4"
CASA_LOGFILE_PATH="$5"
JSON_PAYLOAD_PATH="$6"

# create timestamped terminal logfile path
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
TERMINAL_LOGFILE_PATH="${TERMINAL_LOGFILE_PATH_PREFIX}-$TIMESTAMP.log"

# redirect all output (stdout and stderr) to terminal logfile AND console
exec > >(tee -a "$TERMINAL_LOGFILE_PATH") 2>&1

echo "[INFO] Starting split for MOUS ID: $MOUS_ID"
echo "[INFO] Logging to: $TERMINAL_LOGFILE_PATH"
echo "[INFO] CASA logging to: $CASA_LOGFILE_PATH"

# Run casa_driver.py script
casa --logfile "$CASA_LOGFILE_PATH" -c "$CASA_DRIVER_SCRIPT_PATH" --json-payload "$JSON_PAYLOAD_PATH"

# update database to 'split' state
# sqlite3 "$DB_PATH" "UPDATE pipeline_state SET pre_selfcal_split_status = 'split' WHERE mous_id = '$MOUS_ID';"

echo "[INFO] Split process completed for MOUS ID: $MOUS_ID"
