#!/bin/bash
# run_autoselfcal_prep.sh
# Usage: run_autoselfcal_prep.sh <mous_id> <db_path> <destination_directory> \
# <files_to_transfer>

set -euo pipefail

MOUS_ID="$1"
DB_PATH="$2"
DESTINATION_DIRECTORY="$3"
shift 3

if (( $# == 0 )); then
    echo "[ERROR] No files specified for transfer."
    exit 1
fi

echo "[INFO] Starting autoselfcal prep for MOUS ID: $MOUS_ID"
echo "[INFO] Destination directory: $DESTINATION_DIRECTORY"
echo "Sources:"
printf "  %s\n" "$@"

for SRC in "$@"; do
    if [[ ! -d "$SRC" ]]; then
        echo "[ERROR] Source directory does not exist: $SRC"
        exit 1
    fi

    echo "Copying $(basename "$SRC") to $DESTINATION_DIRECTORY"

    rsync -a \
        --info=progress2 \
        "$SRC" \
        "$DESTINATION_DIRECTORY"/
done

# update database to 'split' state
sqlite3 "$DB_PATH" "UPDATE pipeline_state SET selfcal_status = 'prepped' WHERE mous_id = '$MOUS_ID';"

echo "[INFO] Autoselfcal prep completed for MOUS ID: $MOUS_ID"
