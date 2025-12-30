#!/bin/bash
# run_download.sh
# Usage: run_download.sh <mous_id> <db_path> <download_dir> <url>

set -uo pipefail

# -------------------------
# Positional arguments
# -------------------------
MOUS_ID="$1"
DB_PATH="$2"
DOWNLOAD_DIR="$3"
URL="$4"

echo "[INFO] Starting download for MOUS ID: $MOUS_ID"
echo "[INFO] Database path: $DB_PATH"
echo "[INFO] Download directory: $DOWNLOAD_DIR"
echo "[INFO] URL: $URL"

# Move into the download directory
cd "$DOWNLOAD_DIR"

# Confirm the directory change
echo "[INFO] Changed working directory to: $(pwd)"

# Run wget2 command
wget2 -r -l 10 --reject-regex="index.html*" --progress=bar -np -nH --cut-dirs=3 "$URL"

# Capture exit code
WGET_EXIT_CODE=$?

echo "[INFO] wget2 exit code: $WGET_EXIT_CODE"

# Now decide what to do
if [ $WGET_EXIT_CODE -eq 0 ] || [ $WGET_EXIT_CODE -eq 8 ]; then
    if [ $WGET_EXIT_CODE -eq 8 ]; then
        echo "[WARNING] wget2 exit code 8 (server errors like 404s) but continuing..."
    else
        echo "[SUCCESS] Download completed successfully"
    fi

    # update database to 'downloaded' state
    echo "[INFO] Updating database status to 'downloaded'"
    sqlite3 "$DB_PATH" "UPDATE pipeline_state SET download_status = 'downloaded', download_completed_at = CURRENT_TIMESTAMP WHERE mous_id = '$MOUS_ID';"

    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Database updated successfully"
        exit 0
    else
        echo "[ERROR] Failed to update database"
        exit 1
    fi
else
    echo "[ERROR] wget2 failed with exit code: $WGET_EXIT_CODE"

    # mark error in database
    sqlite3 "$DB_PATH" "UPDATE pipeline_state SET download_status = 'error' WHERE mous_id = '$MOUS_ID';"

    exit $WGET_EXIT_CODE
fi
