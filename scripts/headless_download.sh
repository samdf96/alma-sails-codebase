#!/usr/bin/env bash
# headless_download.sh
# --------------------
# CANFAR headless session entry point for the download step.
# Runs inside the c-astroml container where wget2 is available.
#
# Positional args (passed via session.create args=):
#   $1  mous_id     e.g. uid://A001/X.../X...
#   $2  srdp_url    NRAO SRDP download URL
#   $3  db_path     Canonical path to the pipeline SQLite database

set -euo pipefail

MOUS_ID="$1"
SRDP_URL="$2"
DB_PATH="$3"

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-download] mous_id : $MOUS_ID"
echo "[headless-download] db_path : $DB_PATH"

python3 "${CODEBASE_DIR}/scripts/manual_download.py" \
    --mous-id  "${MOUS_ID}" \
    --srdp-url "${SRDP_URL}" \
    --db-path  "${DB_PATH}"
