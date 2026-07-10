#!/usr/bin/env bash
# headless_line_triage_imaging.sh
# --------------------------------
# CANFAR headless session entry point for ADMIT-bound line-triage imaging.
# Runs inside a CASA pipeline (c-casa) container.
#
# Positional args (passed via session.create args=):
#   $1  mous_id              e.g. uid://A001/X.../X...
#   $2  source               Target/field name within this MOUS
#   $3  db_path              Canonical path to the pipeline SQLite database
#   $4  linewidth_floor_kms  (optional) narrowest protected line FWHM in km/s
#   $5  spw_override         (optional) restrict to a single spw index
#   $6  force                (optional) "1" to re-image spws already complete

set -euo pipefail

MOUS_ID="$1"
SOURCE="$2"
DB_PATH="$3"
LINEWIDTH_FLOOR_KMS="${4:-}"
SPW_OVERRIDE="${5:-}"
FORCE="${6:-}"

CODEBASE_DIR="/arc/projects/ALMA-SAILS/alma-sails-codebase"

echo "[headless-line-triage] mous_id             : $MOUS_ID"
echo "[headless-line-triage] source              : $SOURCE"
echo "[headless-line-triage] db_path             : $DB_PATH"
echo "[headless-line-triage] linewidth_floor_kms : ${LINEWIDTH_FLOOR_KMS:-<default>}"
echo "[headless-line-triage] spw_override        : ${SPW_OVERRIDE:-<all line candidates>}"
echo "[headless-line-triage] force               : ${FORCE:-0}"

EXTRA_ARGS=()
if [ -n "$LINEWIDTH_FLOOR_KMS" ]; then
    EXTRA_ARGS+=(--linewidth-floor-kms "$LINEWIDTH_FLOOR_KMS")
fi
if [ -n "$SPW_OVERRIDE" ]; then
    EXTRA_ARGS+=(--spw "$SPW_OVERRIDE")
fi
if [ "$FORCE" = "1" ]; then
    EXTRA_ARGS+=(--force)
fi

casa --nologger --nogui \
    -c "${CODEBASE_DIR}/scripts/manual_line_triage_imaging.py" \
    --mous-id "${MOUS_ID}" \
    --source "${SOURCE}" \
    --db-path "${DB_PATH}" \
    "${EXTRA_ARGS[@]}"
