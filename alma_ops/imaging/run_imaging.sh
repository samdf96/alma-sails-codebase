#!/usr/bin/env bash
# run_imaging.sh
# --------------
# Shell launcher for ALMA-SAILS continuum imaging.
#
# Launches run_continuum_imaging.py inside a headless CASA session.
# Designed to be called by the Prefect flow (mous_imaging.py) or run
# manually from the command line.
#
# Usage:
#   run_imaging.sh <payload.json> [<log_dir>]
#
# Arguments:
#   payload.json  Path to the JSON payload file (required).
#   log_dir       Directory where CASA logs are written (optional).
#                 Defaults to the parent directory of payload.json.
#
# Environment:
#   CASA_EXECUTABLE   Path to the CASA binary (default: casa)
#   ALMA_SAILS_ROOT   Root of the alma-sails-codebase checkout; added to
#                     PYTHONPATH so that alma_ops is importable inside CASA.

set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
    echo "Usage: run_imaging.sh <payload.json> [<log_dir>]" >&2
    exit 1
fi

PAYLOAD_JSON=$(realpath "$1")

if [[ ! -f "${PAYLOAD_JSON}" ]]; then
    echo "ERROR: payload file not found: ${PAYLOAD_JSON}" >&2
    exit 1
fi

LOG_DIR="${2:-$(dirname "${PAYLOAD_JSON}")}"
mkdir -p "${LOG_DIR}"

# ---------------------------------------------------------------------------
# Locate alma-sails-codebase (so alma_ops is importable inside CASA)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Expected layout: alma-sails-codebase/alma_ops/imaging/run_imaging.sh
ALMA_SAILS_ROOT="${ALMA_SAILS_ROOT:-$(realpath "${SCRIPT_DIR}/../../..")}"

# ---------------------------------------------------------------------------
# CASA executable
# ---------------------------------------------------------------------------
CASA_EXEC="${CASA_EXECUTABLE:-casa}"
if ! command -v "${CASA_EXEC}" &>/dev/null; then
    echo "ERROR: CASA not found. Set CASA_EXECUTABLE or ensure 'casa' is on PATH." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Driver script path (co-located with this shell script)
# ---------------------------------------------------------------------------
DRIVER="${SCRIPT_DIR}/run_continuum_imaging.py"
if [[ ! -f "${DRIVER}" ]]; then
    echo "ERROR: driver script not found: ${DRIVER}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Log file name: derived from payload filename + timestamp
# ---------------------------------------------------------------------------
PAYLOAD_BASE=$(basename "${PAYLOAD_JSON}" .json)
TIMESTAMP=$(date -u +"%Y%m%dT%H%M%SZ")
LOG_FILE="${LOG_DIR}/${PAYLOAD_BASE}_${TIMESTAMP}.log"

echo "[run_imaging.sh] payload  : ${PAYLOAD_JSON}"
echo "[run_imaging.sh] log      : ${LOG_FILE}"
echo "[run_imaging.sh] PYTHONPATH+: ${ALMA_SAILS_ROOT}"

# ---------------------------------------------------------------------------
# Run CASA
# ---------------------------------------------------------------------------
PYTHONPATH="${ALMA_SAILS_ROOT}${PYTHONPATH:+:${PYTHONPATH}}" \
    "${CASA_EXEC}" \
    --nologger \
    --nogui \
    --log2term \
    -c "${DRIVER}" "${PAYLOAD_JSON}" \
    2>&1 | tee "${LOG_FILE}"

EXIT_CODE="${PIPESTATUS[0]}"

if [[ "${EXIT_CODE}" -ne 0 ]]; then
    echo "[run_imaging.sh] CASA exited with code ${EXIT_CODE}" >&2
    exit "${EXIT_CODE}"
fi

echo "[run_imaging.sh] completed successfully."
