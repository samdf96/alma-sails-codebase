#!/bin/bash
# run_autoselfcal.sh
# Usage: run_autoselfcal.sh <sc_dir> <logfile>

set -euo pipefail

SC_DIR="$1"
LOGFILE="$2"

# Move into the autoselfcal working directory
cd "$SC_DIR"

# Confirm the directory change
echo "[INFO] Changed working directory to: $(pwd)"

# Run CASA with your autoselfcal script
casa --logfile "$LOGFILE" -c auto_selfcal.py