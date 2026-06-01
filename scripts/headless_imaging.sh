#!/bin/bash
# Headless CANFAR session entrypoint for continuum imaging.
# Called by imaging_flow.py with the canonical path to a JSON payload file.
#
# Usage (inside a c-casa container):
#   bash headless_imaging.sh /arc/.../imaging/continuum/EC_53/B6_12m/imaging_payload.json
set -e

PAYLOAD="$1"

if [ -z "$PAYLOAD" ]; then
    echo "ERROR: payload path argument required" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NCORES=$(python3 -c "import json; print(json.load(open('$PAYLOAD')).get('ncores', 1))")

if [ "$NCORES" -gt 1 ]; then
    mpicasa -n "$NCORES" casa -c "${SCRIPT_DIR}/headless_continuum_imaging.py" "$PAYLOAD"
else
    casa --nologger --nogui -c "${SCRIPT_DIR}/headless_continuum_imaging.py" "$PAYLOAD"
fi