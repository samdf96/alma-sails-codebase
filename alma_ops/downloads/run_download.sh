#!/bin/bash
# run_download.sh
# Usage: run_download.sh <download_dir> <url>

set -euo pipefail

DOWNLOAD_DIR="$1"
URL="$2"

# Move into the download directory
cd "$DOWNLOAD_DIR"

# Confirm the directory change
echo "[INFO] Changed working directory to: $(pwd)"

# Run wget2 command
wget2 -r -l 10 --reject-regex="index.html*" --progress=bar -np -nH --cut-dirs=3 "$URL"