"""
manual_download.py
------------------
Manually trigger a MOUS download from the NRAO SRDP service.

Run this from inside a c-astroml interactive session where wget2 is available.

Usage:
    python alma-sails-codebase/scripts/manual_download.py <mous_id> <srdp_url> [--db-path <path>]

Example:
    python alma-sails-codebase/scripts/manual_download.py uid://A001/X2d20/X2f95 https://srdp.nrao.edu/...
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DATASETS_DIR, DB_PATH, SRDP_WEBLOG_DIR
from alma_ops.download import run_download
from alma_ops.utils import to_dir_mous_id


def main(mous_id: str, srdp_url: str, db_path: Path) -> None:
    conn = db.get_connection(db_path)

    # ------------------------------------------------------------------
    # Pre-flight: verify the MOUS is in the right state to download
    # ------------------------------------------------------------------
    state_row = db.get_pipeline_state(conn, mous_id)
    if state_row is None:
        print(f"[ERROR] MOUS {mous_id} not found in database")
        sys.exit(1)

    current_state = state_row["current_state"]
    if current_state != "pending":
        print(f"[ERROR] Expected state 'pending', got '{current_state}' — aborting")
        sys.exit(1)

    print(f"[INFO] MOUS {mous_id} confirmed pending")
    print(f"[INFO] SRDP URL: {srdp_url}")
    print(f"[INFO] Datasets dir: {DATASETS_DIR}")

    # ------------------------------------------------------------------
    # Store the URL before starting so it's available for retries
    # ------------------------------------------------------------------
    db.update_pipeline_paths(conn, mous_id, download_url=srdp_url)

    # ------------------------------------------------------------------
    # Run the download
    # ------------------------------------------------------------------
    print(f"[INFO] Starting download...")
    result = run_download(
        mous_id=mous_id,
        srdp_url=srdp_url,
        datasets_dir=DATASETS_DIR,
        weblog_dir=SRDP_WEBLOG_DIR,
    )

    # ------------------------------------------------------------------
    # Record the outcome
    # ------------------------------------------------------------------
    if not result.success:
        print(f"[ERROR] Download failed: {result.error}")
        db.record_failure(
            conn,
            mous_id=mous_id,
            from_state="pending",
            error=result.error or "unknown",
            step="download",
        )
        sys.exit(1)

    db.update_pipeline_paths(conn, mous_id, mous_directory=result.download_path)
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state="pending",
        to_state="downloaded",
        payload={
            "srdp_url":     srdp_url,
            "size_bytes":   result.size_bytes,
            "sha1_verified": result.sha1_verified,
            "duration_s":   result.duration_s,
            "mous_dir":     str(result.download_path),
        },
        step="download",
    )

    print(f"[INFO] Download complete in {result.duration_s:.1f}s")
    print(f"[INFO] SHA1 verified: {result.sha1_verified}")
    print(f"[INFO] Size: {result.size_bytes / 1e9:.2f} GB")
    print(f"[INFO] MOUS directory: {result.download_path}")
    print(f"[OK]   {mous_id} → downloaded")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manually download a MOUS from the NRAO SRDP service.")
    parser.add_argument("mous_id",  help="MOUS ID in database form (uid://A001/...)")
    parser.add_argument("srdp_url", help="Direct URL to the SRDP data product")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()

    main(
        mous_id=args.mous_id,
        srdp_url=args.srdp_url,
        db_path=Path(args.db_path),
    )
