"""
mous_download.py
--------------------
For downloading or dry-running ALMA MOUS datasets.

Intended for MOUS entries with valid SRDP download URLs.
"""

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops when running from scripts/)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------
import argparse
import os
import shutil
import sqlite3
import sys
import tempfile

from alma_ops.config import DATASETS_DIR, DB_PATH, SRDP_WEBLOG_DIR
from alma_ops.db import (
    get_db_connection,
    get_mous_download_url,
    get_mous_expected_asdms,
)
from alma_ops.downloads.fetch import download_archive, dry_run_preview
from alma_ops.downloads.organize import organize_downloaded_files
from alma_ops.downloads.status import (
    mark_download_failure,
    mark_download_partial,
    mark_download_success,
)

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.logging import get_logger
from alma_ops.utils import to_db_mous_id, to_dir_mous_id

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = get_logger("mous_download")


# =====================================================================
# Core functionality
# =====================================================================


def process_mous(
    mous_id: str,
    conn: sqlite3.Connection,
    download_dir: str = DATASETS_DIR,
    weblog_dir: str = SRDP_WEBLOG_DIR,
    dry_run: bool = False,
):
    """Run either a dry-run or full download for a single MOUS."""

    url = get_mous_download_url(conn, mous_id)
    if not url:
        log.error(f"[{mous_id}] No download URL found in database.")
        return

    # -----------------------------------------------------------------
    # Dry-run
    # -----------------------------------------------------------------
    if dry_run:
        log.info(f"[{mous_id}] Running in DRY-RUN mode.")
        dry_run_preview(url, depth=3)
        return

    # -----------------------------------------------------------------
    # Real download
    # -----------------------------------------------------------------
    tmpdir = tempfile.mkdtemp(prefix=f"{to_dir_mous_id(mous_id)}_", dir=download_dir)

    try:
        download_archive(url, tmpdir, mous_id)

        asdm_paths, moved_ms, moved_weblog = organize_downloaded_files(
            mous_id, tmpdir, download_dir, weblog_dir
        )

        if moved_ms and moved_weblog:
            shutil.rmtree(tmpdir)
            mark_download_success(
                conn,
                mous_id,
                download_path=os.path.join(download_dir, to_dir_mous_id(mous_id)),
                asdm_paths=asdm_paths,
            )

        else:
            missing = []
            if not moved_ms:
                missing.append(".ms files")
            if not moved_weblog:
                missing.append("weblog_restore directory")

            expected_count = get_mous_expected_asdms(conn, mous_id)
            mark_download_partial(
                conn,
                mous_id,
                download_path=tmpdir,
                asdm_paths=asdm_paths,
                expected_count=expected_count,
            )

    except Exception as e:
        mark_download_failure(conn, mous_id, e)
        log.error(f"[{mous_id}] Download failed: {e}")
        raise


def process_many_mous(
    mous_ids: list[str],
    conn: sqlite3.Connection,
    download_dir: str = DATASETS_DIR,
    weblog_dir: str = SRDP_WEBLOG_DIR,
    dry_run: bool = False,
):
    """Sequentially process each MOUS."""
    log.info(f"Starting processing for {len(mous_ids)} MOUS IDs...")

    for mid in mous_ids:
        log.info(f"→ Processing MOUS: {mid}")
        process_mous(mid, conn, download_dir, weblog_dir, dry_run)

    log.info("✅ All MOUS processed sequentially.")


# =====================================================================
# Command-line interface
# =====================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ALMA-SAILS MOUS downloader.")
    parser.add_argument("mous", nargs="+", help="MOUS IDs to download.")
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument("--download-dir", default=DATASETS_DIR)
    parser.add_argument("--weblog-dir", default=SRDP_WEBLOG_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # ------------------------------------------------------------
    # Echo back arguments to user before proceeding
    # ------------------------------------------------------------
    print("\n================= ARGUMENT SUMMARY =================")
    print(f"MOUS ID(s):          {args.mous}")
    print(f"Database path:       {args.db_path}")
    print(f"Download directory:  {args.download_dir}")
    print(f"Weblog directory:    {args.weblog_dir}")
    print(f"Dry run:             {args.dry_run}")
    print("====================================================\n")

    # ------------------------------------------------------------
    # Confirmation
    # ------------------------------------------------------------
    # Use a loop so accidental keysmashes don’t approve accidentally
    while True:
        confirm = input("Proceed with these settings? [y/N]: ").strip().lower()

        if confirm in ("y", "yes"):
            print("Confirmed — continuing.\n")
            break
        elif confirm in ("n", "no", ""):
            print("Aborted by user.")
            sys.exit(1)
        else:
            print("Please answer with 'y' or 'n'.")

    with get_db_connection(args.db_path) as conn:
        process_many_mous(
            mous_ids=args.mous,
            conn=conn,
            download_dir=args.download_dir,
            weblog_dir=args.weblog_dir,
            dry_run=args.dry_run,
        )
