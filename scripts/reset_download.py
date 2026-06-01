"""
reset_download.py
-----------------
Recovery script for a failed or stuck download.

Restores the MOUS to 'pending' so the download can be retried cleanly:
  1. Delete the partial dataset directory (derived from mous_directory in the
     DB if set, otherwise inferred from the MOUS ID under datasets/)
  2. Clear all path fields and failure_count on the pipeline_state row
  3. Record the reset in state_transitions and run_log

The download_url is preserved in the database so it can be reused without
having to look it up again.

Usage (VM shell or c-astroml container):
    python alma-sails-codebase/scripts/reset_download.py --mous-id uid://A001/X.../X...

Optional:
    --db-path /path/to/db
    --dry-run  Print what would be deleted without actually deleting anything
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DATASETS_DIR, DB_PATH
from alma_ops.utils import to_dir_mous_id

_RECOVERABLE_STATES = {"failed", "downloaded", "pending"}


def main(mous_id: str, db_path: Path, dry_run: bool) -> None:
    conn = db.get_connection(db_path)

    # ------------------------------------------------------------------
    # Pre-flight
    # ------------------------------------------------------------------
    row = db.get_pipeline_state(conn, mous_id)
    if row is None:
        print(f"[ERROR] MOUS {mous_id} not found in database")
        sys.exit(1)

    current_state = row["current_state"]
    if current_state not in _RECOVERABLE_STATES:
        print(f"[ERROR] Expected state in {_RECOVERABLE_STATES}, got '{current_state}' — aborting")
        sys.exit(1)

    # Locate directories to delete:
    #   1. The final mous_directory if the DB recorded it (download completed but we want a reset)
    #   2. The final inferred path if it exists on disk
    #   3. Any leftover temp dirs — mkdtemp creates these as datasets/<mous_dir_name>_<random>
    #      and they survive a SIGKILL since cleanup only runs on clean exit
    mous_dir_name = to_dir_mous_id(mous_id)
    dirs_to_delete: list[Path] = []

    db_dir = db.get_mous_directory(conn, mous_id)
    if db_dir is not None and db_dir.exists():
        dirs_to_delete.append(db_dir)

    final_dir = DATASETS_DIR / mous_dir_name
    if final_dir.exists() and final_dir not in dirs_to_delete:
        dirs_to_delete.append(final_dir)

    # mkdtemp prefix pattern: uid___A001_X..._X..._<random>
    for candidate in DATASETS_DIR.glob(f"{mous_dir_name}_*"):
        if candidate.is_dir() and candidate not in dirs_to_delete:
            dirs_to_delete.append(candidate)

    print(f"[reset-download] MOUS      : {mous_id}")
    print(f"[reset-download] State     : {current_state}")
    if dirs_to_delete:
        for d in dirs_to_delete:
            print(f"[reset-download] Found     : {d}")
    else:
        print(f"[reset-download] Directory : (nothing found on disk)")
    if row["download_url"]:
        print(f"[reset-download] URL kept  : {row['download_url']}")

    if dry_run:
        print("[reset-download] DRY RUN — no changes will be made")

    # ------------------------------------------------------------------
    # 1. Delete all located directories
    # ------------------------------------------------------------------
    if dirs_to_delete:
        for d in dirs_to_delete:
            if dry_run:
                print(f"[reset-download] Would delete: {d}")
            else:
                shutil.rmtree(d)
                print(f"[reset-download] Deleted: {d}")
    else:
        print(f"[reset-download] No directories to delete")

    if dry_run:
        print(f"[reset-download] Would reset DB row to 'pending'")
        conn.close()
        return

    # ------------------------------------------------------------------
    # 2. Reset the pipeline_state row (preserve download_url)
    # ------------------------------------------------------------------
    conn.execute(
        """
        UPDATE pipeline_state
        SET mous_directory       = NULL,
            split_products       = NULL,
            selfcal_products     = NULL,
            selfcal_products_sub = NULL,
            job_id               = NULL,
            failure_count        = 0,
            last_transition_id   = NULL,
            updated_at           = datetime('now')
        WHERE mous_id = ?
        """,
        (mous_id,),
    )
    conn.commit()

    # ------------------------------------------------------------------
    # 3. Record the reset
    # ------------------------------------------------------------------
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state=current_state,
        to_state="pending",
        note="manual reset after failed download — partial directory removed, ready to retry",
        payload={
            "action":          "reset_download",
            "from_state":      current_state,
            "deleted_dirs":    [str(d) for d in dirs_to_delete],
            "download_url":    row["download_url"],
        },
        step="reset_download",
    )
    conn.close()

    print(f"[OK]   {mous_id} → pending (ready to retry download)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset a failed download and restore pending state."
    )
    parser.add_argument("--mous-id",  required=True,       help="MOUS ID (uid://A001/...)")
    parser.add_argument("--db-path",  default=DB_PATH,     help="Path to the SQLite database")
    parser.add_argument("--dry-run",  action="store_true", help="Print actions without executing them")
    args = parser.parse_args()

    main(mous_id=args.mous_id, db_path=Path(args.db_path), dry_run=args.dry_run)
