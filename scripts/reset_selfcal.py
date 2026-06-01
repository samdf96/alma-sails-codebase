"""
reset_selfcal.py
----------------
Recovery script for a failed or killed autoselfcal run.

Restores the MOUS to split_complete so autoselfcal can be retried cleanly:
  1. Remove CORRECTED_DATA column from any partially-processed MSes in auto_selfcal/
  2. Delete all intermediate selfcal products from auto_selfcal/
  3. Move the clean input MSes back to splits/
  4. Reset the MOUS state to split_complete

Run this when autoselfcal fails due to insufficient RAM or an unexpected kill.
The MOUS must be in 'failed' or 'selfcal_running' state.

Usage (inside a CASA pipeline container):
    casa --nologger -c alma-sails-codebase/scripts/reset_selfcal.py --mous-id uid://A001/X.../X...

Optional:
    --db-path /path/to/db
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
from datetime import datetime

from casatools import table as tbtool

from alma_ops import db
from alma_ops.config import DB_PATH

_RECOVERABLE_STATES = {"failed", "selfcal_running"}


def _remove_corrected_data(ms_path: Path) -> None:
    """Remove CORRECTED_DATA column from an MS if present."""
    tb = tbtool()
    try:
        tb.open(str(ms_path), nomodify=False)
        if "CORRECTED_DATA" in tb.colnames():
            tb.removecols(["CORRECTED_DATA"])
            print(f"[reset-selfcal] removed CORRECTED_DATA: {ms_path.name}")
        else:
            print(f"[reset-selfcal] no CORRECTED_DATA in {ms_path.name}")
    finally:
        tb.close()


def _cleanup_autoselfcal(autoselfcal_dir: Path, input_ms: list[Path]) -> int:
    """Delete everything in auto_selfcal/ except the input MSes. Returns item count deleted."""
    keep = set(input_ms)
    deleted = 0
    for item in list(autoselfcal_dir.iterdir()):
        if item in keep:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
        deleted += 1
    return deleted


def main(mous_id: str, db_path: Path) -> None:
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

    mous_dir = db.get_mous_directory(conn, mous_id)
    if mous_dir is None:
        print(f"[ERROR] mous_directory not set for {mous_id}")
        sys.exit(1)

    autoselfcal_dir = mous_dir / "auto_selfcal"
    if not autoselfcal_dir.exists():
        print(f"[ERROR] auto_selfcal/ not found at {autoselfcal_dir}")
        sys.exit(1)

    # The original split MSes were moved into auto_selfcal/ before selfcal ran.
    # They are the only *_targets.ms directories (not *.selfcal.ms, *.corrected.ms, etc.).
    input_ms = sorted(
        p for p in autoselfcal_dir.iterdir()
        if p.is_dir() and p.name.endswith("_targets.ms")
    )
    if not input_ms:
        print(f"[ERROR] No *_targets.ms found in {autoselfcal_dir} — nothing to recover")
        sys.exit(1)

    print(f"[reset-selfcal] MOUS      : {mous_id}")
    print(f"[reset-selfcal] State     : {current_state}")
    print(f"[reset-selfcal] Input MSes ({len(input_ms)}):")
    for ms in input_ms:
        print(f"    {ms.name}")

    log_path = mous_dir / f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-reset-selfcal.log"
    casalog.setlogfile(str(log_path))  # noqa: F821 — casalog is a CASA global
    print(f"[reset-selfcal] CASA log  : {log_path}")

    # ------------------------------------------------------------------
    # 1. Remove CORRECTED_DATA from each input MS
    # ------------------------------------------------------------------
    print(f"[reset-selfcal] Removing CORRECTED_DATA columns...")
    for ms in input_ms:
        _remove_corrected_data(ms)

    # ------------------------------------------------------------------
    # 2. Delete all intermediate selfcal products from auto_selfcal/
    # ------------------------------------------------------------------
    deleted = _cleanup_autoselfcal(autoselfcal_dir, input_ms)
    print(f"[reset-selfcal] auto_selfcal/ cleanup: removed {deleted} item(s)")

    # ------------------------------------------------------------------
    # 3. Move clean MSes back to splits/
    # ------------------------------------------------------------------
    splits_dir = mous_dir / "splits"
    splits_dir.mkdir(exist_ok=True)

    moved = []
    for ms in input_ms:
        dest = splits_dir / ms.name
        if dest.exists():
            print(f"[reset-selfcal] WARNING: {ms.name} already exists in splits/ — skipping")
            continue
        ms.rename(dest)
        moved.append(dest)
        print(f"[reset-selfcal] moved → splits/{ms.name}")

    if not moved:
        print(f"[ERROR] No MSes were moved to splits/ — aborting DB reset")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4. Transition back to split_complete
    # ------------------------------------------------------------------
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state=current_state,
        to_state="split_complete",
        note="reset after failed selfcal — CORRECTED_DATA removed, MSes restored to splits/",
        step="reset_selfcal",
    )
    conn.close()

    print(f"[OK]   {mous_id} → split_complete (ready to retry selfcal)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset a failed selfcal run and restore split_complete state."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()

    main(mous_id=args.mous_id, db_path=Path(args.db_path))
