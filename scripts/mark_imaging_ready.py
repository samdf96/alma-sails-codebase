"""
mark_imaging_ready.py
---------------------
Promote 12m and 7m imaging units from 'pending' to 'ready' when all
contributing MOUSes are selfcal_complete.

TP imaging units are skipped entirely — their workflow is not yet defined.

Usage:
    python alma-sails-codebase/scripts/mark_imaging_ready.py
    python alma-sails-codebase/scripts/mark_imaging_ready.py --dry-run
    python alma-sails-codebase/scripts/mark_imaging_ready.py --db-path /path/to/db
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH

_REQUIRED_MOUS_STATE: dict[str, str] = {
    "12m":      "selfcal_complete",
    "7m":       "selfcal_complete",
    "tp":       "tp_ready",
    "combined": "selfcal_complete",
}


def _check_readiness(conn, imaging_unit_id: int, config_type: str) -> tuple[bool, list[str]]:
    """Return (ready, blocking_descriptions).

    ready is True only when every contributing MOUS has reached the required
    state for this config_type (selfcal_complete for 12m/7m, tp_ready for TP).
    blocking_descriptions lists each MOUS that is not yet there.
    """
    required = _REQUIRED_MOUS_STATE.get(config_type, "selfcal_complete")
    rows = db.get_mous_for_imaging_unit(conn, imaging_unit_id)
    if not rows:
        return False, ["no contributing MOUSes found"]
    blocking = [
        f"{r['mous_id']} ({r['current_state']})"
        for r in rows
        if r["current_state"] != required
    ]
    return len(blocking) == 0, blocking


def main(db_path: Path, dry_run: bool) -> None:
    conn = db.get_connection(db_path)
    pending_units = db.get_imaging_units_by_state(conn, "pending")

    if not pending_units:
        print("No imaging units currently in 'pending' state.")
        conn.close()
        return

    eligible = pending_units

    print(f"Found {len(pending_units)} pending imaging unit(s).\n")

    promoted = 0
    still_pending = 0

    for row in eligible:
        uid         = row["imaging_unit_id"]
        target      = row["target_name"]
        band        = row["band"]
        config_type = row["config_type"]
        label       = f"{target}  B{band}  {config_type}"

        ready, blocking = _check_readiness(conn, uid, config_type)

        if ready:
            print(f"  [READY]   {label}")
            if not dry_run:
                db.record_imaging_transition(
                    conn,
                    imaging_unit_id=uid,
                    from_state="pending",
                    to_state="ready",
                    note="all contributing MOUSes selfcal_complete",
                    step="mark_ready",
                )
            promoted += 1
        else:
            print(f"  [PENDING] {label}")
            for b in blocking:
                print(f"              waiting: {b}")
            still_pending += 1

    prefix = "[DRY RUN] Would promote" if dry_run else "Promoted"
    print(f"\n{prefix} {promoted} imaging unit(s) to 'ready'.")
    print(f"Still pending: {still_pending}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Promote 12m/7m imaging units to ready when all contributing MOUSes are selfcal_complete."
    )
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    args = parser.parse_args()

    main(db_path=Path(args.db_path), dry_run=args.dry_run)
