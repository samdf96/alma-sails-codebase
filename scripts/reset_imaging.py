"""
reset_imaging.py
-----------------
Recovery script for an imaging unit orphaned in 'running' with no activity
logged -- the launching job crashed or was killed without ever calling
record_imaging_failure(). Confirm via imaging_run_log that nothing is
actually in flight before using this; it does not check CANFAR itself.

Resets the imaging unit to ready so it can be retried cleanly:
  1. Delete partial tclean products (<field>_B<band>_<config>_rob*_cont.*)
     left behind in imaging/continuum/<source>/B<band>_<config>/
  2. Reset imaging_pipeline_state to 'ready' (contributing MOUSes are
     already selfcal_complete, so it does not need to go back to 'pending')

Refuses to act on a unit with imaging_run_log entries unless --force is
given, since that means a job did report back and this may not be an
orphan.

Usage:
    python alma-sails-codebase/scripts/reset_imaging.py --source Ser_S68N --band 3 --config 12m
    python alma-sails-codebase/scripts/reset_imaging.py --source Ser_S68N --band 3 --config 12m --dry-run
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import CONTINUUM_IMAGING_DIR, DB_PATH

_RECOVERABLE_STATES = {"running"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--band", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                         help="reset even if imaging_run_log has entries for this unit")
    args = parser.parse_args()

    conn = db.get_connection(DB_PATH)

    unit = db.get_imaging_unit(conn, args.source, args.band, args.config)
    if unit is None:
        sys.exit(f"No imaging_unit for ({args.source}, B{args.band}, {args.config})")

    state = db.get_imaging_pipeline_state(conn, unit["imaging_unit_id"])
    db.require_state(state["current_state"], _RECOVERABLE_STATES)

    log_count = conn.execute(
        "SELECT COUNT(*) FROM imaging_run_log WHERE imaging_unit_id=?",
        (unit["imaging_unit_id"],),
    ).fetchone()[0]
    if log_count and not args.force:
        sys.exit(
            f"imaging_run_log has {log_count} entry(ies) for this unit -- "
            "it may not be an orphan. Re-check, or pass --force to reset anyway."
        )

    img_dir = CONTINUUM_IMAGING_DIR / args.source / f"B{args.band}_{args.config}"
    prefix = f"{args.source}_B{args.band}_{args.config}_rob"
    to_delete = sorted(img_dir.glob(f"{prefix}*")) if img_dir.is_dir() else []

    print(f"[reset-imaging] imaging_unit_id={unit['imaging_unit_id']}  state={state['current_state']!r}  "
          f"run_log entries={log_count}")
    print(f"[reset-imaging] partial products in {img_dir}:")
    for p in to_delete:
        print(f"  {p.name}")
    if not to_delete:
        print("  (none)")

    if args.dry_run:
        print("[reset-imaging] [DRY RUN] would delete the above and reset state to 'ready'")
        conn.close()
        return

    for p in to_delete:
        shutil.rmtree(p) if p.is_dir() else p.unlink()

    db.record_imaging_transition(
        conn,
        imaging_unit_id=unit["imaging_unit_id"],
        from_state="running",
        to_state="ready",
        note="reset: orphaned running state with no run_log activity; partial products cleaned",
        step="reset_imaging",
    )
    conn.close()
    print(f"[OK] {args.source} B{args.band} {args.config} reset to 'ready'")


if __name__ == "__main__":
    main()
