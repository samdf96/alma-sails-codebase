"""
reset_admit.py
----------------
Recovery script for a failed or killed ADMIT line-identification run.

Unlike selfcal, an ADMIT run doesn't touch the MS or hold expensive
irreplaceable state -- the safest recovery is deleting its (possibly
partially-written) project directory outright and resetting the
line_admit_unit to 'ready' so it can be re-run cleanly from scratch. This
matches the "delete the project directory and rerun clean" fallback used
during interactive development: ADMIT's own restore-from-XML path (parsing
an existing admit.xml to resume a partial run) is under-exercised and
history has shown it can crash instead, so trying to resume a killed run in
place is not worth the risk when a fresh Ingest_AT re-run only costs
roughly a few tens of seconds on these cube sizes.

Any line_admit_detection rows already written by a run that got far enough
to write them are left in place -- this script only touches
line_admit_pipeline_state and the on-disk project directory.

The unit must be in 'failed' or 'running' state.

Usage (inside a CASA pipeline container, or the VM shell -- no CASA needed
since this script only does file/DB cleanup, not ADMIT itself):
    casa --nologger -c alma-sails-codebase/scripts/reset_admit.py \\
        --mous-id uid://A001/X.../X... --source EC_53 --spw 3

Optional:
    --db-path /path/to/db
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DB_PATH

_RECOVERABLE_STATES = {"failed", "running"}


def main(mous_id: str, source: str, spw: int, db_path: Path) -> None:
    conn = db.get_connection(db_path)

    triage_row = db.fetch_one(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? AND spw_idx=?",
        (mous_id, source, spw),
    )
    if triage_row is None:
        print(f"[ERROR] No line_triage_unit for mous_id={mous_id} source={source} spw={spw}")
        sys.exit(1)

    admit_row = db.get_line_admit_unit_for_triage_unit(conn, triage_row["line_triage_unit_id"])
    if admit_row is None:
        print(f"[ERROR] No line_admit_unit for mous_id={mous_id} source={source} spw={spw}")
        sys.exit(1)

    current_state = admit_row["current_state"]
    db.require_state(current_state, _RECOVERABLE_STATES)

    triage_products_list = json.loads(triage_row["triage_products"])
    cube_fits = Path(db.to_local(triage_products_list[-1]["cube_fits"]))
    # Matches admit.Project's own default naming, also used by
    # line_admit_utils.run_admit_lineid: <cube_fits without extension>.admit
    projdir = cube_fits.with_suffix(".admit")

    print(f"[reset-admit] mous_id  : {mous_id}")
    print(f"[reset-admit] source   : {source}")
    print(f"[reset-admit] spw      : {spw}")
    print(f"[reset-admit] state    : {current_state}")
    print(f"[reset-admit] projdir  : {projdir}")

    if projdir.exists():
        shutil.rmtree(projdir)
        print(f"[reset-admit] removed  : {projdir}")
    else:
        print(f"[reset-admit] projdir not present on disk — nothing to remove")

    db.record_line_admit_transition(
        conn, admit_row["line_admit_unit_id"], from_state=current_state, to_state="ready",
        note="reset after failed/killed ADMIT run — project directory removed", step="reset_admit",
    )
    conn.close()

    print(f"[OK]   {mous_id} {source} spw={spw} → ready (ready to retry ADMIT)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset a failed/killed ADMIT line-identification run and restore 'ready' state."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--source", required=True, help="Target/field name within this MOUS")
    parser.add_argument("--spw", type=int, required=True, help="spw index of the line_triage_unit")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()

    main(mous_id=args.mous_id, source=args.source, spw=args.spw, db_path=Path(args.db_path))
