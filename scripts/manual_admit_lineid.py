"""
manual_admit_lineid.py
------------------------
User-facing CASA script for ADMIT line identification on one completed
line-triage cube.

Run from a CANFAR c-casa container:

    casa --nologger --nogui -c alma-sails-codebase/scripts/manual_admit_lineid.py \\
        --mous-id uid://A001/X87a/X9a7 \\
        --source OO_Ser \\
        --spw 3 \\
        [--force] \\
        [--dry-run] \\
        [--db-path /arc/.../db/serpens_v2.db]

For the given (mous_id, source, spw), the parent line_triage_unit must
already be 'complete'. Runs ADMIT's Ingest -> CubeStats -> CubeSum ->
CubeSpectrum -> LineID -> LineCube -> Moment flow (see
alma_ops.imaging.line_admit_utils.run_admit_lineid) on that unit's triage
cube, writes each identified line to line_admit_detection (review_status
starts 'pending' -- see review_line_detections.py at the project root),
and advances the line_admit_unit to 'complete'.

Re-running a unit (--force, or a retry after a failed run) replaces its
detections rather than adding a second set beside them: the old rows are
deleted and the new ones inserted in one transaction once ADMIT succeeds,
and the replaced rows are kept in the run's line_admit_run_log payload. A
unit with any human-reviewed detection (confirmed, or rejected other than
by review_line_detections.py --reject-below) or one with a deep
line-imaging unit is refused before ADMIT runs -- a re-run would discard
that review, so it has to be dealt with by hand first.

Deletes ADMIT's own duplicate copy of the input cube (*-incube.im) after a
successful run -- measured at ~99% of the triage cube's size, pure
duplication since the source triage cube is untouched. Does NOT delete the
triage cube itself; that only happens after review (see
line_admit_utils.purge_triage_cube_if_reviewed).
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually run ADMIT line identification for one line-triage unit."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS id (e.g. 'uid://A001/X.../X...')")
    parser.add_argument("--source", required=True, help="Target/field name within this MOUS")
    parser.add_argument("--spw", type=int, required=True, help="spw index of the line_triage_unit to run")
    parser.add_argument("--force", action="store_true", help="Re-run even if already 'complete'")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without running ADMIT")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    # --- Validate the parent line_triage_unit ---
    triage_row = db.fetch_one(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? AND spw_idx=?",
        (args.mous_id, args.source, args.spw),
    )
    if triage_row is None:
        print(f"[ERROR] No line_triage_unit for mous_id={args.mous_id} source={args.source} spw={args.spw}")
        conn.close()
        sys.exit(1)
    if triage_row["current_state"] != "complete":
        print(f"[ERROR] line_triage_unit is in state '{triage_row['current_state']}'; expected 'complete'.")
        conn.close()
        sys.exit(1)

    triage_products_list = json.loads(triage_row["triage_products"])
    cube_fits = Path(db.to_local(triage_products_list[-1]["cube_fits"]))
    if not cube_fits.exists():
        print(f"[ERROR] Triage cube not found on disk: {cube_fits}")
        conn.close()
        sys.exit(1)

    target_row = next(
        (r for r in db.get_mous_targets(conn, args.mous_id) if r["alma_source_name"] == args.source),
        None,
    )
    if target_row is None or target_row["vlsr_kms"] is None:
        print(f"[ERROR] No vlsr_kms set for source '{args.source}' — run migrate_add_admit_tables.py "
              f"and check targets.vlsr_kms.")
        conn.close()
        sys.exit(1)
    vlsr_kms = target_row["vlsr_kms"]

    print(f"[admit-lineid] {args.mous_id}  source={args.source}  spw={args.spw}")
    print(f"  cube_fits : {cube_fits}")
    print(f"  vlsr_kms  : {vlsr_kms}")

    if args.dry_run:
        print("[OK] dry-run — nothing executed.")
        conn.close()
        return

    line_triage_unit_id = triage_row["line_triage_unit_id"]
    unit_id = db.get_or_create_line_admit_unit(conn, line_triage_unit_id)
    ips = db.get_line_admit_pipeline_state(conn, unit_id)
    current_state = ips["current_state"]

    if current_state == "running":
        print("[SKIP] already running.")
        conn.close()
        return
    if current_state == "complete" and not args.force:
        print("[SKIP] already complete (use --force to re-run).")
        conn.close()
        return

    blocking = db.get_line_admit_detections_blocking_rerun(conn, unit_id)
    if blocking:
        print(f"[ERROR] This unit has {len(blocking)} reviewed or deep-imaged detection(s); "
              f"re-running ADMIT would replace them and lose that review:")
        for r in blocking:
            extra = f", line_imaging_unit {r['line_imaging_unit_id']}" if r["line_imaging_unit_id"] else ""
            print(f"  det {r['line_admit_detection_id']}  {r['formula']}  {r['frequency_ghz']:.4f} GHz  "
                  f"{r['review_status']}{extra}")
        print("Deal with these by hand (record the reviews, remove any line-imaging unit) before re-running.")
        conn.close()
        sys.exit(1)

    run_id = str(uuid.uuid4())
    db.record_line_admit_transition(
        conn, unit_id, from_state=current_state, to_state="running",
        run_id=run_id, note="manual run", step="start",
    )

    try:
        from alma_ops.imaging.line_admit_utils import (
            parse_lltable, purge_incube_copy, run_admit_lineid,
        )

        result = run_admit_lineid(cube_fits, vlsr_kms)
        detections = parse_lltable(result["lltable_path"])
        replaced = db.replace_line_admit_detections(conn, unit_id, detections)
        purge_incube_copy(result["projdir"])
    except Exception:
        err = traceback.format_exc()
        print(f"[ERROR]\n{err}", flush=True)
        db.record_line_admit_failure(
            conn, unit_id, from_state="running", error=err[-4000:],
            run_id=run_id, step="admit_lineid",
        )
        conn.close()
        sys.exit(1)

    admit_products = {
        "projdir": result["projdir"],
        "lltable_path": result["lltable_path"],
        "n_detections": len(detections),
    }
    payload = {"admit_products": admit_products}
    if replaced:
        payload["replaced_detections"] = replaced
    db.record_line_admit_transition(
        conn, unit_id, from_state="running", to_state="complete",
        run_id=run_id,
        note="admit line-id complete" + (f" (replaced {len(replaced)} earlier detection(s))"
                                         if replaced else ""),
        step="complete",
        admit_products=admit_products,
        payload=payload,
    )
    conn.close()

    print(f"[OK] {len(detections)} line(s) identified, written to line_admit_detection (review_status='pending').")


if __name__ == "__main__":
    main()
