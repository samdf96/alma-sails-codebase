"""
reset_scouse.py
---------------
Reset a SCOUSE unit back to a prior state so a stage can be re-run.

Run from the VM shell (no CASA or venv required):

    python alma-sails-codebase/scripts/reset_scouse.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)" \\
        --to-state prep_complete [--dry-run]
    python alma-sails-codebase/scripts/reset_scouse.py --unit-id 6 --clear-job

Valid reset targets (--to-state):
    prep_complete    re-run from stage 1
    stage1_complete  re-run from stage 2
    stage2_complete  re-run from stage 3
    stage3_complete  re-run from stage 4
    stage4_complete  re-run ACORNS

Every target precedes ACORNS, so a reset also clears the unit's ACORNS results from
the DB: its scouse_cluster rows, _acorns_outputs in parameters_json, and
n_fitted_components / n_acorns_clusters / acorns_output_path. A reset to before
stage4_complete also deletes acorns_input.dat, the component catalogue exported from
stage 4, which manual_acorns_run.py would otherwise reuse after stage 4 is re-run.

Otherwise the output directory is untouched (the .acorn trees are overwritten by the
next ACORNS run). For a clean re-run from stage 1 (e.g. a new vel_range), use
manual_scouse_prep.py --force instead: it also deletes the stage_1-4 dirs. The sN.scousepy
files are NOT deleted by a reset, and scousepy loads an existing one and skips the stage,
so to redo stage 2 delete stage_2/s2.scousepy as well (--refit reloads the old fits).

Job columns (batch_scouse.py tracking):
    --clear-job      empty the unit's job columns: a failed job, so batch_scouse.py
                     launches the stage again, or a submitted/running job whose session
                     or terminal is gone. Can be given alone (no state change).
A reset also clears them, and refuses while a job is submitted or running unless
--clear-job is given (stop that session first: canfar delete <id>).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    JOB_ACTIVE_STATUSES,
    SCOUSE_FSM_ORDER,
    advance_scouse_state,
    clear_acorns_results,
    clear_scouse_job,
    cube_stem,
    fetch_scouse_unit,
    scouse_connection,
)

VALID_TARGETS = {
    "prep_complete",
    "stage1_complete",
    "stage2_complete",
    "stage3_complete",
    "stage4_complete",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset a SCOUSE unit back to a prior FSM state."
    )
    parser.add_argument("--unit-id",    type=int, default=None, help="scouse_unit_id")
    parser.add_argument("--source",     default=None)
    parser.add_argument("--line-label", default=None)
    parser.add_argument(
        "--to-state", default=None,
        choices=sorted(VALID_TARGETS),
        help="Target state to reset to",
    )
    parser.add_argument("--clear-job", action="store_true",
                        help="Empty the unit's job columns (failed or orphaned job)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be reset and cleared without changing anything")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    if args.unit_id is None and not (args.source and args.line_label):
        sys.exit("[ERROR] give --unit-id, or both --source and --line-label")
    if args.to_state is None and not args.clear_job:
        sys.exit("[ERROR] give --to-state and/or --clear-job")

    with scouse_connection(args.db_path) as conn:
        state_row = fetch_scouse_unit(conn, args.unit_id, args.source, args.line_label)
        if state_row is None:
            print("[ERROR] No such SCOUSE unit.")
            sys.exit(1)
        source, label = state_row["source_name"], state_row["line_label"]
        unit_id = state_row["scouse_unit_id"]
        job = (f"stage={state_row['job_stage']} status={state_row['job_status']} "
               f"id={state_row['job_id']} note={state_row['job_note']}")

        if args.to_state is None:
            if state_row["job_status"] is None:
                print(f"[reset-scouse] {source} / {label}: no job recorded, nothing to clear.")
                return
            print(f"[reset-scouse] {source} / {label}: job to clear: {job}")
            if args.dry_run:
                print("[DRY RUN] nothing changed.")
                return
            clear_scouse_job(conn, unit_id)
            print(f"[OK] {source} / {label}: job cleared (state stays '{state_row['current_state']}')")
            return

        if state_row["job_status"] in JOB_ACTIVE_STATUSES and not args.clear_job:
            print(f"[ERROR] {source} / {label} has a {state_row['job_status']} job ({job}). "
                  f"Stop that session first (canfar delete <id>), then re-run with --clear-job.")
            sys.exit(1)

        current = state_row["current_state"]
        target  = args.to_state
        current_idx = SCOUSE_FSM_ORDER.index(current) if current in SCOUSE_FSM_ORDER else 999
        if current_idx <= SCOUSE_FSM_ORDER.index(target):
            print(
                f"[ERROR] Current state '{current}' is not ahead of target '{target}'. "
                f"Nothing to reset."
            )
            sys.exit(1)

        params  = json.loads(state_row["parameters_json"] or "{}")
        n_rows  = db.fetch_one(
            conn, "SELECT COUNT(*) AS n FROM scouse_cluster WHERE scouse_unit_id=?", (unit_id,)
        )["n"]
        catalogue = (Path(db.to_local(state_row["output_dir"]))
                     / cube_stem(db.to_local(state_row["cube_fits_path"])) / "acorns_input.dat")
        drop_catalogue = (SCOUSE_FSM_ORDER.index(target) < SCOUSE_FSM_ORDER.index("stage4_complete")
                          and catalogue.exists())

        print(f"[reset-scouse] {source} / {label}: '{current}' → '{target}'")
        print(f"[reset-scouse] scouse_cluster rows to delete: {n_rows}")
        print(f"[reset-scouse] ACORNS payload to clear: n_fitted_components="
              f"{state_row['n_fitted_components']} n_acorns_clusters={state_row['n_acorns_clusters']} "
              f"_acorns_outputs={'set' if '_acorns_outputs' in params else 'absent'}")
        if drop_catalogue:
            print(f"[reset-scouse] stale stage-4 catalogue to delete: {catalogue}")
        if state_row["job_status"] is not None:
            print(f"[reset-scouse] job to clear: {job}")
        if args.dry_run:
            print("[DRY RUN] nothing changed.")
            return

        n_deleted = clear_acorns_results(conn, unit_id, params)
        clear_scouse_job(conn, unit_id)
        advance_scouse_state(
            conn, unit_id, source, label,
            from_state=current, to_state=target,
            note=f"manual reset; {n_deleted} scouse_cluster rows deleted",
        )

    if drop_catalogue:
        catalogue.unlink()

    print(f"[OK] {source} / {label}: '{current}' → '{target}' "
          f"({n_deleted} cluster rows deleted)")


if __name__ == "__main__":
    main()
