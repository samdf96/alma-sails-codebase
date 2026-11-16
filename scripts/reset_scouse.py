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

No SAAs:
    --mark-no-saas   move a stage1_complete unit whose stage 1 saved 0 SAAs to the final
                     state 'no_saas' (and clear its failed stage-2 job), so batch_scouse.py
                     stops launching it. Stage 1 does this itself since 2026-11-12; this is
                     for units that went through stage 1 before then.
A 'no_saas' unit is not reset with --to-state: re-open it with manual_scouse_prep.py
--force (it deletes the old stage_1 output, which scousepy would otherwise reload).

Compact lines:
    --mark-compact   move a unit whose detection is tagged compact
                     (review_line_detections.py --morph compact) to the final state
                     'compact_skip', from any state, and delete its SCOUSE + ACORNS
                     products so no compact line has results anywhere:
                       - scouse_cluster rows and the ACORNS payload (as --to-state does);
                       - the <output_dir>/<cube stem>/ tree (stage_1-4, acorns_output,
                         acorns_input.dat, config_files);
                       - the summary PDF, its _png/ folder and Jupyter checkpoint copies in
                         <output_dir>, and its copy in imaging/lines/scouse/summary_pdfs/.
                     Kept: the scouse_unit row, its transition log, prep_velocity_window.png
                     and anything else in <output_dir> (listed). A failed job is cleared; a
                     submitted/running one refuses unless --clear-job.
    --all-compact    with --mark-compact: every compact-tagged unit not yet compact_skip,
                     instead of --unit-id. Every unit is checked before anything changes.
A 'compact_skip' unit is re-opened by changing its tag, then manual_scouse_prep.py --force.

    python alma-sails-codebase/scripts/reset_scouse.py --mark-compact --all-compact --dry-run
    python alma-sails-codebase/scripts/reset_scouse.py --mark-compact --unit-id 46
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH, SCOUSE_SUMMARY_DIR
from alma_ops.imaging.scouse_utils import (
    JOB_ACTIVE_STATUSES,
    SCOUSE_COMPACT_SKIP,
    SCOUSE_FSM_ORDER,
    SCOUSE_NO_SAAS,
    advance_scouse_state,
    clear_acorns_results,
    clear_scouse_job,
    cube_stem,
    fetch_scouse_unit,
    n_saas,
    no_saas_note,
    scouse_connection,
    unit_morphology,
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
    parser.add_argument("--mark-no-saas", action="store_true",
                        help="Move a stage1_complete unit with 0 SAAs to the final state 'no_saas'")
    parser.add_argument("--mark-compact", action="store_true",
                        help="Move a compact-tagged unit to 'compact_skip' and delete its "
                             "SCOUSE/ACORNS products")
    parser.add_argument("--all-compact", action="store_true",
                        help="With --mark-compact: every compact-tagged unit not yet compact_skip")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be reset and cleared without changing anything")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    if args.all_compact and not args.mark_compact:
        sys.exit("[ERROR] --all-compact goes with --mark-compact")
    if sum([args.mark_no_saas, args.mark_compact, args.to_state is not None]) > 1:
        sys.exit("[ERROR] --to-state, --mark-no-saas and --mark-compact are separate actions; give one")
    if args.mark_compact:
        if args.all_compact == (args.unit_id is not None or bool(args.source)):
            sys.exit("[ERROR] --mark-compact takes --unit-id (or --source/--line-label), "
                     "or --all-compact")
        mark_compact(args)
        return
    if args.unit_id is None and not (args.source and args.line_label):
        sys.exit("[ERROR] give --unit-id, or both --source and --line-label")
    if args.to_state is None and not (args.clear_job or args.mark_no_saas):
        sys.exit("[ERROR] give --to-state, --clear-job, --mark-no-saas or --mark-compact")

    with scouse_connection(args.db_path) as conn:
        state_row = fetch_scouse_unit(conn, args.unit_id, args.source, args.line_label)
        if state_row is None:
            print("[ERROR] No such SCOUSE unit.")
            sys.exit(1)
        source, label = state_row["source_name"], state_row["line_label"]
        unit_id = state_row["scouse_unit_id"]
        job = (f"stage={state_row['job_stage']} status={state_row['job_status']} "
               f"id={state_row['job_id']} note={state_row['job_note']}")

        if args.mark_no_saas:
            mark_no_saas(conn, state_row, job, args.dry_run)
            return

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
        if current == SCOUSE_NO_SAAS:
            print(f"[ERROR] {source} / {label} is '{SCOUSE_NO_SAAS}'. Re-open it with "
                  f"manual_scouse_prep.py --detection-id N --force (new mask/vel_range), "
                  f"then stage 1 again; a --to-state reset would let scousepy reload the old "
                  f"empty stage-1 output.")
            sys.exit(1)
        if current == SCOUSE_COMPACT_SKIP:
            print(f"[ERROR] {source} / {label} is '{SCOUSE_COMPACT_SKIP}'. To fit it after all, "
                  f"change its detection's tag away from compact, then "
                  f"manual_scouse_prep.py --detection-id N --force.")
            sys.exit(1)
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


def mark_no_saas(conn, state_row, job: str, dry_run: bool) -> None:
    """Move a stage1_complete unit whose stage 1 saved no SAAs to the final 'no_saas' state."""
    source, label = state_row["source_name"], state_row["line_label"]
    current = state_row["current_state"]
    params = json.loads(state_row["parameters_json"] or "{}")
    if current != "stage1_complete":
        print(f"[ERROR] {source} / {label} is '{current}'; only a stage1_complete unit can be "
              f"marked {SCOUSE_NO_SAAS}.")
        sys.exit(1)
    if state_row["job_status"] in JOB_ACTIVE_STATUSES:
        print(f"[ERROR] {source} / {label} has a {state_row['job_status']} job ({job}). "
              f"Stop that session first (canfar delete <id>).")
        sys.exit(1)
    if n_saas(params) != 0:
        print(f"[ERROR] {source} / {label}: stage 1 recorded totalsaas={params.get('totalsaas')}, "
              f"not 0. Nothing to mark.")
        sys.exit(1)

    note = no_saas_note(params)
    print(f"[reset-scouse] {source} / {label}: '{current}' → '{SCOUSE_NO_SAAS}'")
    print(f"[reset-scouse] note: {note}")
    if state_row["job_status"] is not None:
        print(f"[reset-scouse] job to clear: {job}")
    if dry_run:
        print("[DRY RUN] nothing changed.")
        return
    clear_scouse_job(conn, state_row["scouse_unit_id"])
    advance_scouse_state(conn, state_row["scouse_unit_id"], source, label,
                         from_state=current, to_state=SCOUSE_NO_SAAS,
                         note=f"manual --mark-no-saas; {note}")
    print(f"[OK] {source} / {label}: '{current}' → '{SCOUSE_NO_SAAS}'")


def _tree_size(path: Path) -> tuple[int, int]:
    """(number of files, total bytes) under path, or of the single file."""
    if path.is_file():
        return 1, path.stat().st_size
    n = size = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            n += 1
            try:
                size += (Path(root) / f).stat().st_size
            except OSError:
                pass
    return n, size


def compact_products(state_row) -> tuple[list[Path], list[Path]]:
    """(paths --mark-compact deletes, other entries left in output_dir) for one unit."""
    out = Path(db.to_local(state_row["output_dir"]))
    tree = out / cube_stem(db.to_local(state_row["cube_fits_path"]))
    delete = [tree] if tree.exists() else []
    if out.is_dir():
        delete += sorted(out.glob("*_scouse_summary.pdf"))
        delete += sorted(p for p in out.glob("*_scouse_summary_png") if p.is_dir())
        delete += sorted((out / ".ipynb_checkpoints").glob("*_scouse_summary*"))
    names = {p.name for p in delete if p.suffix == ".pdf"}
    # scouse_summary.summary_pdf_path's name (not imported: that module needs numpy)
    safe = re.sub(r"[^A-Za-z0-9+.\-]+", "_",
                  f"{state_row['source_name']}_{state_row['line_label']}").strip("_")
    names.add(f"{safe}_scouse_summary.pdf")
    for name in sorted(names):
        copy = Path(db.to_local(str(SCOUSE_SUMMARY_DIR))) / name
        if copy.exists():
            delete.append(copy)
        png = copy.with_name(copy.stem + "_png")
        if png.is_dir():
            delete.append(png)
    keep = []
    if out.is_dir():
        gone = set(delete)
        for p in sorted(out.iterdir()):
            if p in gone:
                continue
            if p.name == ".ipynb_checkpoints" and all(c in gone for c in p.iterdir()):
                continue
            keep.append(p)
    return delete, keep


def mark_compact(args) -> None:
    """Move compact-tagged units to 'compact_skip', deleting their SCOUSE/ACORNS products."""
    with scouse_connection(args.db_path) as conn:
        if args.all_compact:
            rows = db.fetch_all(
                conn,
                "SELECT su.scouse_unit_id FROM scouse_unit su JOIN line_admit_detection d "
                "ON d.line_admit_detection_id = su.line_admit_detection_id "
                "JOIN scouse_pipeline_state sps USING (scouse_unit_id) "
                "WHERE d.morphology='compact' AND sps.current_state<>? "
                "ORDER BY su.scouse_unit_id", (SCOUSE_COMPACT_SKIP,))
            ids = [r["scouse_unit_id"] for r in rows]
        else:
            row = fetch_scouse_unit(conn, args.unit_id, args.source, args.line_label)
            if row is None:
                sys.exit("[ERROR] No such SCOUSE unit.")
            ids = [row["scouse_unit_id"]]

        plan, errors = [], []
        for uid in ids:
            r = fetch_scouse_unit(conn, uid)
            who = f"unit {uid} {r['source_name']} / {r['line_label']}"
            det_id, morph = unit_morphology(conn, uid)
            if r["current_state"] == SCOUSE_COMPACT_SKIP:
                print(f"[SKIP] {who} is already {SCOUSE_COMPACT_SKIP}")
                continue
            if morph != "compact":
                errors.append(f"{who}: detection {det_id} is tagged {morph or 'nothing'}, not compact")
                continue
            if r["job_status"] in JOB_ACTIVE_STATUSES and not args.clear_job:
                errors.append(f"{who}: stage {r['job_stage']} job is {r['job_status']} "
                              f"({r['job_id']}); stop it first, then add --clear-job")
                continue
            note = db.fetch_one(conn, "SELECT reviewed_note FROM line_admit_detection "
                                "WHERE line_admit_detection_id=?", (det_id,))["reviewed_note"]
            n_rows = db.fetch_one(conn, "SELECT COUNT(*) AS n FROM scouse_cluster "
                                  "WHERE scouse_unit_id=?", (uid,))["n"]
            plan.append((r, det_id, note, n_rows, *compact_products(r)))
        if errors and (not args.dry_run or not plan):
            for e in errors:
                print(f"[ERROR] {e}")
            sys.exit("[ERROR] nothing changed")
        if not plan:
            print("[reset-scouse] no unit to mark compact")
            return

        tot_files = tot_bytes = 0
        for r, det_id, note, n_rows, delete, keep in plan:
            print(f"[reset-scouse] unit {r['scouse_unit_id']} {r['source_name']} / {r['line_label']} "
                  f"(det {det_id}): '{r['current_state']}' → '{SCOUSE_COMPACT_SKIP}'")
            print(f"    note: {note or '(none)'}")
            print(f"    scouse_cluster rows: {n_rows}   n_fitted_components="
                  f"{r['n_fitted_components']} n_acorns_clusters={r['n_acorns_clusters']}")
            if r["job_status"] is not None:
                print(f"    job to clear: stage={r['job_stage']} status={r['job_status']} "
                      f"note={(r['job_note'] or '')[:100]}")
            for p in delete:
                n, size = _tree_size(p)
                tot_files += n
                tot_bytes += size
                kind = f"dir, {n} files" if p.is_dir() else "file"
                print(f"    delete {p}  ({kind}, {size / 1e6:.1f} MB)")
            for p in keep:
                print(f"    keep   {p}")
        print(f"[reset-scouse] {len(plan)} unit(s); {tot_files} files, {tot_bytes / 1e9:.2f} GB to delete")
        for e in errors:
            print(f"[ERROR] {e}")
        if args.dry_run:
            print("[DRY RUN] nothing changed.")
            return

        for r, det_id, note, n_rows, delete, keep in plan:
            uid = r["scouse_unit_id"]
            params = json.loads(r["parameters_json"] or "{}")
            n_deleted = clear_acorns_results(conn, uid, params)
            clear_scouse_job(conn, uid)
            advance_scouse_state(
                conn, uid, r["source_name"], r["line_label"],
                from_state=r["current_state"], to_state=SCOUSE_COMPACT_SKIP,
                note=(f"manual --mark-compact; detection {det_id} compact: {note or ''}; "
                      f"{n_deleted} scouse_cluster rows and {len(delete)} product path(s) deleted")[:1000],
            )
            for p in delete:
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()
            ckpt = Path(db.to_local(r["output_dir"])) / ".ipynb_checkpoints"
            if ckpt.is_dir() and not any(ckpt.iterdir()):
                ckpt.rmdir()
            print(f"[OK] unit {uid} {r['source_name']} / {r['line_label']}: "
                  f"'{r['current_state']}' → '{SCOUSE_COMPACT_SKIP}' "
                  f"({n_deleted} cluster rows, {len(delete)} path(s) deleted)")


if __name__ == "__main__":
    main()
