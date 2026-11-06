"""
manual_scouse_stage3.py
------------------------
SCOUSE stage 3: automated fitting of every spectrum, starting from the stage-2 SAA
fits. Headless — no display required. Run from a CASA container (the venv does not
work outside one), or as a headless session via batch_scouse.py.

Launch it with scouse_run.sh from the project root (it sets LD_LIBRARY_PATH and
uses the structure_id_env venv python):

    alma-sails-codebase/scripts/scouse_run.sh --stage 3 \
        --source S68Nc --line-label "HC3N(10-9)"   # or --unit-id 2; [--njobs 4]

Prerequisites: unit must be in state 'stage2_complete'.

Stage 3 fits in parallel with scousepy.config's njobs workers (run_setup writes 3).
--njobs N rewrites that value first; batch_scouse.py passes the session's pinned
core count. Run time scales with the number of channels in vel_range.
"""

from __future__ import annotations

import argparse
import os
import sys

_casa_prefix = "/opt/casa"
sys.path = [p for p in sys.path if not p.startswith(_casa_prefix)]

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("TQDM_DISABLE", "1")

from alma_ops import db
from alma_ops.imaging.scouse_utils import (
    add_stage_args,
    advance_scouse_state,
    cube_stem,
    import_scouse,
    load_stage_unit,
    scouse_connection,
    scousepy_config_path,
    set_scousepy_njobs,
    stage_job,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 3: headless automated fitting."
    )
    add_stage_args(parser)
    parser.add_argument("--njobs", type=int, default=None,
                        help="Parallel workers; written to scousepy.config "
                             "(default: keep the config's value)")
    args = parser.parse_args()

    state_row = load_stage_unit(args, "3")
    with stage_job(args, state_row, "3"):
        run(args, state_row)


def run(args, state_row) -> None:
    source, label = state_row["source_name"], state_row["line_label"]
    output_dir    = Path(db.to_local(state_row["output_dir"]))
    stem          = cube_stem(db.to_local(state_row["cube_fits_path"]))
    config        = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        sys.exit(1)
    # run_setup creates it, but scousepy only writes s3.scousepy at the very end of the
    # stage and fails then if the directory has gone.
    (output_dir / stem / "stage_3").mkdir(exist_ok=True)
    if args.njobs is not None:
        set_scousepy_njobs(output_dir, stem, args.njobs)

    scouse = import_scouse(headless=True)

    njobs_note = f"njobs={args.njobs}" if args.njobs is not None else "njobs from config"
    print(f"[scouse] stage 3 — {source} / {label} ({njobs_note}, {os.cpu_count()} CPUs visible)",
          flush=True)
    s = scouse.stage_3(config=config)
    if s is None or "s3" not in s.completed_stages:
        print("[ERROR] stage 3 returned without completing; state not advanced.")
        sys.exit(1)
    print("[scouse] stage 3 complete.", flush=True)

    with scouse_connection(args.db_path) as conn:
        advance_scouse_state(
            conn, state_row["scouse_unit_id"], source, label,
            from_state="stage2_complete", to_state="stage3_complete",
            note=njobs_note,
        )
    print(f"[OK] {source} / {label} → stage3_complete")


if __name__ == "__main__":
    main()
