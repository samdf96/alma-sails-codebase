"""
manual_scouse_stage4.py
------------------------
SCOUSE stage 4: fit checking.

Headless by default: scousepy builds the fit checker without showing it, which
computes and saves the diagnostic maps (stage_4/stage_4_{ncomps,redchisq,rms,...}.fits)
and writes s4.scousepy with every stage-3 fit accepted as is. This is how
batch_scouse.py runs it in a headless session.
--interactive opens the bitesize GUI instead (needs a live display: Desktop session
on CANFAR) to review and refit individual spectra.

Launch it with scouse_run.sh from the project root (it sets LD_LIBRARY_PATH and
uses the structure_id_env venv python):

    alma-sails-codebase/scripts/scouse_run.sh --stage 4 \
        --source S68Nc --line-label "HC3N(10-9)"   # or --unit-id 2; [--interactive]

Prerequisites: unit must be in state 'stage3_complete'.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

_casa_prefix = "/opt/casa"
sys.path = [p for p in sys.path if not p.startswith(_casa_prefix)]

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("TQDM_DISABLE", "1")
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

from alma_ops import db
from alma_ops.imaging.scouse_utils import (
    add_stage_args,
    advance_scouse_state,
    cube_stem,
    import_scouse,
    load_stage_unit,
    scouse_connection,
    scousepy_config_path,
    stage_job,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 4: fit checking (headless unless --interactive)."
    )
    add_stage_args(parser)
    parser.add_argument("--interactive", action="store_true",
                        help="Open the bitesize review GUI instead of accepting the stage-3 fits")
    args = parser.parse_args()

    state_row = load_stage_unit(args, "4")
    with stage_job(args, state_row, "4"):
        run(args, state_row)


def run(args, state_row) -> None:
    source, label = state_row["source_name"], state_row["line_label"]
    output_dir    = Path(db.to_local(state_row["output_dir"]))
    stem          = cube_stem(db.to_local(state_row["cube_fits_path"]))
    config        = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        sys.exit(1)
    # run_setup creates it, but scousepy only writes s4.scousepy at the very end of the
    # stage and fails then if the directory has gone.
    (output_dir / stem / "stage_4").mkdir(exist_ok=True)

    scouse = import_scouse(headless=not args.interactive)

    print(f"[scouse] stage 4 — {source} / {label} "
          f"({'interactive bitesize GUI' if args.interactive else 'headless, fits accepted as is'})",
          flush=True)
    if args.interactive:
        print("  Review each flagged pixel in the GUI. Close the window when done.", flush=True)
        s = scouse.stage_4(config=config, bitesize=True)
    else:
        s = scouse.stage_4(config=config, nocheck=True)
    if s is None or "s4" not in s.completed_stages:
        print("[ERROR] stage 4 returned without completing; state not advanced.")
        sys.exit(1)
    print("[scouse] stage 4 complete.", flush=True)

    with scouse_connection(args.db_path) as conn:
        advance_scouse_state(
            conn, state_row["scouse_unit_id"], source, label,
            from_state="stage3_complete", to_state="stage4_complete",
            note=None if args.interactive else "headless: stage-3 fits accepted",
        )
    print(f"[OK] {source} / {label} → stage4_complete")


if __name__ == "__main__":
    main()
