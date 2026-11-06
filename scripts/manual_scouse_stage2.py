"""
manual_scouse_stage2.py
------------------------
SCOUSE stage 2: spectral decomposition of the stage-1 SAA spectra.

Headless by default: scousepy fits every SAA with derivative spectroscopy at SNR 3,
alpha 5, i.e. exactly what the GUI's "apply to all" button does, and saves the
result. This is how batch_scouse.py runs it in a headless session.
--interactive opens the GUI instead (needs a live display: Desktop session on
CANFAR) for hand-fitting individual SAAs.

Launch it with scouse_run.sh from the project root (it sets LD_LIBRARY_PATH and
uses the structure_id_env venv python):

    alma-sails-codebase/scripts/scouse_run.sh --stage 2 \
        --source S68Nc --line-label "HC3N(10-9)"   # or --unit-id 2; [--interactive]

Prerequisites: unit must be in state 'stage1_complete'.
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

SNR, ALPHA = 3, 5   # scousepy's defaults, the GUI's starting slider values


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 2: spectral decomposition (headless unless --interactive)."
    )
    add_stage_args(parser)
    parser.add_argument("--interactive", action="store_true",
                        help="Open the fitting GUI instead of fitting every SAA automatically")
    parser.add_argument("--refit",      action="store_true",
                        help="Re-fit SAAs even if stage-2 output already exists")
    args = parser.parse_args()

    state_row = load_stage_unit(args, "2")
    with stage_job(args, state_row, "2"):
        run(args, state_row)


def run(args, state_row) -> None:
    source, label = state_row["source_name"], state_row["line_label"]
    output_dir    = Path(db.to_local(state_row["output_dir"]))
    stem          = cube_stem(db.to_local(state_row["cube_fits_path"]))
    config        = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        print("        Did stage 1 complete successfully?")
        sys.exit(1)

    # run_setup creates it, but a forced re-prep deletes it and scousepy writes into it
    # without creating it.
    (output_dir / stem / "stage_2").mkdir(exist_ok=True)

    scouse = import_scouse(headless=not args.interactive)

    mode = "interactive GUI" if args.interactive else f"headless, SNR={SNR} alpha={ALPHA}"
    print(f"[scouse] stage 2 — {source} / {label} ({mode})", flush=True)
    print(f"  config: {config}", flush=True)

    s = scouse.stage_2(config=config, refit=args.refit, interactive=args.interactive,
                       SNR=SNR, alpha=ALPHA)
    # stage_2 saves partial progress and returns normally when not every SAA has a fit.
    if s is None or "s2" not in s.completed_stages:
        print("[ERROR] stage 2 did not fit every SAA; state not advanced.")
        sys.exit(1)
    print("[scouse] stage 2 complete.", flush=True)

    with scouse_connection(args.db_path) as conn:
        advance_scouse_state(
            conn, state_row["scouse_unit_id"], source, label,
            from_state="stage1_complete", to_state="stage2_complete",
            note=None if args.interactive else f"headless dspec SNR={SNR} alpha={ALPHA}",
        )
    print(f"[OK] {source} / {label} → stage2_complete")


if __name__ == "__main__":
    main()
