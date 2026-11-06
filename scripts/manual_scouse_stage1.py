"""
manual_scouse_stage1.py
------------------------
SCOUSE stage 1: coverage definition + SAA fitting.  INTERACTIVE — requires a
live X11 or VNC display.  Must be run inside a CASA container launched from
the CANFAR Science Portal desktop session.

Launch it with scouse_run.sh from the project root (it sets LD_LIBRARY_PATH and
uses the structure_id_env venv python):

    alma-sails-codebase/scripts/scouse_run.sh --stage 1 \
        --source S68Nc --line-label "HC3N(10-9)"      # or --unit-id 2

Stage 1 is the one stage that stays interactive: batch_scouse.py picks the unit up
from stage1_complete and runs stages 2-4 and ACORNS as headless sessions.

Prerequisites: the unit must already be in state 'prep_complete' (run
manual_scouse_prep.py first).

GUI workflow:
  1. Integrated intensity map appears.
  2. Click "run moments" (green, left panel).
  3. Click "run coverage" (green, right panel) — SAA grid overlaid on map.
  4. Optionally click "mask" to review pixel masking.
  5. Click "continue" (blue) — SAA fitting runs (may take minutes).
  6. GUI closes; output saved to <output_dir>/<cube_stem>/stage_1/.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

# Strip CASA Python paths that shadow the venv's packages (astropy, matplotlib …)
_casa_prefix = "/opt/casa"
sys.path = [p for p in sys.path if not p.startswith(_casa_prefix)]

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("TQDM_DISABLE", "1")
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

import json
import math
import shutil

from alma_ops import db
from alma_ops.imaging.scouse_utils import (
    add_stage_args,
    advance_scouse_state,
    cube_stem,
    load_stage_unit,
    read_coverage_config,
    scouse_connection,
    scousepy_config_path,
    stage_job,
    update_scouse_parameters,
    write_coverage_config,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 1: interactive coverage/SAA fitting."
    )
    add_stage_args(parser)
    args = parser.parse_args()

    state_row = load_stage_unit(args, "1")
    with stage_job(args, state_row, "1"):
        run(args, state_row)


def run(args, state_row) -> None:
    source, label   = state_row["source_name"], state_row["line_label"]
    params          = json.loads(state_row["parameters_json"])
    cube_fits_local = Path(db.to_local(state_row["cube_fits_path"]))
    output_dir      = Path(db.to_local(state_row["output_dir"]))
    stem            = cube_stem(str(cube_fits_local))
    unit_id         = state_row["scouse_unit_id"]

    output_dir.mkdir(parents=True, exist_ok=True)
    stem_dir    = output_dir / stem
    config_file = scousepy_config_path(output_dir, stem)

    import matplotlib
    matplotlib.rcParams["font.family"] = "DejaVu Sans"
    from scousepy import scouse

    print(f"[scouse] stage 1 — {source} / {label}", flush=True)
    print(f"  cube:       {cube_fits_local.name}", flush=True)
    print(f"  output_dir: {output_dir}", flush=True)
    print(f"  vel_range:  {params['vel_range']}  wsaa: {params['wsaa']}  "
          f"mask_below: {params['mask_below']}", flush=True)
    if params.get("_rms_jybeam"):
        rms = params["_rms_jybeam"]
        beam_px = math.sqrt(params["_bmaj_arcsec"] * params["_bmin_arcsec"]) / params["pixel_size_arcsec"]
        print(f"  RMS:        {rms:.6f} Jy/beam per channel  → mask_below at "
              + "  ".join(f"{n}σ={n * rms:.6f}" for n in (5, 8, 10, 15)), flush=True)
        print(f"  beam:       {beam_px:.1f} px across  → wsaa at 2/3 beams = "
              f"{round(2 * beam_px)}/{round(3 * beam_px)} px", flush=True)

    if config_file.exists():
        # Resume from a previous partial run that already created the config
        config = str(config_file)
        write_coverage_config(output_dir, stem, params)
        print(f"[scouse] existing config found — patching params and resuming stage 1", flush=True)
    else:
        # Fresh setup.  run_setup creates the stem directory and scousepy.config.
        # Do NOT pre-create the stem directory — scousepy detects it as a conflict.
        if stem_dir.exists():
            shutil.rmtree(stem_dir)
        config = scouse.run_setup(stem, str(cube_fits_local.parent), outputdir=str(output_dir) + "/")
        if config is None:
            config = str(config_file)
        # Inject stored parameters into the coverage config that run_setup created
        write_coverage_config(output_dir, stem, params)
        if not config_file.exists():
            print(f"[ERROR] run_setup failed: no config file at {config_file}")
            sys.exit(1)

    # run_setup creates the stage directories, but a resumed unit skips it, and a
    # forced re-prep deletes them; scousepy writes coverage.pdf there without creating it.
    (stem_dir / "stage_1").mkdir(exist_ok=True)

    print("[scouse] GUI opening — run moments → run coverage → continue", flush=True)
    scouse.stage_1(config=config, interactive=True)

    # Guard: stage_1 prints an error and returns (no exception) on failure.
    # Verify it actually produced output before advancing state.
    stage1_dir = stem_dir / "stage_1"
    if not stage1_dir.exists() or not any(stage1_dir.iterdir()):
        print("[ERROR] stage 1 produced no output — GUI may have been closed before completion.")
        print("        Re-run this script to try again (state not advanced).")
        sys.exit(1)

    print("[scouse] stage 1 complete.", flush=True)

    gui = read_coverage_config(output_dir, stem)
    changed = {k: v for k, v in gui.items() if params.get(k) != v}
    params.update(gui)
    if params.get("_rms_jybeam"):
        params["_mask_sigma"] = round(params["mask_below"] / params["_rms_jybeam"], 2)
    print("  settings used: " + "  ".join(f"{k}={v}" for k, v in gui.items()), flush=True)

    with scouse_connection(args.db_path) as conn:
        update_scouse_parameters(conn, unit_id, params)
        advance_scouse_state(
            conn, unit_id, source, label,
            from_state="prep_complete", to_state="stage1_complete",
            note=("GUI changed " + ", ".join(f"{k}={v}" for k, v in changed.items())) if changed else None,
        )
    print(f"[OK] {source} / {label} → stage1_complete")


if __name__ == "__main__":
    main()
