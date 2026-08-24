"""
manual_scouse_stage1.py
------------------------
SCOUSE stage 1: coverage definition + SAA fitting.  INTERACTIVE — requires a
live X11 or VNC display.  Must be run inside a CASA container launched from
the CANFAR Science Portal desktop session.

Invoke with the structure_id_env venv python and LD_LIBRARY_PATH set:

    export LD_LIBRARY_PATH=/arc/projects/ALMA-SAILS/tools/xcb-libs/usr/lib/x86_64-linux-gnu
    /arc/projects/ALMA-SAILS/structure_id_env/.venv/bin/python3 \\
        alma-sails-codebase/scripts/manual_scouse_stage1.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)"

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

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    advance_scouse_state,
    cube_stem,
    scouse_output_dir_path,
    scousepy_config_path,
    write_coverage_config,
)

import json


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 1: interactive coverage/SAA fitting."
    )
    parser.add_argument("--source",     required=True)
    parser.add_argument("--line-label", required=True)
    parser.add_argument("--db-path",    default=str(DB_PATH))
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)
    state_row = db.fetch_one(
        conn,
        "SELECT sps.*, su.cube_fits_path, su.output_dir FROM scouse_pipeline_state sps "
        "JOIN scouse_unit su USING (scouse_unit_id) "
        "WHERE sps.source_name=? AND sps.line_label=?",
        (args.source, args.line_label),
    )
    if state_row is None:
        print(f"[ERROR] No SCOUSE unit for source='{args.source}' line='{args.line_label}'. "
              f"Run manual_scouse_prep.py first.")
        sys.exit(1)
    if state_row["current_state"] != "prep_complete":
        print(f"[ERROR] Expected state 'prep_complete', got '{state_row['current_state']}'.")
        sys.exit(1)

    params = json.loads(state_row["parameters_json"])
    cube_fits_local = Path(db.to_local(state_row["cube_fits_path"]))
    output_dir      = Path(db.to_local(state_row["output_dir"]))
    stem            = cube_stem(str(cube_fits_local))
    unit_id         = state_row["scouse_unit_id"]

    output_dir.mkdir(parents=True, exist_ok=True)
    write_coverage_config(output_dir, stem, params)

    import matplotlib
    matplotlib.rcParams["font.family"] = "DejaVu Sans"
    from scousepy import scouse

    print(f"[scouse] stage 1 — {args.source} / {args.line_label}", flush=True)
    print(f"  cube:       {cube_fits_local.name}", flush=True)
    print(f"  output_dir: {output_dir}", flush=True)
    print(f"  vel_range:  {params['vel_range']}  wsaa: {params['wsaa']}  "
          f"mask_below: {params['mask_below']}", flush=True)
    print("[scouse] GUI opening — run moments → run coverage → continue", flush=True)

    config = scouse.run_setup(stem, str(cube_fits_local.parent), outputdir=str(output_dir) + "/")
    if config is None:
        config = str(scousepy_config_path(output_dir, stem))
    write_coverage_config(output_dir, stem, params)  # patch after run_setup creates the file

    scouse.stage_1(config=config, interactive=True)
    print("[scouse] stage 1 complete.", flush=True)

    advance_scouse_state(
        conn, unit_id, args.source, args.line_label,
        from_state="prep_complete", to_state="stage1_complete",
    )
    print(f"[OK] {args.source} / {args.line_label} → stage1_complete")
    conn.close()


if __name__ == "__main__":
    main()
