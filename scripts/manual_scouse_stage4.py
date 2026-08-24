"""
manual_scouse_stage4.py
------------------------
SCOUSE stage 4: bitesize interactive quality review.  INTERACTIVE — requires
a live X11 or VNC display (Desktop session on CANFAR).

    export LD_LIBRARY_PATH=/arc/projects/ALMA-SAILS/tools/xcb-libs/usr/lib/x86_64-linux-gnu
    /arc/projects/ALMA-SAILS/structure_id_env/.venv/bin/python3 \\
        alma-sails-codebase/scripts/manual_scouse_stage4.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)"

Prerequisites: unit must be in state 'stage3_complete'.

Stage 4 shows each remaining poorly-fitted pixel one at a time ("bitesize")
so you can accept, reject, or manually refit.  Exit the GUI when done.
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
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    advance_scouse_state,
    cube_stem,
    scousepy_config_path,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 4: interactive bitesize quality review."
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
        print(f"[ERROR] No SCOUSE unit for source='{args.source}' line='{args.line_label}'.")
        sys.exit(1)
    if state_row["current_state"] != "stage3_complete":
        print(f"[ERROR] Expected state 'stage3_complete', got '{state_row['current_state']}'.")
        sys.exit(1)

    cube_fits_local = Path(db.to_local(state_row["cube_fits_path"]))
    output_dir      = Path(db.to_local(state_row["output_dir"]))
    stem            = cube_stem(str(cube_fits_local))
    unit_id         = state_row["scouse_unit_id"]
    config          = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        sys.exit(1)

    import matplotlib
    matplotlib.rcParams["font.family"] = "DejaVu Sans"
    from scousepy import scouse

    print(f"[scouse] stage 4 — {args.source} / {args.line_label}", flush=True)
    print("  Review each flagged pixel in the GUI. Close the window when done.", flush=True)

    scouse.stage_4(config=config, bitesize=True)
    print("[scouse] stage 4 complete.", flush=True)

    advance_scouse_state(
        conn, unit_id, args.source, args.line_label,
        from_state="stage3_complete", to_state="stage4_complete",
    )
    print(f"[OK] {args.source} / {args.line_label} → stage4_complete")
    conn.close()


if __name__ == "__main__":
    main()
