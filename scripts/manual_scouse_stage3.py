"""
manual_scouse_stage3.py
------------------------
SCOUSE stage 3: automated quality control.  Headless — no display required.
Run from the VM shell or any terminal inside a CASA container.

    /arc/projects/ALMA-SAILS/structure_id_env/.venv/bin/python3 \\
        alma-sails-codebase/scripts/manual_scouse_stage3.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)"

Prerequisites: unit must be in state 'stage2_complete'.

Stage 3 applies automated quality-control checks to the stage-2 Gaussian fits
and flags low-quality solutions.  It runs entirely headlessly and is the only
stage that is safe to run in a non-Desktop (headless) CANFAR session.
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
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    advance_scouse_state,
    cube_stem,
    scousepy_config_path,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SCOUSE stage 3: headless automated quality control."
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
    if state_row["current_state"] != "stage2_complete":
        print(f"[ERROR] Expected state 'stage2_complete', got '{state_row['current_state']}'.")
        sys.exit(1)

    cube_fits_local = Path(db.to_local(state_row["cube_fits_path"]))
    output_dir      = Path(db.to_local(state_row["output_dir"]))
    stem            = cube_stem(str(cube_fits_local))
    unit_id         = state_row["scouse_unit_id"]
    config          = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        sys.exit(1)

    from scousepy import scouse

    print(f"[scouse] stage 3 — {args.source} / {args.line_label}", flush=True)
    scouse.stage_3(config=config)
    print("[scouse] stage 3 complete.", flush=True)

    advance_scouse_state(
        conn, unit_id, args.source, args.line_label,
        from_state="stage2_complete", to_state="stage3_complete",
    )
    print(f"[OK] {args.source} / {args.line_label} → stage3_complete")
    conn.close()


if __name__ == "__main__":
    main()
