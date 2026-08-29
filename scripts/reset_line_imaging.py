"""
reset_line_imaging.py
------------------------
Recovery script for a failed or killed deep line-imaging run.

Same failure mode as reset_line_triage.py, not reset_admit.py: a killed CASA
process (most commonly OOM during tclean) leaves partial CASA-format
products on disk (.pb/.psf/.sumwt/.weight, no final .image) and the
line_imaging_unit stuck in 'running' with zero line_imaging_run_log entries,
since manual_line_imaging.py's own try/except only catches Python
exceptions, not the process being killed outright.

Deletes the partial CASA-format products (via the same build_img_base helper
run_line_imaging itself uses, so this doesn't depend on imaging_products --
which may not exist yet if the run died before ever completing), plus any
orphaned TempLattice* scratch files, then resets the unit back to 'ready'.

The unit must be in 'failed' or 'running' state.

Usage (VM shell or CASA container -- no CASA needed, this is file/DB cleanup
only):
    python alma-sails-codebase/scripts/reset_line_imaging.py --detection-id 2

Optional:
    --db-path /path/to/db
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DB_PATH, LINE_FINAL_DIR
from alma_ops.imaging.continuum_config import DEFAULT_ROBUST
from alma_ops.imaging.line_imaging_utils import build_img_base
from alma_ops.utils import to_dir_mous_id

_RECOVERABLE_STATES = {"failed", "running"}

_CASA_SUFFIXES = [
    ".image", ".image.pbcor", ".image.fits", ".image.pbcor.fits", ".mask", ".model",
    ".pb", ".psf", ".residual", ".sumwt", ".weight",
]


def main(detection_id: int, db_path: Path, robust: float = DEFAULT_ROBUST) -> None:
    conn = db.get_connection(db_path)

    detection = db.get_line_admit_detection(conn, detection_id)
    if detection is None:
        print(f"[ERROR] No line_admit_detection_id={detection_id} found")
        sys.exit(1)

    unit_row = db.get_line_imaging_unit_for_detection(conn, detection_id)
    if unit_row is None:
        print(f"[ERROR] No line_imaging_unit for detection_id={detection_id}")
        sys.exit(1)

    current_state = unit_row["current_state"]
    db.require_state(current_state, _RECOVERABLE_STATES)

    mous_id = unit_row["mous_id"]
    target_name = unit_row["target_name"]
    mous_dir_id = to_dir_mous_id(mous_id)

    # manual_line_imaging.py writes straight into <target>/ with no
    # B<band>_<config> level (unlike the triage stage) -- must match it exactly
    # or nothing gets cleaned up and the reset silently does nothing.
    imaging_dir = LINE_FINAL_DIR / target_name
    img_base = Path(build_img_base(
        imaging_dir, target_name, mous_dir_id, unit_row["spw_idx"],
        unit_row["species_name"], unit_row["frequency_ghz"], detection_id, robust,
    ))

    print(f"[reset-line-imaging] detection_id : {detection_id}")
    print(f"[reset-line-imaging] mous_id      : {mous_id}")
    print(f"[reset-line-imaging] target       : {target_name}")
    print(f"[reset-line-imaging] state        : {current_state}")
    print(f"[reset-line-imaging] robust       : {robust}")
    print(f"[reset-line-imaging] imaging_dir  : {imaging_dir}")

    removed = []
    for suffix in _CASA_SUFFIXES:
        p = Path(str(img_base) + suffix)
        if not p.exists():
            continue
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink()
        removed.append(p.name)

    if imaging_dir.exists():
        for p in imaging_dir.glob("TempLattice*"):
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink()
            removed.append(p.name)

    if removed:
        print(f"[reset-line-imaging] removed {len(removed)} item(s): {', '.join(removed)}")
    else:
        print(f"[reset-line-imaging] no partial products found on disk")

    db.record_line_imaging_transition(
        conn, unit_row["line_imaging_unit_id"], from_state=current_state, to_state="ready",
        note="reset after failed/killed line-imaging run — partial products removed",
        step="reset_line_imaging",
    )
    conn.close()

    print(f"[OK]   detection {detection_id} → ready (ready to retry)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset a failed/killed deep line-imaging run and restore 'ready' state."
    )
    parser.add_argument("--detection-id", type=int, required=True, help="line_admit_detection_id")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    parser.add_argument("--robust", type=float, default=DEFAULT_ROBUST,
                        help="Briggs robust of the run being reset. Products are named "
                             "per-robust, so this must match the failed run or nothing "
                             f"will be found (default: {DEFAULT_ROBUST})")
    args = parser.parse_args()

    main(detection_id=args.detection_id, db_path=Path(args.db_path), robust=args.robust)
