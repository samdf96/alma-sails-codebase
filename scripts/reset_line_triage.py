"""
reset_line_triage.py
-----------------------
Recovery script for a failed or killed line-triage imaging run.

A killed CASA process (most commonly OOM during tclean) leaves partial
CASA-format imaging products on disk -- .pb/.psf/.sumwt/.weight from the
gridding stage, but no final .image, since the process died before tclean
finished writing it. It also leaves the line_triage_unit stuck in 'running'
with zero line_triage_run_log entries, since manual_line_triage_imaging.py's
own try/except only catches Python exceptions raised inside the script, not
the process being killed outright.

Deletes the partial CASA-format products (and any orphaned TempLattice*
scratch files a hard-killed tclean leaves behind) for the given spw, then
resets the line_triage_unit back to 'ready' so it can be retried cleanly.
Does not touch other spws' products in the same imaging_dir. The TempLattice*
sweep is not spw-scoped (CASA doesn't namespace these by spw) -- it assumes
no other tclean job is concurrently running against the same imaging_dir,
matching the "don't submit concurrent jobs sharing a MOUS" constraint
already documented in DEVLOG.md for continuum imaging.

The unit must be in 'failed' or 'running' state.

Usage (VM shell or CASA container -- no CASA needed, this is file/DB cleanup
only):
    python alma-sails-codebase/scripts/reset_line_triage.py \\
        --mous-id uid://A001/X.../X... --source EC_53 --spw 3

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
from alma_ops.config import DB_PATH, LINE_TRIAGE_DIR
from alma_ops.utils import to_dir_mous_id

_RECOVERABLE_STATES = {"failed", "running"}

_CASA_SUFFIXES = [
    ".image", ".image.fits", ".mask", ".model",
    ".pb", ".psf", ".residual", ".sumwt", ".weight",
]


def main(mous_id: str, source: str, spw: int, db_path: Path) -> None:
    conn = db.get_connection(db_path)

    triage_row = db.fetch_one(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? AND spw_idx=?",
        (mous_id, source, spw),
    )
    if triage_row is None:
        print(f"[ERROR] No line_triage_unit for mous_id={mous_id} source={source} spw={spw}")
        sys.exit(1)

    current_state = triage_row["current_state"]
    db.require_state(current_state, _RECOVERABLE_STATES)

    mous_row = db.get_mous(conn, mous_id)
    band, config_type = mous_row["band"], mous_row["array_config"]
    mous_dir_id = to_dir_mous_id(mous_id)

    imaging_dir = LINE_TRIAGE_DIR / source / f"B{band}_{config_type}"
    img_base = imaging_dir / f"{source}_{mous_dir_id}_spw{spw}_triage"

    print(f"[reset-line-triage] mous_id     : {mous_id}")
    print(f"[reset-line-triage] source      : {source}")
    print(f"[reset-line-triage] spw         : {spw}")
    print(f"[reset-line-triage] state       : {current_state}")
    print(f"[reset-line-triage] imaging_dir : {imaging_dir}")

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

    for p in imaging_dir.glob("TempLattice*"):
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink()
        removed.append(p.name)

    if removed:
        print(f"[reset-line-triage] removed {len(removed)} item(s): {', '.join(removed)}")
    else:
        print(f"[reset-line-triage] no partial products found on disk")

    db.record_line_triage_transition(
        conn, triage_row["line_triage_unit_id"], from_state=current_state, to_state="ready",
        note="reset after failed/killed line-triage imaging run — partial products removed",
        step="reset_line_triage",
    )
    conn.close()

    print(f"[OK]   {mous_id} {source} spw={spw} → ready (ready to retry)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset a failed/killed line-triage imaging run and restore 'ready' state."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--source", required=True, help="Target/field name within this MOUS")
    parser.add_argument("--spw", type=int, required=True, help="spw index of the line_triage_unit")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()

    main(mous_id=args.mous_id, source=args.source, spw=args.spw, db_path=Path(args.db_path))
