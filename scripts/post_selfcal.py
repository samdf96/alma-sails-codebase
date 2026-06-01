"""
post_selfcal.py
---------------
Post-autoselfcal work unit. Run via casa -c immediately after mpicasa completes.

Steps:
  1. Extract CORRECTED_DATA → *_targets.corrected.ms for each selfcal MS,
     validate with MD5, delete originals.
  2. Record selfcal_complete in the database.
  3. Move listobs files from splits/ back to the MOUS directory.
  4. Delete the (now empty) splits/ directory.
  5. Clean up auto_selfcal/, keeping only final products and key selfcal outputs.

Usage (inside a CASA container, called by manual_selfcal.sh):
    casa --nologger -c alma-sails-codebase/scripts/post_selfcal.py --mous-id uid://A001/X.../X...

Optional:
    --db-path /path/to/db
    --skip-integrity-checks   Skip MD5 validation; use when RAM is insufficient for large MSes.
                              Logs a warning to run_log; all other post-selfcal steps run normally.
"""

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.selfcal import run_post_selfcal

# Top-level items in auto_selfcal/ to keep after selfcal completes.
# Everything else is deleted. Directories are kept with all their contents.
_AUTOSELFCAL_KEEP_PATTERNS = [
    "*_targets.corrected.ms",   # final non-continuum-subtracted products
    "*_targets.contsub.ms",     # continuum-subtracted products
    "*.g",                      # gain calibration tables
    "applycal_to_orig_MSes.py",
    "uvcontsub_orig_MSes.py",
    "run_autoselfcal_entry.py",
    "selfcal_library.pickle",
    "selfcal_plan.pickle",
    "cont.dat",
    "pipeline-*",
    "weblog",
    "casa*.log",
]


def _move_listobs(splits_dir: Path, mous_dir: Path) -> None:
    """Move *.listobs.txt files from splits/ up to the MOUS directory."""
    moved = 0
    for txt in splits_dir.glob("*.listobs.txt"):
        dest = mous_dir / txt.name
        if dest.exists():
            dest.unlink()
        txt.rename(dest)
        moved += 1
    print(f"[post-selfcal] moved {moved} listobs file(s) to MOUS directory")


def _delete_splits(splits_dir: Path) -> None:
    """Delete the splits/ directory (expected to be empty after listobs move)."""
    remaining = list(splits_dir.iterdir())
    if remaining:
        print(f"[post-selfcal] WARNING: splits/ not empty ({len(remaining)} item(s)), skipping deletion")
        for r in remaining:
            print(f"    {r.name}")
        return
    splits_dir.rmdir()
    print(f"[post-selfcal] deleted splits/ directory")


def _cleanup_autoselfcal(autoselfcal_dir: Path, extra_keep: list = None) -> None:
    """Delete top-level items in auto_selfcal/ not matching _AUTOSELFCAL_KEEP_PATTERNS."""
    keep = set()
    for pattern in _AUTOSELFCAL_KEEP_PATTERNS:
        keep.update(autoselfcal_dir.glob(pattern))
    if extra_keep:
        keep.update(extra_keep)

    deleted = 0
    for item in list(autoselfcal_dir.iterdir()):
        if item in keep:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
        deleted += 1
    print(f"[post-selfcal] auto_selfcal cleanup: removed {deleted} item(s)")


def main(mous_id: str, db_path: Path, skip_integrity_checks: bool = False) -> None:
    conn = db.get_connection(db_path)

    state_row = db.get_pipeline_state(conn, mous_id)
    if state_row is None:
        print(f"[ERROR] MOUS {mous_id} not found in database")
        sys.exit(1)

    current_state = state_row["current_state"]
    if current_state != "selfcal_running":
        print(f"[ERROR] Expected state 'selfcal_running', got '{current_state}' — aborting")
        sys.exit(1)

    mous_dir = db.get_mous_directory(conn, mous_id)
    if mous_dir is None:
        print(f"[ERROR] mous_directory not set for {mous_id}")
        sys.exit(1)

    # Redirect CASA log to MOUS directory
    from datetime import datetime
    log_path = mous_dir / f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-post-selfcal.log"
    casalog.setlogfile(str(log_path))  # noqa: F821 — casalog is a CASA global
    print(f"[post-selfcal] CASA log: {log_path}")

    if skip_integrity_checks:
        print(f"[post-selfcal] WARNING: --skip-integrity-checks set; MD5 validation will be bypassed")

    print(f"[post-selfcal] Running post-selfcal for {mous_id}")
    result = run_post_selfcal(
        mous_id=mous_id,
        mous_directory=mous_dir,
        skip_integrity_checks=skip_integrity_checks,
    )

    if not result.success:
        print(f"[ERROR] Post-selfcal failed: {result.error}")
        db.record_failure(conn, mous_id=mous_id, from_state="selfcal_running",
                          error=result.error or "unknown", step="selfcal_post")
        sys.exit(1)

    nonsub = [str(p) for p in result.products_nonsub]
    sub    = [str(p) for p in result.products_sub]
    db.update_pipeline_paths(conn, mous_id,
                             selfcal_products=nonsub,
                             selfcal_products_sub=sub)
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state="selfcal_running",
        to_state="selfcal_complete",
        payload={
            "selfcal_products":          nonsub,
            "selfcal_products_sub":      sub,
            "n_corrected":               len(nonsub),
            "n_sub":                     len(sub),
            "duration_s":                result.duration_s,
            "integrity_checks_skipped":  result.integrity_checks_skipped,
        },
        step="selfcal",
    )
    conn.close()

    print(f"[post-selfcal] nonsub products ({len(nonsub)}):")
    for p in nonsub:
        print(f"    {p}")
    print(f"[post-selfcal] contsub products ({len(sub)}):")
    for p in sub:
        print(f"    {p}")
    print(f"[OK]   {mous_id} → selfcal_complete")

    # ------------------------------------------------------------------
    # 2. Move listobs files from splits/ to MOUS directory, delete splits/
    # ------------------------------------------------------------------
    splits_dir = mous_dir / "splits"
    if splits_dir.exists():
        _move_listobs(splits_dir, mous_dir)
        _delete_splits(splits_dir)
    else:
        print(f"[post-selfcal] splits/ not found — skipping listobs move")

    # ------------------------------------------------------------------
    # 3. Clean up auto_selfcal/
    # ------------------------------------------------------------------
    autoselfcal_dir = mous_dir / "auto_selfcal"
    if autoselfcal_dir.exists():
        _cleanup_autoselfcal(autoselfcal_dir, extra_keep=result.products_nonsub + result.products_sub)
    else:
        print(f"[post-selfcal] auto_selfcal/ not found — skipping cleanup")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Post-autoselfcal extraction and DB recording."
    )
    parser.add_argument("--mous-id",  required=True,   help="MOUS ID (uid://A001/...)")
    parser.add_argument("--db-path",  default=DB_PATH, help="Path to the SQLite database")
    parser.add_argument(
        "--skip-integrity-checks", action="store_true", default=False,
        help="Skip MD5 validation of CORRECTED_DATA extraction (use for very large MSes that OOM during hashing)",
    )
    args = parser.parse_args()

    main(
        mous_id=args.mous_id,
        db_path=Path(args.db_path),
        skip_integrity_checks=args.skip_integrity_checks,
    )