"""
mous_split.py
For splitting ALMA MOUS datasets according to the targets found in the
database.
"""

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops when running from scripts/)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
from alma_ops.logging import get_logger

log = get_logger("mous_split")

# ---------------------------------------------------------------------
# CANFAR imports
# ---------------------------------------------------------------------
from canfar.sessions import Session

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.config import CASA_IMAGE, DATASETS_DIR, DB_PATH, SRDP_WEBLOG_DIR
from alma_ops.db import db_fetch_one, get_db_connection, get_mous_spw_mapping
from alma_ops.utils import to_dir_mous_id

# =====================================================================
# Core functionality
# =====================================================================


def schedule_split_for_mous(
    conn, mous_id: str, db_path: str, datasets_dir: str, dry_run: bool = False
):
    """
    Builds a *job payload* and submit it to alma_ops for headless
    CASA execution.
    """
    log.info(f"[{mous_id}] Schedule splitting for MOUS...")
    log.info(f"[{mous_id}] Building split-job schema...")

    # --- Fetch ASDMs
    result = db_fetch_one(
        conn, "SELECT asdm_paths FROM mous WHERE mous_id=?", (mous_id,)
    )
    if not result:
        raise ValueError(f"MOUS {mous_id} not found.")

    asdm_paths = json.loads(result["asdm_paths"] or "[]")
    if not asdm_paths:
        raise RuntimeError(f"No ASDMs recorded for {mous_id}.")
    log.info(f"[{mous_id}] Found {len(asdm_paths)}.")

    # --- SPW map
    spw_map = get_mous_spw_mapping(conn, mous_id)
    if not spw_map:
        raise RuntimeError(f"No targets found for {mous_id}.")
    log.info(f"[{mous_id}] Found {len(spw_map)} targets.")

    # --- Prepare output dir
    mous_dir = Path(datasets_dir) / to_dir_mous_id(mous_id)
    splits_dir = mous_dir / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)

    # --- Build JSON task list -
    tasks = []
    for asdm_path in asdm_paths:
        asdm_name = Path(asdm_path).stem
        for target, spws in spw_map.items():
            out_ms = splits_dir / f"{asdm_name}_{target}.ms"

            # change the spws to string format for casa call
            spw_str = ",".join(map(str, spws))

            tasks.append(
                {
                    "task": "split",
                    "vis": str(asdm_path),
                    "outputvis": str(out_ms),
                    "field": target,
                    "spw": spw_str,
                    "datacolumn": "data",
                }
            )

    # --- Full JSON payload
    payload = {
        "mous_id": mous_id,
        "db_path": str(db_path),
        "datasets_dir": str(datasets_dir),
        "tasks": tasks,
    }

    # --- Write JSON file
    json_path = mous_dir / f"{to_dir_mous_id(mous_id)}_splits.json"
    with json_path.open("w") as f:
        json.dump(payload, f, indent=2)
    log.info(f"[{mous_id}] Wrote task file → {json_path}")

    # --- LAST STEP: submit the actual remote job
    if dry_run:
        log.info(
            f"[DRY RUN] Would submit job for {mous_id} with {len(tasks)} separate splits."
        )
        return

    log.info(f"→ Submitting headless alma_ops split job(s) for MOUS {mous_id}...")

    # Initialize session manager
    session = Session()

    job_name = f"casa-{datetime.now().strftime('%Y%m%d')}-splits"
    logfile_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-splits.log"
    logfile_path = mous_dir / logfile_name

    args_str = " ".join(
        [
            "--logfile",
            str(logfile_path),
            "-c",
            "/arc/projects/ALMA-SAILS/alma-sails-codebase/alma_ops/casa_driver.py",
            f"--json-payload {json_path}",
        ]
    )

    job_id = session.create(
        name=job_name,
        image=CASA_IMAGE,
        cmd="casa",
        args=args_str,
    )

    # ---- Logging for the submitted job ----
    log.info(f"Submitted job:")
    log.info(f"      Job ID:     {job_id}")
    log.info(f"      Job name:   {job_name}")
    log.info(f"      Log file:   {logfile_path}")
    log.info("")
    log.info(f"      → To view job info:  canfar info {job_id[0]}")
    log.info("")

    log.info(f"✅ MOUS {mous_id} scheduled for splitting.")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for splitting a MOUS.")
    parser.add_argument("mous", help="MOUS ID (e.g., uid://A001/X12d1/X22e)")
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument("--datasets-dir", default=DATASETS_DIR)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate without spinning up headless session.",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------
    # Echo back arguments to user before proceeding
    # ------------------------------------------------------------
    print("\n================= ARGUMENT SUMMARY =================")
    print(f"MOUS ID:             {args.mous}")
    print(f"Database path:       {args.db_path}")
    print(f"Datasets directory:  {args.datasets_dir}")
    print(f"Dry run:             {args.dry_run}")
    print("====================================================\n")

    # ------------------------------------------------------------
    # Confirmation
    # ------------------------------------------------------------
    while True:
        confirm = input("Proceed with these settings? [y/N]: ").strip().lower()

        if confirm in ("y", "yes"):
            print("Confirmed — continuing.\n")
            break
        elif confirm in ("n", "no", ""):
            print("Aborted by user.")
            sys.exit(1)
        else:
            print("Please answer with 'y' or 'n'.")

    with get_db_connection(args.db_path) as conn:
        schedule_split_for_mous(
            conn, args.mous, args.db_path, args.datasets_dir, dry_run=args.dry_run
        )
