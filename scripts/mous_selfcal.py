"""
mous_selfcal.py
Invokes the auto self calibration scripts, to generate the final
pre-imaging data products.
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
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
from alma_ops.logging import get_logger

log = get_logger("mous_split")

# ---------------------------------------------------------------------
# Alma Ops imports - SELFCAL SCRIPT LOCATION GRAB
# ---------------------------------------------------------------------
import alma_ops

SELF_CAL_SCRIPT = (
    Path(alma_ops.__file__).resolve().parent / "autoselfcal_helpers" / "run_selfcal.sh"
)

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.config import (
    AUTO_SELFCAL_DIR,
    CASA_IMAGE_PIPE,
    DATASETS_DIR,
    DB_PATH,
)
from alma_ops.db import get_db_connection
from alma_ops.utils import to_dir_mous_id

# ---------------------------------------------------------------------
# CANFAR imports
# ---------------------------------------------------------------------
from canfar.sessions import Session

# =====================================================================
# Core functionality
# =====================================================================


def schedule_selfcal_for_mous(
    conn, mous_id: str, db_path: str, datasets_dir: str, dry_run: bool = False
):
    """
    Builds directory within the MOUS directory, copies all necessary
    asdms and autoselfcal files, and launches a CASA pipeline session
    to perform the tasks.
    """

    log.info(f"[{mous_id}] Schedule auto self calibration for MOUS...")

    # --- Prepare autoselfcal directory
    mous_dir = Path(datasets_dir) / to_dir_mous_id(mous_id)
    sc_dir = mous_dir / "autoselfcal"

    # Check if folder already exists
    if sc_dir.exists():
        log.info(f"[{mous_id}] Autoselfcal folder exists — skipping copy steps.")
    else:
        # Create folder
        log.info(f"[{mous_id}] Creating autoselfcal folder...")
        sc_dir.mkdir(parents=True, exist_ok=True)

        # --- Copy all .py files into autoselfcal
        py_files = list(AUTO_SELFCAL_DIR.glob("*.py"))
        for file in py_files:
            shutil.copy2(file, sc_dir)
        log.info(f"[{mous_id}] Copied {len(py_files)} .py files to autoselfcal folder.")

        # --- Copy all split files (*.ms) into autoselfcal
        splits_dir = mous_dir / "splits"
        ms_files = list(splits_dir.glob("*.ms"))

        for ms in ms_files:

            # Insert `_target` before the .ms suffix - for autoselfcal to catch the vis names
            new_name = ms.stem + "_target" + ms.suffix
            dest = sc_dir / new_name

            if dest.exists():
                log.info(f"[{mous_id}] MS directory already exists, skipping: {dest}")
                continue

            log.info(f"[{mous_id}] Copying {ms} to autoselfcal directory...")
            shutil.copytree(ms, dest)
        log.info(
            f"[{mous_id}] Copied {len(ms_files)} split products to autoselfcal folder."
        )

    # --- LAST STEP: submit the actual remote job
    if dry_run:
        log.info(f"[DRY RUN] Would submit job for {mous_id} to perform autoselfcal.")
        return

    # Initialize session manager
    session = Session()

    job_name = f"casa-{datetime.now().strftime('%Y%m%d')}-autoselfcal"
    logfile_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-autoselfcal.log"
    logfile_path = mous_dir / logfile_name

    cmd = str(SELF_CAL_SCRIPT)

    job_id = session.create(
        name=job_name,
        image=CASA_IMAGE_PIPE,
        cmd=cmd,
        args=f"{sc_dir} {logfile_path}",
    )

    # ---- Logging for the submitted job ----
    log.info("     Submitted job:")
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
    parser = argparse.ArgumentParser(
        description="Script for performing auto-self-calibration for a MOUS."
    )
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
        schedule_selfcal_for_mous(
            conn, args.mous, args.db_path, args.datasets_dir, dry_run=args.dry_run
        )
