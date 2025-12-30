"""
mous_post_download_organize.py
--------------------
Script for organizing downloaded files in temporary directories.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
------------------------
1. Validates that the MOUS ID has a 'downloaded' status in the database.
2. Organizes downloaded .ms and weblog_restore directories into standardized locations.
3. Updates the database with the new file paths and marks the download as 'complete'.
"""
# ruff: noqa: E402

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------
import os
import shutil
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from alma_ops.config import (
    DATASETS_DIR,
    DB_PATH,
    SRDP_WEBLOG_DIR,
    to_platform_path,
    to_vm_path,
)
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_record,
    update_pipeline_state_record,
)
from alma_ops.utils import to_dir_mous_id

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Organize Downloaded Files")
def organize_downloaded_files(
    mous_id: str,
    mous_dir: str,
    tmpdir: str,
    weblog_dir: str,
):
    """Moves .ms and weblog_restore directories from tmpdir to mous_dir and weblog_dir.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    mous_dir : str
        Path to the MOUS dataset directory.
    tmpdir : str
        Path to the temporary download directory holding downloaded files.
    weblog_dir : str
        Path to the weblog_restore directory.

    Returns
    -------
    list[str]
        List of paths to the moved calibrated products (.ms directories).
    """
    log = get_run_logger()

    log.info(f"[{mous_id}] Starting organization of downloaded files...")

    # initialize lists
    ms_dirs, weblog_dirs = [], []
    for root, dirs, files in os.walk(tmpdir):
        for d in list(dirs):
            if d.endswith(".ms"):
                ms_dirs.append(os.path.join(root, d))
                dirs.remove(d)
            elif d == "weblog_restore":
                weblog_dirs.append(os.path.join(root, d))
                dirs.remove(d)

    ms_dirs = sorted(set(ms_dirs))
    log.info(
        f"Found {len(ms_dirs)} .ms directories:\n"
        + "\n".join(f"  {i + 1}. {ms}" for i, ms in enumerate(ms_dirs))
    )
    weblog_dirs = sorted(set(weblog_dirs))
    log.info(
        f"Found {len(weblog_dirs)} weblog_restore directories:\n"
        + "\n".join(f"  {i + 1}. {wb}" for i, wb in enumerate(weblog_dirs))
    )

    calibrate_products_paths = []

    if ms_dirs:
        for ms in ms_dirs:
            dest = Path(mous_dir) / Path(ms).name
            shutil.move(ms, dest)
            calibrate_products_paths.append(str(dest))
        log.info(f"[{mous_id}] Moved {len(ms_dirs)} .ms → {mous_dir}")

    if weblog_dirs:
        for wb in weblog_dirs:
            dest = Path(weblog_dir) / Path(wb).name
            shutil.move(wb, dest)
        log.info(f"[{mous_id}] Moved {len(weblog_dirs)} weblog_restore → {weblog_dir}")

    return calibrate_products_paths


@task(name="Validate MOUS Downloaded and Temporary Directory Status")
def validate_mous_download_status(mous_id: str, db_path: str) -> str:
    """Ensures the download status is 'downloaded' and retrieves the temporary download directory.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str
        Path to the SQLite database.

    Returns
    -------
    str
        The temporary download directory.

    Raises
    ------
    ValueError
        If the MOUS ID is not found in the database.
    ValueError
        If the download status is not 'downloaded'.
    ValueError
        If the temporary download directory is not set.
    """
    log = get_run_logger()

    # gather the mous_id record
    with get_db_connection(db_path) as conn:
        row = get_pipeline_state_record(conn, mous_id)

    if not row:
        raise ValueError(f"MOUS ID {mous_id} not found")

    download_status = row["download_status"]

    if download_status != "downloaded":
        raise ValueError(
            f"MOUS {mous_id} has status '{download_status}', expected 'downloaded'"
        )

    log.info(f"[{mous_id}] download_status validated as 'downloaded'.")

    # get tmpdir location stored in the mous_directory column
    tmpdir = row["mous_directory"]

    if not tmpdir:
        raise ValueError(f"MOUS ID {mous_id} has no mous_directory set")

    log.info(f"[{mous_id}] Download tmpdir found to be: {tmpdir}.")
    return tmpdir


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Post Download Organize")
def post_download_organize_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    datasets_dir: Optional[str] = None,
    weblog_dir: Optional[str] = None,
):
    """Prefect flow to organize downloaded files for a MOUS ID.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    db_path : Optional[str], optional
        Path to the SQLite database, by default None (which uses default from config).
    datasets_dir : Optional[str], optional
        Path to the datasets directory, by default None (which uses default from config).
    weblog_dir : Optional[str], optional
        Path to the weblog directory, by default None (which uses default from config).
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    datasets_dir = datasets_dir or DATASETS_DIR
    log.info(f"[{mous_id}] datasets_dir set as: {datasets_dir}")
    weblog_dir = weblog_dir or SRDP_WEBLOG_DIR
    log.info(f"[{mous_id}] weblog_dir set as: {weblog_dir}")

    # validate status is 'downloaded'
    log.info(f"[{mous_id}] Validating mous pipeline_state status...")
    tmpdir = validate_mous_download_status(mous_id, db_path)

    # change tmpdir to be vm-style so the organize files can work
    tmpdir = to_vm_path(tmpdir)

    # create the appropriate mous_dir
    mous_dir = Path(datasets_dir) / to_dir_mous_id(mous_id)
    mous_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"[{mous_id}] Created MOUS directory at: {mous_dir}")

    # create the appropriate weblog_dir
    weblog_mous_dir = Path(weblog_dir) / to_dir_mous_id(mous_id)
    weblog_mous_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"[{mous_id}] Created weblog_restore directory at: {weblog_mous_dir}")

    # organizing downloaded files
    log.info(f"[{mous_id}] Organizing downloaded files...")
    calibrated_products = organize_downloaded_files(
        mous_id, str(mous_dir), str(tmpdir), str(weblog_mous_dir)
    )

    # updating database with mous_directory, calibrated_products, and complete state
    with get_db_connection(db_path) as conn:
        # convert mous_directory to platform path for database storage
        platform_mous_directory = to_platform_path(mous_dir)
        update_pipeline_state_record(
            conn,
            mous_id,
            mous_directory=platform_mous_directory,
        )
        # convert calibrated_products to platform paths for database storage
        platform_calibrated_products = to_platform_path(calibrated_products)
        update_pipeline_state_record(
            conn,
            mous_id,
            calibrated_products=platform_calibrated_products,
        )

        # finally setting download_status to complete
        update_pipeline_state_record(
            conn,
            mous_id,
            download_status="complete",
        )
    log.info(
        f"[{mous_id}] Updated database records with organized file paths and status."
    )

    # finally removing temporary directory
    log.info(f"[{mous_id}] Removing temporary directory...")
    # TODO: re-enable tmpdir removal once testing is complete
    # shutil.rmtree(tmpdir)

    log.info(f"✅ Post Download Organize Completed: {mous_id}")
