"""
mous_post_split_organize.py
----------------------------
Script to organize MOUS data after splitting.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
------------------------
1. Validates that the MOUS ID has a 'split' status in the database.
2. Verifies that the split products exist.
3. Updates the database to mark the split as 'complete' and removes calibrated products.
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
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from alma_ops.config import (
    DB_PATH,
    to_vm_path,
)
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_record,
    get_pipeline_state_record_column_value,
    update_pipeline_state_record,
)

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate MOUS Split Status")
def validate_mous_split_status(
    mous_id: str,
    db_path: str,
):
    """Validates that the MOUS ID has a 'split' status in the database.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str, optional
        Path to the database

    Raises
    ------
    ValueError
        If the MOUS ID is not found.
    ValueError
        If the MOUS ID does not have a 'split' status.
    """

    with get_db_connection(db_path) as conn:
        record = get_pipeline_state_record(conn, mous_id)

        if record is None:
            raise ValueError(f"MOUS ID {mous_id} not found in database.")

        status = record["pre_selfcal_split_status"]

        if status != "split":
            raise ValueError(
                f"MOUS ID {mous_id} has status '{status}', expected 'split'."
            )

        return


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Post Split Organize")
def post_split_organize_flow(
    mous_id: str,
    db_path: Optional[str] = None,
):
    """Prefect flow to organize MOUS data after splitting.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to organize.
    db_path : str, optional
        Path to the database, by default DB_PATH
    weblog_dir : str, optional
        Base directory for weblogs, by default SRDP_WEBLOG_DIR
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input parameters...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")

    # validate status is 'split'
    log.info(f"[{mous_id}] Validating mous pipeline_state status...")
    validate_mous_split_status(mous_id, db_path)

    # verify split products exist
    log.info(f"[{mous_id}] Verifying split products exist...")
    with get_db_connection(db_path) as conn:
        split_products = get_pipeline_state_record_column_value(
            conn, mous_id, "split_products_path"
        )

    if not split_products:
        raise ValueError(f"MOUS ID {mous_id} has no split products recorded.")
    for product_path in split_products:
        vm_path = to_vm_path(product_path)
        if not vm_path or not Path(vm_path).exists():
            raise ValueError(f"Split product does not exist: {vm_path}")
        log.info(f"[{mous_id}] Verified split product exists: {vm_path}")

    # update database to mark as 'complete'
    log.info(f"[{mous_id}] Updating database to mark as 'complete'...")
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(conn, mous_id, pre_selfcal_split_status="complete")

    # remove calibrated_products
    log.info(f"[{mous_id}] Removing calibrated_products from the mous directory...")
    with get_db_connection(db_path) as conn:
        calibrated_products = get_pipeline_state_record_column_value(
            conn, mous_id, "calibrated_products"
        )

    # go through each calibrated product and remove its directory
    if calibrated_products:
        for product_path in calibrated_products:
            vm_path = to_vm_path(product_path)
            if vm_path and Path(vm_path).exists():
                # TODO: move removal of calibrated products to a separate flow - post listobs
                # shutil.rmtree(vm_path, ignore_errors=True)
                log.info(f"[{mous_id}] Removed calibrated product directory: {vm_path}")
            else:
                log.warning(
                    f"[{mous_id}] Calibrated product directory does not exist: {vm_path}"
                )
