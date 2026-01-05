"""
mous_post_autoselfcal_organize.py
--------------------
Script to clean auto_selfcal directory after autoselfcal is complete.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
------------------------

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
import shutil
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

# declare list of files/directories to keep during cleanup for autoselfcal
KEEP_PATTERNS = [
    "*_targets.ms",
    "*_targets.contsub.ms",
    "*.g",
    "applycal_to_orig_MSes.py",
    "uvcontsub_orig_MSes.py",
    "selfcal_library.pickle",
    "selfcal_plan.pickle",
    "cont.dat",
    "pipeline-*",
    "weblog",
    "casa*.log",
]

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate MOUS Selfcal Status")
def validate_mous_selfcal_status(
    mous_id: str,
    db_path: str,
):
    """Validates that the MOUS ID has a 'selfcaled' status in the database.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str
        Path to the database.

    Raises
    ------
    ValueError
        If the MOUS ID is not found.
    ValueError
        If the MOUS ID does not have a 'selfcaled' status.
    """
    with get_db_connection(db_path) as conn:
        record = get_pipeline_state_record(conn, mous_id)

        if record is None:
            raise ValueError(f"MOUS ID {mous_id} not found in database.")

        status = record["selfcal_status"]

        if status != "selfcaled":
            raise ValueError(
                f"MOUS ID {mous_id} has status '{status}', expected 'selfcaled'."
            )

    return


@task(name="Cleanup AutoSelfcal Directory")
def cleanup(
    mous_dir: str,
    dry_run: bool,
):
    """Clean up task.

    Parameters
    ----------
    mous_dir : str
        The MOUS directory path.
    dry_run : bool
        If True, perform a dry run without deleting files.
    """

    # iterate over all patterns and build set of files to keep
    keep_files = set()
    for pattern in KEEP_PATTERNS:
        keep_files.update(mous_dir.glob(pattern))

    # build the set of all items
    all_items = set(mous_dir.rglob("*"))

    # determine items to delete
    delete_items = all_items - keep_files

    if dry_run:
        for item in delete_items:
            print(f"[DRY RUN] Would delete: {item}")

    else:
        for item in sorted(delete_items, reverse=True):
            shutil.rmtree(item) if item.is_dir() else item.unlink()

    return


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Post Autoselfcal Organize")
def post_autoselfcal_organize_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> None:
    """Prefect flow to clean up after autoselfcal is complete.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to process.
    db_path : Optional[str], optional
        Path to the database, by default None (uses default DB_PATH).
    dry_run : Optional[bool], optional
        If True, perform a dry run without deleting files, by default None (False).
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input parameters...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    dry_run = dry_run or False
    log.info(f"[{mous_id}] dry_run set as: {dry_run}")

    # validate selfcal status is selfcaled
    log.info(f"[{mous_id}] Validating mous pipeline_state status...")
    validate_mous_selfcal_status(mous_id, db_path)

    # get the mous_directory entry from the database
    with get_db_connection(db_path) as conn:
        mous_directory = get_pipeline_state_record_column_value(
            conn, mous_id, "mous_directory"
        )

    # set the auto_selfcal directory path
    platform_autoselfcal_dir = Path(mous_directory) / "auto_selfcal"
    vm_autoselfcal_dir = to_vm_path(platform_autoselfcal_dir)

    # make sure the auto_selfcal directory exists before attempting to clean contents
    if not vm_autoselfcal_dir.exists():
        log.warning(
            f"[{mous_id}] auto_selfcal directory does not exist at {vm_autoselfcal_dir}. Skipping cleanup."
        )
        return

    # clean up the auto_selfcal directory
    log.info(
        f"[{mous_id}] Cleaning up auto_selfcal directory at {vm_autoselfcal_dir}..."
    )

    if dry_run:
        log.info(f"[{mous_id}] Performing dry run cleanup...")
        cleanup(Path(vm_autoselfcal_dir), dry_run=True)
    else:
        log.info(f"[{mous_id}] Performing actual cleanup...")
        cleanup(Path(vm_autoselfcal_dir), dry_run=False)

    # update database to mark as 'complete'
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(conn, mous_id, selfcal_status="complete")
