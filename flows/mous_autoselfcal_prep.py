"""
mous_autoselfcal_prep.py
--------------------
Script to prepare datasets for automatic self-calibration on MOUS data.

This script spins up a headless session to tackle the copying of data from the
splits directory to the autoselfcal working directory.

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
from datetime import datetime
from pathlib import Path
from typing import Optional

from alma_ops.config import (
    DATASETS_DIR,
    DB_PATH,
    PROJECT_ROOT,
    WGET2_IMAGE,
    to_platform_path,
    to_vm_path,
)
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_record,
    get_pipeline_state_record_column_value,
    update_pipeline_state_record,
)
from canfar.sessions import Session
from prefect import flow, get_run_logger, task

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate MOUS Selfcal Status")
def validate_mous_selfcal_status(
    mous_id: str,
    db_path: str,
):
    """Validate that the MOUS is in the correct state to start self-calibration.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str
        Path to the database.

    Raises
    ------
    ValueError
        The MOUS is not found in the database.
    ValueError
        The MOUS pre_selfcal_listobs_status is not 'complete'.
    ValueError
        The MOUS selfcal_status is not 'pending'.
    """
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        row = get_pipeline_state_record(conn, mous_id)

    if not row:
        raise ValueError(f"MOUS ID {mous_id} not found")

    preselfcal_listobs_status = row["pre_selfcal_listobs_status"]

    if preselfcal_listobs_status != "complete":
        raise ValueError(
            f"MOUS {mous_id} has pre_selfcal_listobs_status '{preselfcal_listobs_status}', expected 'complete'"
        )

    log.info(f"[{mous_id}] MOUS pre_selfcal_listobs_status validated as 'complete'.")

    selfcal_status = row["selfcal_status"]

    if selfcal_status != "pending":
        raise ValueError(
            f"MOUS {mous_id} has selfcal_status '{selfcal_status}', expected 'pending'"
        )

    log.info(f"[{mous_id}] MOUS selfcal_status validated as 'pending'.")
    return


@task(name="Launch AutoSelfcal Prep Job Task")
def launch_autoselfcal_prep_job_task(
    mous_id: str,
    db_path: str,
    autoselfcal_mous_dir: str,
    split_products_paths: list[str],
):
    """Task that calls the function to launch the autoselfcal prep headless session.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to process.
    db_path : str
        Path to the database.
    autoselfcal_mous_dir : str
        Path to the autoselfcal MOUS directory.
    split_products_paths : list[str]
        List of paths to split products.

    Returns
    -------
    str
        The ID of the launched job.
    """
    log = get_run_logger()

    # creating job name
    job_name = f"wget2-{datetime.now().strftime('%Y%m%d-%H%M')}-autoselfcal-prep"

    run_headless_autoselfcal_prep_path = (
        Path(PROJECT_ROOT)
        / "alma-sails-codebase"
        / "alma_ops"
        / "autoselfcal"
        / "run_autoselfcal_prep.sh"
    )
    platform_run_headless_autoselfcal_path = to_platform_path(
        run_headless_autoselfcal_prep_path
    )
    log.info(
        f"[{mous_id}] Autoselfcal prep script path set as: {platform_run_headless_autoselfcal_path}"
    )

    # use platform based path for db
    platform_db_path = to_platform_path(Path(db_path))

    # call task to launch headless session job
    job_id = launch_autoselfcal_prep_headless_session(
        mous_id=mous_id,
        db_path=platform_db_path,
        job_name=job_name,
        img=WGET2_IMAGE,
        run_headless_autoselfcal_prep_path=platform_run_headless_autoselfcal_path,
        autoselfcal_mous_dir=autoselfcal_mous_dir,
        split_products_paths=split_products_paths,
    )

    log.info(f"[{mous_id}] Launched autoselfcal prep job with ID: {job_id[0]}")

    return job_id[0]


@task(name="Launch AutoSelfcal Prep Headless Session")
def launch_autoselfcal_prep_headless_session(
    mous_id: str,
    db_path: str,
    job_name: str,
    img: str,
    run_headless_autoselfcal_prep_path: str,
    autoselfcal_mous_dir: str,
    split_products_paths: list[str],
) -> str:
    """Launch a headless session to run the autoselfcal prep script.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to process.
    db_path : str
        Path to the database - platform path.
    job_name : str
        Name of the job.
    img : str
        Docker image to use for the job.
    run_headless_autoselfcal_prep_path : str
        Path to the autoselfcal prep script to run.
    autoselfcal_mous_dir : str
        Path to the autoselfcal MOUS directory.
    split_products_paths : list[str]
        List of paths to split products.

    Returns
    -------
    list[str]
        The ID(s) of the launched job(s).

    Raises
    ------
    RuntimeError
        If the job launch was unsuccessful.
    """
    log = get_run_logger()

    # create session
    session = Session()

    # construct argument based on fixed and variable inputs
    args = " ".join(
        [mous_id, str(db_path), autoselfcal_mous_dir] + split_products_paths
    )

    # submit job
    job_id = session.create(
        name=job_name, image=img, cmd=run_headless_autoselfcal_prep_path, args=args
    )

    if not job_id:
        raise RuntimeError(f"[{mous_id}] Unsuccessful job launch.")

    return job_id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="AutoSelfcal MOUS - Prep")
def autoselfcal_prep_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    datasets_dir: Optional[str] = None,
):
    """Prepare datasets for automatic self-calibration on MOUS data.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to process.
    db_path : Optional[str], optional
        The path to the database, by default None (which uses default from config).
    datasets_dir : Optional[str], optional
        The path to the datasets directory, by default None (which uses default from config).
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    datasets_dir = datasets_dir or DATASETS_DIR
    log.info(f"[{mous_id}] datasets_dir set as: {datasets_dir}")

    # validate status in database
    log.info(f"[{mous_id}] Validating MOUS selfcal status in database...")
    validate_mous_selfcal_status(mous_id=mous_id, db_path=db_path)

    # creating new directories and paths
    with get_db_connection(db_path) as conn:
        platform_mous_dir = get_pipeline_state_record_column_value(
            conn, mous_id, "mous_directory"
        )

    vm_mous_dir = to_vm_path(platform_mous_dir)

    # create autoselfcal working directory
    vm_autoselfcal_dir = vm_mous_dir / "auto_selfcal"
    vm_autoselfcal_dir.mkdir(parents=True, exist_ok=True)
    platform_autoselfcal_dir = to_platform_path(vm_autoselfcal_dir)

    # gather split products paths
    with get_db_connection(db_path) as conn:
        split_products = get_pipeline_state_record_column_value(
            conn, mous_id, "split_products_path"
        )

    # submit job to headless session
    try:
        launch_autoselfcal_prep_job_task(
            mous_id=mous_id,
            db_path=db_path,
            autoselfcal_mous_dir=platform_autoselfcal_dir,
            split_products_paths=split_products,
        )

    except Exception as e:
        log.info(f"[{mous_id}] Updating database status to error")
        with get_db_connection(db_path) as conn:
            update_pipeline_state_record(conn, mous_id, selfcal_status="error")

        log.error(f"[{mous_id}] Failed to launch autoselfcal prep job: {e}")
        raise
