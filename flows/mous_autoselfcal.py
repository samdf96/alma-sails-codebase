"""
mous_autoselfcal.py
--------------------
Script to perform automatic self-calibration on MOUS data.

This script spins up a headless session to run the main auto_selfcal script.

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

from canfar.sessions import Session
from prefect import flow, get_run_logger, task

from alma_ops.config import (
    AUTO_SELFCAL_ENTRY_SCRIPT,
    CASA_IMAGE_PIPE,
    DATASETS_DIR,
    DB_PATH,
    PROJECT_ROOT,
    to_platform_path,
    to_vm_path,
)
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_record,
    get_pipeline_state_record_column_value,
    update_pipeline_state_record,
)
from alma_ops.utils import to_dir_mous_id

# =====================================================================
# Configuration for Headless Session
# =====================================================================
# the number of cores and memory to allocate to the headless session
NCORES = 4
MEM_GB = 32
# whether to use the above fixed session configuration
USE_FIXED_SESSION = True

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
        The MOUS selfcal_status is not 'prepped'.
    """
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        row = get_pipeline_state_record(conn, mous_id)

    if not row:
        raise ValueError(f"MOUS ID {mous_id} not found")

    selfcal_status = row["selfcal_status"]

    if selfcal_status != "prepped":
        raise ValueError(
            f"MOUS {mous_id} has selfcal_status '{selfcal_status}', expected 'prepped'"
        )

    log.info(f"[{mous_id}] MOUS selfcal_status validated as 'prepped'.")
    return


@task(name="Launch AutoSelfcal Job Task")
def launch_autoselfcal_job_task(
    mous_id: str,
    platform_db_path: str,
    platform_mous_dir: str,
    platform_autoselfcal_dir: str,
):
    log = get_run_logger()

    # creating job name and logfile paths
    job_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M')}-autoselfcal"

    # setting terminal logfile path prefix
    terminal_logfile_path_prefix = Path(platform_mous_dir) / (
        to_dir_mous_id(mous_id) + "_autoselfcal_terminal"
    )
    log.info(
        f"[{mous_id}] Terminal logfile path prefix set as: {terminal_logfile_path_prefix}"
    )

    # set path to autoselfcal script
    if USE_FIXED_SESSION:
        run_headless_autoselfcal_path = (
            Path(PROJECT_ROOT)
            / "alma-sails-codebase"
            / "alma_ops"
            / "autoselfcal"
            / "run_autoselfcal_fixed.sh"
        )
    else:
        run_headless_autoselfcal_path = (
            Path(PROJECT_ROOT)
            / "alma-sails-codebase"
            / "alma_ops"
            / "autoselfcal"
            / "run_autoselfcal_flexible.sh"
        )
    platform_run_headless_autoselfcal_path = to_platform_path(
        run_headless_autoselfcal_path
    )
    log.info(
        f"[{mous_id}] run_headless_autoselfcal script path set as: {platform_run_headless_autoselfcal_path}"
    )

    # set autoselfcal entry script path to be platform path
    vm_autoselfcal_entry_script = to_platform_path(str(AUTO_SELFCAL_ENTRY_SCRIPT))

    # call task to launch headless session job
    job_id = launch_autoselfcal_headless_session(
        mous_id=mous_id,
        db_path=platform_db_path,
        job_name=job_name,
        img=CASA_IMAGE_PIPE,
        run_headless_autoselfcal_path=platform_run_headless_autoselfcal_path,
        terminal_logfile_path_prefix=str(terminal_logfile_path_prefix),
        autoselfcal_entry_script=str(vm_autoselfcal_entry_script),
        autoselfcal_mous_dir=platform_autoselfcal_dir,
    )

    log.info(f"[{mous_id}] Launched auto_selfcal job with Job ID: {job_id[0]}")

    return job_id[0]


@task(name="Launch AutoSelfcal Headless Session", retries=2, retry_delay_seconds=20)
def launch_autoselfcal_headless_session(
    mous_id: str,
    db_path: str,
    job_name: str,
    img: str,
    run_headless_autoselfcal_path: str,
    terminal_logfile_path_prefix: str,
    autoselfcal_entry_script: str,
    autoselfcal_mous_dir: str,
):
    log = get_run_logger()

    # create session
    session = Session()

    # submit job
    if USE_FIXED_SESSION:
        log.info(
            f"[{mous_id}] Launching headless session with fixed resources: {NCORES} cores, {MEM_GB} GB memory"
        )
        job_id = session.create(
            name=job_name,
            image=img,
            cores=NCORES,
            ram=MEM_GB,
            cmd=run_headless_autoselfcal_path,
            args=f"{mous_id} {db_path} {terminal_logfile_path_prefix} {autoselfcal_entry_script} {autoselfcal_mous_dir} {NCORES} {MEM_GB}",
        )
    else:
        log.info(f"[{mous_id}] Launching headless session with flexible resources")
        job_id = session.create(
            name=job_name,
            image=img,
            cmd=run_headless_autoselfcal_path,
            args=f"{mous_id} {db_path} {terminal_logfile_path_prefix} {autoselfcal_entry_script} {autoselfcal_mous_dir}",
        )

    if not job_id:
        raise RuntimeError("Unsuccessful job launch.")

    return job_id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="AutoSelfcal MOUS")
def autoselfcal_mous_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    datasets_dir: Optional[str] = None,
):
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    datasets_dir = datasets_dir or DATASETS_DIR
    log.info(f"[{mous_id}] datasets_dir set as: {datasets_dir}")

    # validate status in database
    log.info(f"[{mous_id}] Validating MOUS pipeline_state statuses in database...")
    validate_mous_selfcal_status(mous_id, db_path)

    # setting the database selfcal status to 'in_progress'
    log.info(f"[{mous_id}] Setting MOUS selfcal_status to 'in_progress'")
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(conn, mous_id, selfcal_status="in_progress")

    # organizing and creating new directories and paths
    with get_db_connection(db_path) as conn:
        platform_mous_dir = get_pipeline_state_record_column_value(
            conn, mous_id, "mous_directory"
        )
    vm_mous_dir = to_vm_path(platform_mous_dir)

    # set auto_selfcal directory paths
    vm_autoselfcal_dir = vm_mous_dir / "auto_selfcal"
    platform_autoselfcal_dir = to_platform_path(vm_autoselfcal_dir)
    # verify that the auto_selfcal directory exists
    if not vm_autoselfcal_dir.exists():
        raise FileNotFoundError(
            f"[{mous_id}] auto_selfcal directory not found at {vm_autoselfcal_dir}"
        )

    # # set timestamp for the selfcal portion of this run
    # TODO: implement start time tracking with CURRENT_TIMESTAMP implementation
    # with get_db_connection(db_path) as conn:
    #     update_pipeline_state_record(
    #         conn, mous_id, selfcal_started_at=datetime.now()
    #     )

    # submit job to headless session
    try:
        launch_autoselfcal_job_task(
            mous_id=mous_id,
            platform_db_path=to_platform_path(db_path),
            platform_mous_dir=platform_mous_dir,
            platform_autoselfcal_dir=platform_autoselfcal_dir,
        )
    except Exception as e:
        log.info(f"[{mous_id}] Updating database status to error")
        # update database status to 'error'
        with get_db_connection(db_path) as conn:
            update_pipeline_state_record(conn, mous_id, selfcal_status="error")

        log.error(f"[{mous_id}] AutoSelfcal failed: {e}")
        raise
