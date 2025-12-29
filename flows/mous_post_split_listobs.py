"""
mous_post_split_listobs.py
--------------------
Script to generate listobs outputs for raw and split ASDMs.

This script spins up a headless session to tackle the casa calls.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory
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
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from alma_ops.config import (
    CASA_IMAGE_PIPE,
    DATASETS_DIR,
    DB_PATH,
    PROJECT_ROOT,
    to_platform_path,
)
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_calibrated_products,
    get_pipeline_state_split_products_path,
    set_pre_selfcal_listobs_status_error,
    set_pre_selfcal_listobs_status_in_progress,
)
from alma_ops.utils import to_dir_mous_id
from canfar.sessions import Session
from prefect import flow, get_run_logger, task

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate MOUS Status")
def validate_mous_status(mous_id: str, db_path: str):
    """Check if MOUS is in 'complete' status for pre_selfcal_split status and 'pending' for pre_selfcal_listobs status."""
    log = get_run_logger()

    # first check for 'complete' status on pre_selfcal_split_status
    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            "SELECT pre_selfcal_split_status FROM pipeline_state WHERE mous_id = ?",
            (mous_id,),
        )
        row = cursor.fetchone()

        if not row:
            log.error(f"[{mous_id}] MOUS ID not found in database.")
            raise ValueError(f"MOUS ID not found: {mous_id}")

        status = row[0]

        # TODO: remove 'complete' check when split step is fully retired
        if status != "complete" and status != "split":
            log.error(
                f"[{mous_id}] pre_selfcal_split_status is '{status}', expected 'complete'."
            )
            raise ValueError(
                f"Invalid pre_selfcal_split_status for MOUS {mous_id}: {status}"
            )

        # now check for pending status on pre_selfcal_listobs_status
        cursor = conn.execute(
            "SELECT pre_selfcal_listobs_status FROM pipeline_state WHERE mous_id = ?",
            (mous_id,),
        )
        row = cursor.fetchone()
        listobs_status = row[0]

        if listobs_status != "pending":
            log.error(
                f"[{mous_id}] pre_selfcal_listobs_status is '{listobs_status}', expected 'pending'."
            )
            raise ValueError(
                f"Invalid pre_selfcal_listobs_status for MOUS {mous_id}: {listobs_status}"
            )

        return


@task(name="Build Split Job Payload")
def build_split_job_payload(
    mous_id: str,
    platform_db_path: str,
    platform_datasets_dir: str,
    platform_calibrated_products_path: list[str],
    platform_split_products_path: list[str],
):
    # build JSON task list
    tasks = []
    for calibrated_product in platform_calibrated_products_path:
        ms_path = Path(calibrated_product)
        listfile = ms_path.with_suffix(ms_path.suffix + ".listobs.txt")

        tasks.append(
            {
                "task": "listobs",
                "vis": str(ms_path),
                "listfile": str(listfile),
            }
        )

    for split_product in platform_split_products_path:
        ms_path = Path(split_product)
        listfile = ms_path.with_suffix(ms_path.suffix + ".listobs.txt")

        tasks.append(
            {
                "task": "listobs",
                "vis": str(ms_path),
                "listfile": str(listfile),
            }
        )

    # build the payload
    payload = {
        "mous_id": mous_id,
        "db_path": str(platform_db_path),
        "datasets_dir": str(platform_datasets_dir),
        "tasks": tasks,
    }

    return payload


@task(name="Write JSON Payload")
def json_write_payload(
    payload: dict,
    output_path: Path,
):
    """Write the JSON payload to a file."""
    log = get_run_logger()
    log.info(f"Writing JSON payload to {output_path}...")

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=4)

    log.info(f"[{payload['mous_id']}] Wrote task file → {output_path}")
    return


@task(name="Launch Listobs Job Task")
def launch_listobs_job_task(
    mous_id: str,
    platform_mous_dir: str,
    platform_db_path: str,
    img: str,
    json_payload_path: str,
):
    log = get_run_logger()

    # creating job name and logfile paths
    job_name = f"casa-{datetime.now().strftime('%Y%m%d')}-listobs"
    casa_logfile_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-listobs.log"
    casa_logfile_path = Path(platform_mous_dir) / casa_logfile_name
    log.info(f"[{mous_id}] Logfile path: {casa_logfile_path}")

    # setting terminal logfile path prefix
    terminal_logfile_path_prefix = Path(platform_mous_dir) / (
        to_dir_mous_id(mous_id) + "_listobs_terminal"
    )
    log.info(
        f"[{mous_id}] Terminal logfile path prefix: {terminal_logfile_path_prefix}"
    )

    # set run headless listobs script path
    run_headless_listobs_path = (
        Path(PROJECT_ROOT)
        / "alma-sails-codebase"
        / "alma_ops"
        / "listobs"
        / "run_listobs.sh"
    )
    platform_run_headless_listobs_path = to_platform_path(run_headless_listobs_path)
    log.info(f"[{mous_id}] run_listobs.sh path: {platform_run_headless_listobs_path}")

    # set casa driver script path
    casa_driver_path = (
        Path(PROJECT_ROOT) / "alma-sails-codebase" / "alma_ops" / "casa_driver.py"
    )
    platform_casa_driver_path = to_platform_path(casa_driver_path)
    log.info(f"[{mous_id}] CASA driver path set as: {platform_casa_driver_path}")

    # call task to launch headless session
    job_id = launch_headless_listobs_session(
        mous_id=mous_id,
        db_path=platform_db_path,
        job_name=job_name,
        img=img,
        run_headless_listobs_script_path=platform_run_headless_listobs_path,
        terminal_logfile_path_prefix=str(terminal_logfile_path_prefix),
        casa_logfile_path=casa_logfile_path,
        casa_driver_script_path=platform_casa_driver_path,
        json_payload_path=json_payload_path,
    )

    log.info(f"[{mous_id}] Launched headless listobs job with ID: {job_id[0]}")

    return job_id[0]


@task(name="Launch Headless Listobs Session", retries=2, retry_delay_seconds=30)
def launch_headless_listobs_session(
    mous_id: str,
    db_path: str,
    job_name: str,
    img: str,
    run_headless_listobs_script_path: str,
    terminal_logfile_path_prefix: str,
    casa_logfile_path: str,
    casa_driver_script_path: str,
    json_payload_path: str,
):
    log = get_run_logger()

    # create session manager
    session = Session()

    # submit job
    job_id = session.create(
        name=job_name,
        image=img,
        cmd=run_headless_listobs_script_path,
        args=f"{mous_id} {db_path} {terminal_logfile_path_prefix} {casa_driver_script_path} {casa_logfile_path} {json_payload_path}",
    )

    if not job_id:
        log.error(f"[{mous_id}] Failed to submit headless listobs job.")
        raise RuntimeError(f"Failed to submit headless listobs job for MOUS {mous_id}")

    return job_id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Listobs MOUS")
def mous_listobs_flow(
    mous_id: str, db_path: Optional[str] = None, datasets_dir: Optional[str] = None
):
    """Listobs the raw and split ASDMs for a given MOUS ID."""
    log = get_run_logger()

    # parsing input information
    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    datasets_dir = datasets_dir or DATASETS_DIR
    log.info(f"[{mous_id}] datasets_dir set as: {datasets_dir}")

    # validate pre_selfcal_split_status is 'complete'
    log.info(f"[{mous_id}] Validating pre_selfcal_split_status...")
    validate_mous_status(mous_id, db_path)

    # setting database status to 'in_progress'
    log.info(f"[{mous_id}] Setting pre_selfcal_listobs_status to 'in_progress'...")
    with get_db_connection(db_path) as conn:
        set_pre_selfcal_listobs_status_in_progress(conn, mous_id)

    # fetch calibrated product locations and split product locations
    log.info(f"[{mous_id}] Fetching calibrated and split product locations...")
    with get_db_connection(db_path) as conn:
        calibrated_products_path = get_pipeline_state_calibrated_products(conn, mous_id)
        split_products_path = get_pipeline_state_split_products_path(conn, mous_id)

    log.info(f"[{mous_id}] Calibrated products path: {calibrated_products_path}")
    log.info(f"[{mous_id}] Split products path: {split_products_path}")

    if not calibrated_products_path:
        log.error(f"[{mous_id}] No calibrated products found in database.")
        with get_db_connection(db_path) as conn:
            set_pre_selfcal_listobs_status_error(conn, mous_id)
        raise ValueError(f"No calibrated products for MOUS {mous_id}")

    if not split_products_path:
        log.error(f"[{mous_id}] No split products found in database.")
        with get_db_connection(db_path) as conn:
            set_pre_selfcal_listobs_status_error(conn, mous_id)
        raise ValueError(f"No split products for MOUS {mous_id}")

    payload = build_split_job_payload(
        mous_id,
        to_platform_path(db_path),
        to_platform_path(datasets_dir),
        [to_platform_path(p) for p in calibrated_products_path],
        [to_platform_path(p) for p in split_products_path],
    )

    # write JSON payload to file
    vm_mous_dir = Path(datasets_dir) / to_dir_mous_id(mous_id)
    json_output_path = vm_mous_dir / f"{to_dir_mous_id(mous_id)}_listobs.json"
    json_write_payload(
        payload,
        json_output_path,
    )

    try:
        launch_listobs_job_task(
            mous_id=mous_id,
            platform_mous_dir=to_platform_path(vm_mous_dir),
            platform_db_path=to_platform_path(db_path),
            img=CASA_IMAGE_PIPE,
            json_payload_path=str(to_platform_path(json_output_path)),
        )
    except Exception as e:
        log.error(f"[{mous_id}] Error launching headless listobs job: {e}")
        with get_db_connection(db_path) as conn:
            set_pre_selfcal_listobs_status_error(conn, mous_id)

        log.error(f"[{mous_id}] Listobs(s) failed: {e}")
        raise
