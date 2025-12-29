"""
mous_split.py
--------------------
Script for splitting ASDMs according to target name and spectral window configuration.

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
    get_mous_spw_mapping,
    get_pipeline_state_calibrated_products,
    get_pipeline_state_preferred_datacolumn,
    set_pre_selfcal_split_status_error,
    set_pre_selfcal_split_status_in_progress,
)
from alma_ops.utils import to_dir_mous_id
from canfar.sessions import Session
from prefect import flow, get_run_logger, task

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate MOUS Status")
def validate_mous_split_status(mous_id: str, db_path: str):
    """Check if MOUS is in 'pending' state for split status, and 'completed' state for download status."""
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        cursor = conn.execute(
            "SELECT pre_selfcal_split_status FROM pipeline_state WHERE mous_id = ?",
            (mous_id,),
        )
        row = cursor.fetchone()

        if not row:
            log.error(f"[{mous_id}] MOUS ID not found in database.")
            raise ValueError(f"MOUS ID {mous_id} not found")

        status = row[0]

        if status != "pending":
            log.warning(
                f"[{mous_id}] Status is '{status}', not 'pending'. Skipping splitting."
            )
            raise ValueError(
                f"MOUS {mous_id} has status '{status}', expected 'pending'"
            )

        # check for download status
        cursor = conn.execute(
            "SELECT download_status FROM pipeline_state WHERE mous_id = ?",
            (mous_id,),
        )
        row = cursor.fetchone()

        download_status = row[0]

        if download_status != "complete":
            log.error(
                f"[{mous_id}] Download status is '{download_status}', expected 'complete'. Cannot proceed to splitting."
            )
            raise ValueError(
                f"MOUS {mous_id} has download status '{download_status}', expected 'complete'"
            )

        return


@task(name="Build Split Job Payload")
def build_split_job_payload(
    mous_id: str,
    vm_db_path: str,
    platform_db_path: str,
    calibrated_products: list,
    datasets_dir: str,
    splits_dir: str,
) -> dict:
    """
    Builds a *job payload* dictionary for splitting calibrated products.

    The payload needs to have platform-specific paths since it will run in headless.
    """
    log = get_run_logger()
    log.info(f"[{mous_id}] Building split job payload...")

    # get spw mapping from database
    with get_db_connection(vm_db_path) as conn:
        spw_map = get_mous_spw_mapping(conn, mous_id)
        if not spw_map:
            raise RuntimeError(f"No targets found for {mous_id}.")
        log.info(f"[{mous_id}] Found {len(spw_map)} targets.")

    # get preferred datacolumn
    with get_db_connection(vm_db_path) as conn:
        preferred_datacolumn = get_pipeline_state_preferred_datacolumn(conn, mous_id)
        if preferred_datacolumn is None:
            data_column = "data"
            log.info(f"[{mous_id}] No preferred_datacolumn set, defaulting to 'data'.")
        else:
            data_column = preferred_datacolumn
            log.info(f"[{mous_id}] Using preferred_datacolumn: {data_column}.")

    # build JSON task list
    tasks = []
    for calibrated_product in calibrated_products:
        log.info(f"[{mous_id}] Preparing split tasks for {calibrated_product}...")

        product_name = Path(calibrated_product).stem
        splits_dir_path = Path(splits_dir)
        for target_name, spws in spw_map.items():
            out_ms = splits_dir_path / f"{product_name}_{target_name}.ms"

            # change the spws to string format for casa
            spw_str = ",".join(map(str, spws))

            tasks.append(
                {
                    "task": "split",
                    "vis": str(calibrated_product),
                    "outputvis": str(out_ms),
                    "field": target_name,
                    "spw": spw_str,
                    "datacolumn": str(data_column),
                }
            )

    # build full JSON payload
    payload = {
        "mous_id": mous_id,
        "db_path": str(platform_db_path),
        "datasets_dir": str(datasets_dir),
        "tasks": tasks,
    }

    # extract all the outputvis paths for setting paths in the database
    outputvis_path_list = [task["outputvis"] for task in tasks]

    return payload, outputvis_path_list


@task(name="Write JSON Payload")
def json_write_payload(
    payload: dict,
    output_path: Path,
):
    """Write out the job payload to a JSON file."""
    log = get_run_logger()
    log.info(f"[{payload['mous_id']}] Writing job payload to {output_path}...")

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=4)
    log.info(f"[{payload['mous_id']}] Wrote task file → {output_path}")
    return


@task(name="Launch Split Job Task")
def launch_split_job_task(
    mous_id: str,
    mous_dir: str,
    db_path: str,
    img: str,
    json_payload_path: str,
):
    """Launch a headless session to run the split job."""
    log = get_run_logger()

    # creating job name and logfile paths
    job_name = f"casa-{datetime.now().strftime('%Y%m%d')}-splits"
    casa_logfile_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-splits.log"
    casa_logfile_path = Path(mous_dir) / casa_logfile_name
    log.info(f"[{mous_id}] Logfile path set as: {casa_logfile_path}")

    terminal_logfile_path_prefix = Path(mous_dir) / (
        to_dir_mous_id(mous_id) + "_splits_terminal"
    )
    log.info(
        f"[{mous_id}] Terminal logfile path prefix set as: {terminal_logfile_path_prefix}"
    )

    # set run headless split script path
    run_headless_split_path = (
        Path(PROJECT_ROOT)
        / "alma-sails-codebase"
        / "alma_ops"
        / "splits"
        / "run_split.sh"
    )
    platform_run_headless_split_path = to_platform_path(run_headless_split_path)
    log.info(
        f"[{mous_id}] Run headless split script path set as: {platform_run_headless_split_path}"
    )

    # set casa driver script path
    casa_driver_path = (
        Path(PROJECT_ROOT) / "alma-sails-codebase" / "alma_ops" / "casa_driver.py"
    )
    platform_casa_driver_path = to_platform_path(casa_driver_path)
    log.info(f"[{mous_id}] CASA driver path set as: {platform_casa_driver_path}")

    # call task to launch headless session
    job_id = launch_split_headless_session(
        mous_id=mous_id,
        db_path=db_path,
        job_name=job_name,
        img=CASA_IMAGE_PIPE,
        run_headless_split_path=platform_run_headless_split_path,
        terminal_logfile_path_prefix=str(terminal_logfile_path_prefix),
        casa_logfile_path=casa_logfile_path,
        casa_driver_script_path=platform_casa_driver_path,
        json_payload_path=json_payload_path,
    )

    log.info(f"[{mous_id}] Launched headless split job with ID: {job_id[0]}")

    return job_id[0]


@task(name="Launch Split Headless Session", retries=2, retry_delay_seconds=30)
def launch_split_headless_session(
    mous_id: str,
    db_path: str,
    job_name: str,
    img: str,
    run_headless_split_path: str,
    terminal_logfile_path_prefix: str,
    casa_logfile_path: str,
    casa_driver_script_path: str,
    json_payload_path: str,
):
    """Launch a headless session to run the split job."""
    log = get_run_logger()

    # create session
    session = Session()

    # submit job
    job_id = session.create(
        name=job_name,
        image=img,
        cmd=run_headless_split_path,
        args=f"{mous_id} {db_path} {terminal_logfile_path_prefix} {casa_driver_script_path} {casa_logfile_path} {json_payload_path}",
    )

    if not job_id:
        log.error(f"[{mous_id}] Unsuccessful job launch.")
        raise RuntimeError(f"Failed to submit headless split job for MOUS {mous_id}.")

    return job_id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Split MOUS")
def mous_split_flow(
    mous_id: str, db_path: Optional[str] = None, download_dir: Optional[str] = None
):
    """Split the raw asdms for a given mous."""
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    download_dir = download_dir or DATASETS_DIR
    log.info(f"[{mous_id}] download_dir set as: {download_dir}")

    # Validate status is 'pending'
    log.info(f"[{mous_id}] Validating mous pipeline_state status...")
    validate_mous_split_status(mous_id, db_path)

    # setting database splitting status
    log.info(f"[{mous_id}] Updating database status to in_progress")
    with get_db_connection(db_path) as conn:
        set_pre_selfcal_split_status_in_progress(conn, mous_id)

    # fetch calibrated product locations
    log.info(f"[{mous_id}] Fetching calibrated product locations from database...")
    with get_db_connection(db_path) as conn:
        calibrated_products = get_pipeline_state_calibrated_products(conn, mous_id)

    if not calibrated_products:
        log.error(f"[{mous_id}] No calibrated products found in database.")
        with get_db_connection(db_path) as conn:
            set_pre_selfcal_split_status_error(conn, mous_id)
        raise ValueError(
            f"No calibrated products found for MOUS {mous_id} in database."
        )

    log.info(f"[{mous_id}] Calibrated products: {calibrated_products}")

    # prepare output directory for splits
    vm_mous_dir = Path(download_dir) / to_dir_mous_id(mous_id)
    log.info(f"[{mous_id}] Preparing output directory at {vm_mous_dir}...")
    vm_splits_dir = vm_mous_dir / "splits"
    vm_splits_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"[{mous_id}] Output directory ready at: {vm_splits_dir}")

    # build the job payload json file and save it to mous_directory
    log.info(f"[{mous_id}] Building split-job schema...")

    log.info(
        f"Checking platform calibrated_products paths: {to_platform_path(calibrated_products)}"
    )

    # pass in platform-specific paths
    payload, outputvis_path_list = build_split_job_payload(
        mous_id=mous_id,
        vm_db_path=db_path,
        platform_db_path=to_platform_path(db_path),
        calibrated_products=to_platform_path(calibrated_products),
        datasets_dir=to_platform_path(download_dir),
        splits_dir=to_platform_path(vm_splits_dir),
    )

    # write out the json file
    json_path = vm_mous_dir / f"{to_dir_mous_id(mous_id)}_splits.json"
    json_write_payload(
        payload=payload,
        output_path=json_path,
    )

    # update database with split output paths
    log.info(
        f"[{mous_id}] Updating database with split output paths: {outputvis_path_list}"
    )
    # TODO: implement database update function to add split paths
    # into the split_product_paths field in the pipeline_state table

    # submit job to headless session
    try:
        launch_split_job_task(
            mous_id=mous_id,
            mous_dir=to_platform_path(vm_mous_dir),
            db_path=to_platform_path(db_path),
            img=CASA_IMAGE_PIPE,
            json_payload_path=to_platform_path(json_path),
        )

    except Exception as e:
        log.info(f"[{mous_id}] Updating database status to error")
        with get_db_connection(db_path) as conn:
            set_pre_selfcal_split_status_error(conn, mous_id)

        log.error(f"[{mous_id}] Split(s) failed: {e}")
        raise
