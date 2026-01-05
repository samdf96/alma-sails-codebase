"""
mous_split.py
--------------------
Script for splitting ASDMs according to target name and spectral window configuration.

This script spins up a headless session to tackle the casa calls.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
-----------------------
1. Validate MOUS status in database is 'pending' for split and 'complete' for download.
2. Update database status to 'in_progress' for split.
3. Fetch calibrated product paths from database.
4. Build JSON payload for split job.
5. Write JSON payload to file.
6. Launch headless session to run split job.
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

from canfar.sessions import Session
from prefect import flow, get_run_logger, task

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
    get_pipeline_state_record,
    get_pipeline_state_record_column_value,
    update_pipeline_state_record,
)
from alma_ops.utils import to_dir_mous_id

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate Pre Selfcal Split Status")
def validate_mous_preselfcal_split_status(mous_id: str, db_path: str):
    """Validate that the MOUS pre_selfcal_split_status is 'pending' and download_status is 'complete'.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str
        Path to the SQLite database.

    Raises
    ------
    ValueError
        The MOUS ID is not found in the database.
    ValueError
        The pre_selfcal_split_status is not 'pending'.
    ValueError
        The download_status is not 'complete'.
    """
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        row = get_pipeline_state_record(conn, mous_id)

    if not row:
        raise ValueError(f"MOUS ID {mous_id} not found")

    preselfcal_split_status = row["pre_selfcal_split_status"]

    if preselfcal_split_status != "pending":
        log.warning(
            f"[{mous_id}] Status is '{preselfcal_split_status}', not 'pending'. Skipping splitting."
        )
        raise ValueError(
            f"MOUS {mous_id} has status '{preselfcal_split_status}', expected 'pending'"
        )

    log.info(f"[{mous_id}] Pre selfcal split status validated as 'pending'.")

    download_status = row["download_status"]

    if download_status != "complete":
        raise ValueError(
            f"MOUS {mous_id} has download status '{download_status}', expected 'complete'"
        )

    log.info(f"[{mous_id}] Download status validated as 'complete'.")
    return


@task(name="Build Split Job Payload")
def build_split_job_payload(
    mous_id: str,
    vm_db_path: str,
    platform_db_path: str,
    calibrated_products: list,
    mous_dir: str,
    splits_dir: str,
) -> dict:
    """Build the JSON payload for the split job.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to split.
    vm_db_path : str
        Path to the SQLite database on the VM.
    platform_db_path : str
        Path to the SQLite database on the platform.
    calibrated_products : list
        List of calibrated product paths to split.
    mous_dir : str
        Directory where the MOUS dataset is located.
    splits_dir : str
        Directory where the split measurement sets will be stored.

    Returns
    -------
    tuple(dict, list)
        The JSON payload dictionary and list of outputvis paths.

    Raises
    ------
    RuntimeError
        If no targets are found for the MOUS ID.
    """
    log = get_run_logger()
    log.info(f"[{mous_id}] Building split job payload...")

    # get spw mapping from database
    with get_db_connection(vm_db_path) as conn:
        spws = get_mous_spw_mapping(conn, mous_id)

        if not spws:
            raise RuntimeError(f"No targets found for {mous_id}.")

        log.info(f"[{mous_id}] Science SPWs found: {spws}")

    # get preferred datacolumn
    with get_db_connection(vm_db_path) as conn:
        preferred_datacolumn = get_pipeline_state_record_column_value(
            conn, mous_id, "preferred_datacolumn"
        )

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
        out_ms = splits_dir_path / f"{product_name}_targets.ms"

        # change the spws to string format for casa
        spw_str = ",".join(map(str, spws))

        # only split on OBSERVE_TARGET intents, and not per field
        tasks.append(
            {
                "task": "split",
                "vis": str(calibrated_product),
                "outputvis": str(out_ms),
                "intent": "*OBSERVE_TARGET*",
                "spw": spw_str,
                "datacolumn": str(data_column),
            }
        )

    # build full JSON payload
    payload = {
        "mous_id": mous_id,
        "db_path": str(platform_db_path),
        "mous_dir": str(mous_dir),
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
    """Write the JSON payload to a file.

    Parameters
    ----------
    payload : dict
        The JSON payload dictionary.
    output_path : Path
        The output file path to write the JSON payload to.
    """
    log = get_run_logger()
    log.info(f"[{payload['mous_id']}] Writing job payload to {output_path}...")

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=4)
    log.info(f"[{payload['mous_id']}] Wrote task file → {output_path}")
    return


@task(name="Launch Split Job Task")
def launch_split_job_task(
    mous_id: str,
    db_path: str,
    mous_dir: str,
    img: str,
    json_payload_path: str,
) -> str:
    """Calls the task that launches the headless session to run the split job.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    db_path : str
        The path to the database.
    mous_dir : str
        The directory for the MOUS dataset.
    img : str
        The docker image to use.
    json_payload_path : str
        The path to the JSON payload file.

    Returns
    -------
    str
        The job ID of the launched headless split job.
    """
    log = get_run_logger()

    # creating job name and logfile paths
    job_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M')}-splits"
    casa_logfile_name = f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-splits.log"
    casa_logfile_path = Path(mous_dir) / casa_logfile_name
    log.info(f"[{mous_id}] Logfile path set as: {casa_logfile_path}")

    # setting terminal logfile path prefix
    terminal_logfile_path_prefix = Path(mous_dir) / (
        to_dir_mous_id(mous_id) + "_splits_terminal"
    )
    log.info(
        f"[{mous_id}] Terminal logfile path prefix set as: {terminal_logfile_path_prefix}"
    )

    # set path to split script - relative to PROJECT_ROOT
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
) -> list[str]:
    """Launches a headless session to run the split job.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    db_path : str
        The path to the SQLite database.
    job_name : str
        The name of the headless session job.
    img : str
        The image to use for the job.
    run_headless_split_path : str
        The path to the headless split script.
    terminal_logfile_path_prefix : str
        The prefix for the terminal logfile path.
    casa_logfile_path : str
        The path to the CASA logfile.
    casa_driver_script_path : str
        The path to the CASA driver script.
    json_payload_path : str
        The path to the JSON payload file.

    Returns
    -------
    list[str]
        The launched job ID list.

    Raises
    ------
    RuntimeError
        If the job launch was unsuccessful.
    """
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
        raise RuntimeError("Unsuccessful job launch.")

    return job_id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Split MOUS")
def split_mous_flow(
    mous_id: str, db_path: Optional[str] = None, download_dir: Optional[str] = None
):
    """Prefect flow to split a MOUS into target-specific measurement sets.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to split.
    db_path : Optional[str], optional
        Path to the SQLite database, by default None (will use default from config).
    download_dir : Optional[str], optional
        Directory to download the dataset to, by default None (will use default from config).
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    download_dir = download_dir or DATASETS_DIR
    log.info(f"[{mous_id}] download_dir set as: {download_dir}")

    # Validate status is 'pending'
    log.info(
        f"[{mous_id}] Validating mous pipeline_state download and preselfcal split status..."
    )
    validate_mous_preselfcal_split_status(mous_id, db_path)

    # setting database splitting status to in_progress
    log.info(f"[{mous_id}] Updating database status to in_progress")
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(
            conn, mous_id, pre_selfcal_split_status="in_progress"
        )

    # fetch calibrated product locations
    log.info(f"[{mous_id}] Fetching calibrated product locations from database...")
    with get_db_connection(db_path) as conn:
        calibrated_products = get_pipeline_state_record_column_value(
            conn, mous_id, "calibrated_products"
        )

    if not calibrated_products:
        with get_db_connection(db_path) as conn:
            update_pipeline_state_record(
                conn, mous_id, pre_selfcal_split_status="error"
            )
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

    # pass in platform-specific paths
    payload, outputvis_path_list = build_split_job_payload(
        mous_id=mous_id,
        vm_db_path=db_path,  # vm path for reading spw mapping
        platform_db_path=to_platform_path(db_path),
        calibrated_products=to_platform_path(calibrated_products),
        mous_dir=to_platform_path(vm_mous_dir),
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
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(
            conn, mous_id, split_products_path=outputvis_path_list
        )

    # submit job to headless session
    try:
        launch_split_job_task(
            mous_id=mous_id,
            db_path=to_platform_path(db_path),
            mous_dir=to_platform_path(vm_mous_dir),
            img=CASA_IMAGE_PIPE,
            json_payload_path=to_platform_path(json_path),
        )

    except Exception as e:
        log.info(f"[{mous_id}] Updating database status to error")
        with get_db_connection(db_path) as conn:
            update_pipeline_state_record(
                conn, mous_id, pre_selfcal_split_status="error"
            )

        log.error(f"[{mous_id}] Split(s) failed: {e}")
        raise
