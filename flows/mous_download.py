"""
mous_download.py
--------------------
Script for downloading ALMA MOUS datasets from a pre-configured NRAO SRDP url.

This script spins up a headless session to run the wget2 download command
within a containerized environment. This prevents platform write issues
when running over an sshfs-mounted connection.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
------------------------
1. Validate MOUS status is 'pending' and has a valid download URL.
2. Update database status to 'in_progress'.
3. Launch headless session to run wget2 download command.
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
import tempfile
import time
from datetime import datetime
from typing import Optional

from canfar.sessions import Session
from prefect import flow, get_run_logger, task

from alma_ops.config import (
    DATASETS_DIR,
    DB_PATH,
    PROJECT_ROOT,
    SRDP_WEBLOG_DIR,
    WGET2_IMAGE,
    to_platform_path,
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


@task(name="Launch Headless Download Session", retries=2, retry_delay_seconds=60)
def launch_download_headless_session(
    mous_id: str,
    db_path: str,
    job_name: str,
    img: str,
    cmd: str,
    tmpdir: str,
    url: str,
) -> list[str]:
    """Launches a headless session to run the download command.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    db_path : str
        Path to the SQLite database.
    job_name : str
        Job name for the headless session.
    img : str
        Docker image to use for the headless session.
    cmd : str
        Command to run in the headless session.
    tmpdir : str
        Temporary directory for the download.
    url : str
        URL to download from.
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

    # initialize the session
    session = Session()

    job_id = session.create(
        name=job_name,
        image=img,
        cmd=cmd,
        args=f"{mous_id} {db_path} {tmpdir} {url}",
    )

    if not job_id:
        raise RuntimeError("Unsuccessful job launch.")

    return job_id


@task(name="Launch Download Job")
def launch_download_job_task(
    mous_id: str, url: str, download_dir: str, db_path: str
) -> tuple[str, str]:
    """Calls the task that launches the headless session to run the download.

    Parameters
    ----------
    mous_id : str
        The MOUS ID.
    url : str
        The download URL.
    download_dir : str
        The base directory to create the temporary download directory in.
    db_path : str
        Path to the SQLite database.

    Returns
    -------
    tuple[str, str]
        The launched job ID and the temporary directory path.
    """
    log = get_run_logger()

    # creating temp directory
    tmpdir = tempfile.mkdtemp(prefix=f"{to_dir_mous_id(mous_id)}_", dir=download_dir)
    log.info(f"[{mous_id}] Creating temporary directory: {tmpdir}")
    os.makedirs(tmpdir, exist_ok=True)

    # creating job name for headless session
    job_name = f"wget2-{datetime.now().strftime('%Y%m%d_%H%M')}"

    # for headless sessions: use platform-native paths
    log.info(f"[{mous_id}] Setting platform-native paths for headless session launch")

    platform_tmpdir = to_platform_path(tmpdir)
    log.info(f"[{mous_id}] Platform tmpdir location set as: {platform_tmpdir}")

    # setting path to download script - relative to PROJECT_ROOT
    run_download_script_filepath = to_platform_path(
        str(
            PROJECT_ROOT
            / "alma-sails-codebase"
            / "alma_ops"
            / "downloads"
            / "run_download.sh"
        )
    )
    log.info(
        f"[{mous_id}] Platform download script location set as: {run_download_script_filepath}"
    )
    platform_db_path = to_platform_path(db_path)
    log.info(f"[{mous_id}] Platform db_path location set as: {platform_db_path}")

    # updating database to contain the tmpdir in the mous_directory spot
    # so that the organize files script can read where the data is
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(conn, mous_id, mous_directory=f"{platform_tmpdir}")

    # call task to launch headless session
    job_id = launch_download_headless_session(
        mous_id=mous_id,
        db_path=platform_db_path,
        job_name=job_name,
        img=WGET2_IMAGE,
        cmd=run_download_script_filepath,
        tmpdir=platform_tmpdir,
        url=url,
    )

    log.info(f"[{mous_id}] Launched download job: {job_id[0]}")
    return job_id[0], tmpdir


@task(name="Monitor Download Headless Session")
def monitor_download_headless_session(job_id: str, mous_id: str):
    """Monitor the headless session for its end state."""
    log = get_run_logger()

    # initialize the session
    session = Session()

    # initiate response tracking
    log.info(f"[{mous_id}] Setting response tracking values...")
    consecutive_empty_responses = 0
    max_empty_responses = 3  # allows some transient failures

    # start with waiting - helps the session get into the API calls
    log.info(f"[{mous_id}] Pre-wait period before first session.info() call.")
    time.sleep(10)

    while True:
        try:
            # query job status
            log.info(f"[{mous_id}] Conducting API call through session.info().")
            session_info = session.info(ids=job_id)

            if not session_info:
                # tally the empty response
                consecutive_empty_responses += 1
                log.warning(f"[{mous_id}] Empty response from session.info()")
                log.warning(f"{consecutive_empty_responses}/{max_empty_responses}")

                # if too many consecutive failures, something is wrong
                if consecutive_empty_responses >= max_empty_responses:
                    raise RuntimeError(
                        f"Job {job_id} returned empty info {max_empty_responses} times - may have expired or been deleted"
                    )

                # wait and retry for next check
                time.sleep(60)
                continue

            # reset counter on successful response
            consecutive_empty_responses = 0

            # now we check the status
            status = session_info[0]["status"]
            log.debug(f"[{mous_id}] Job status: {status}")

            if status in ["Succeeded", "Failed", "Terminated"]:
                log.info(f"Job completed with status: {status}")

                if status == "Succeeded":
                    return True
                else:
                    raise RuntimeError(f"Job failed with status: {status}")

        except Exception as e:
            log.error(f"[{mous_id}] Error checking job status: {e}")

            # don't retry on known errors
            if isinstance(e, (RuntimeError)):
                raise

            # for other errors, wait and retry
            consecutive_empty_responses += 1
            if consecutive_empty_responses >= max_empty_responses:
                raise

        # wait before next check
        log.debug(f"[{mous_id}] Waiting 60s before next check...")
        time.sleep(60)


@task(name="Validate Download and URL Values")
def validate_mous_download_status(mous_id: str, db_path: str) -> tuple[str, str]:
    """Ensures the download status is 'pending' and a valid URL exists.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to validate.
    db_path : str
        Path to the SQLite database.

    Returns
    -------
    tuple[str, str]
        The download status and URL.

    Raises
    ------
    ValueError
        The MOUS ID is not found in the database.
    ValueError
        The download status is not 'pending'.
    ValueError
        No download URL is found for the MOUS ID.
    """
    log = get_run_logger()

    # gather the mous_id record
    with get_db_connection(db_path) as conn:
        row = get_pipeline_state_record(conn, mous_id)

    if not row:
        raise ValueError(f"MOUS ID {mous_id} not found")

    download_status = row["download_status"]

    if download_status != "pending":
        log.warning(
            f"[{mous_id}] Status is '{download_status}', not 'pending'. Skipping download."
        )
        raise ValueError(
            f"MOUS {mous_id} has status '{download_status}', expected 'pending'"
        )

    log.info(f"[{mous_id}] download_status status validated as 'pending'.")

    # Get URL
    url = row["download_url"]

    if not url:
        raise ValueError(f"No download URL for {mous_id}")

    log.info(f"[{mous_id}] Found download URL: {url}")

    return download_status, url


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Download MOUS")
def download_mous_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    download_dir: Optional[str] = None,
    weblog_dir: Optional[str] = None,
):
    """Prefect flow to download a MOUS dataset.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to download.
    db_path : Optional[str], optional
        Path to the SQLite database, by default None (will use default from config).
    download_dir : Optional[str], optional
        Directory to download the dataset to, by default None (will use default from config).
    weblog_dir : Optional[str], optional
        Directory for weblog files, by default None (will use default from config).
    """
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    download_dir = download_dir or DATASETS_DIR
    log.info(f"[{mous_id}] download_dir set as: {download_dir}")
    weblog_dir = weblog_dir or SRDP_WEBLOG_DIR
    log.info(f"[{mous_id}] weblog_dir set as: {weblog_dir}")

    # validation steps
    log.info(f"[{mous_id}] Validating mous pipeline_state status...")
    download_status, url = validate_mous_download_status(mous_id, db_path)

    # updating database to in_progress
    log.info(f"[{mous_id}] Updating database status to in_progress")
    with get_db_connection(db_path) as conn:
        update_pipeline_state_record(conn, mous_id, download_status="in_progress")

    # submit job to headless session
    try:
        # calling task to launch job
        log.info(f"[{mous_id}] Calling task to launch download job...")
        job_id, tmpdir = launch_download_job_task(mous_id, url, download_dir, db_path)

        # # monitor the headless session
        # log.info(f'[{mous_id}] Monitoring headless job...')
        # status_flag = monitor_download_job(job_id, mous_id)

        # # if status is completed, then organize the files
        # if status_flag:
        #     log.info(f'[{mous_id}] Download status marked as completed...')
        #     log.info(f'[{mous_id}] Organizing downloaded files...')
        #     calibrated_products, _, _ = organize_downloaded_files(mous_id, tmpdir, download_dir, weblog_dir)

        #     # updating database with mous_directory and calibrated products
        #     with get_db_connection(db_path) as conn:
        #         # grab the mous_directory
        #         mous_directory_path = to_dir_mous_id(mous_id)
        #         set_mous_directory(conn, mous_id, mous_directory_path)
        #         set_calibrated_products(conn, mous_id, calibrated_products)

        #     log.info(f'[{mous_id}] Updating database status to completed')
        #     with get_db_connection(db_path) as conn:
        #         set_download_status_complete(conn, mous_id, timestamp=True)

        #     # removing temporary directory after files have been moved
        #     log.info(f'[{mous_id}] Removing temporary directory...')
        # shutil.rmtree(tmpdir)

    except Exception as e:
        log.info(f"[{mous_id}] Updating database status to error")
        with get_db_connection(db_path) as conn:
            update_pipeline_state_record(conn, mous_id, download_status="error")

        log.error(f"[{mous_id}] Download failed: {e}")
        raise
