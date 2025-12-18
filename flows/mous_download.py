"""
mous_download.py
--------------------
Script for downloading ALMA MOUS datasets from a pre-configured NRAO SRDP url.

Most tasks are executed from the worker running on the vm, but the main
downloading command will happen within a headless session to prevent
platform writes across a mounted sshfs connection.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory
"""

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------
import os  # noqa: E402
import shutil  # noqa: E402
import tempfile  # noqa: E402
import time  # noqa: E402
from datetime import datetime  # noqa: E402
from typing import Optional  # noqa: E402

from alma_ops.config import (  # noqa: E402
    DATASETS_DIR,
    DB_PATH,
    PROJECT_ROOT,
    SRDP_WEBLOG_DIR,
    WGET2_IMAGE,
    to_platform_path,
)
from alma_ops.db import (  # noqa: E402
    get_db_connection,
    get_pipeline_state_download_url,
    set_calibrated_products,
    set_download_status_complete,
    set_download_status_error,
    set_download_status_in_progress,
    set_mous_directory,
)
from alma_ops.downloads.organize import organize_downloaded_files  # noqa: E402
from alma_ops.utils import to_dir_mous_id  # noqa: E402
from canfar.sessions import Session  # noqa: E402
from prefect import flow, get_run_logger, task  # noqa: E402

# =====================================================================
# Prefect Tasks
# =====================================================================

@task(name="Launch Headless Download Session", retries=2, retry_delay_seconds=60)
def launch_download_headless_session(job_name: str, img: str, cmd: str, tmpdir: str, url: str):
    log = get_run_logger()

    # initialize the session
    session = Session()

    job_id = session.create(
        name=job_name,
        image=img,
        cmd=cmd,
        args=f"{tmpdir} {url}",
    )

    if not job_id:
        log.error("Unsuccessful job launch.")
        raise Exception("Unsuccessful Job Launch.")

    return job_id


@task(name="Launch Download Job")
def launch_download_job_task(mous_id: str, url: str, download_dir: str):
    """Submit wget2 download command as a headless session on platform."""
    log = get_run_logger()

    # preparing download command
    tmpdir = tempfile.mkdtemp(prefix=f"{to_dir_mous_id(mous_id)}_", dir=download_dir)

    # creating temporary directory to host data download
    log.info(f'[{mous_id}] Creating temporary directory: {tmpdir}')
    os.makedirs(tmpdir, exist_ok=True)

    job_name = f"wget2-{datetime.now().strftime('%Y%m%d')}"

    # for headless sessions: use platform-native paths
    log.info(f'[{mous_id}] Setting platform-native paths for headless session launch')
    platform_tmpdir = to_platform_path(tmpdir)
    log.info(f'[{mous_id}] Platform tmpdir location set as: {platform_tmpdir}')
    run_download_script_filepath = to_platform_path(str(PROJECT_ROOT / "alma-sails-codebase" / "alma_ops" / "downloads" / "run_download.sh"))
    log.info(f'[{mous_id}] Platform download script location set as: {run_download_script_filepath}')

    # call task to launch headless session
    job_id = launch_download_headless_session(job_name, WGET2_IMAGE, run_download_script_filepath, platform_tmpdir, url)

    log.info(f"[{mous_id}] Launched download job: {job_id[0]}")
    return job_id[0], tmpdir

@task(name="Monitor Download Job")
def monitor_download_job(job_id: str, mous_id: str):
    """Monitor the headless session for its end state."""
    log = get_run_logger()

    # initialize the session
    session = Session()

    # gather session information
    session_info = session.info(ids=job_id)
    log.info(f"Status: {session_info[0]['status']}")

    # waiting for completion
    while True:
        status = session.info(ids=job_id)[0]['status']
        if status in ["Succeeded", "Failed", "Terminated"]:
            log.info(f'Job completed with status: {status}')
            break
        time.sleep(60)

    return status


@task(name="Validate MOUS Status")
def validate_mous_status(mous_id: str, db_path: str) -> tuple[str, str]:
    """Check if MOUS is in 'pending' state and has a valid download URL."""
    log = get_run_logger()
    
    with get_db_connection(db_path) as conn:
        # Check download status
        cursor = conn.execute(
            "SELECT download_status FROM pipeline_state WHERE mous_id = ?",
            (mous_id,)
        )
        row = cursor.fetchone()
        
        if not row:
            log.error(f"[{mous_id}] MOUS ID not found in database.")
            raise ValueError(f"MOUS ID {mous_id} not found")
        
        status = row[0]
        
        if status != 'pending':
            log.warning(f"[{mous_id}] Status is '{status}', not 'pending'. Skipping download.")
            raise ValueError(f"MOUS {mous_id} has status '{status}', expected 'pending'")
        
        # Get URL
        url = get_pipeline_state_download_url(conn, mous_id)
        
        if not url:
            log.error(f"[{mous_id}] No download URL found in database.")
            raise ValueError(f"No download URL for {mous_id}")
    
    log.info(f"[{mous_id}] Status is 'pending' with valid URL: {url}")
    return status, url

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
    """Download a single MOUS."""
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f'[{mous_id}] db_path set as: {db_path}')
    download_dir = download_dir or DATASETS_DIR
    log.info(f'[{mous_id}] download_dir set as: {download_dir}')
    weblog_dir = weblog_dir or SRDP_WEBLOG_DIR
    log.info(f'[{mous_id}] weblog_dir set as: {weblog_dir}')
    
    # Validate status is 'pending' and URL exists
    log.info(f'[{mous_id}] Validating mous pipeline_state status...')
    status, url = validate_mous_status(mous_id, db_path)

    # setting database download status
    log.info(f'[{mous_id}] Updating database status to in_progress')
    with get_db_connection(db_path) as conn:
        set_download_status_in_progress(conn, mous_id, timestamp=True)

    # execution of main steps
    try:
        # calling task to launch job
        log.info(f'[{mous_id}] Calling task to launch download job...')
        job_id, tmpdir = launch_download_job_task(mous_id, url, download_dir)    

        # monitor the headless session
        log.info(f'[{mous_id}] Monitoring headless job...')
        status = monitor_download_job(job_id, mous_id)

        # if status is completed, then organize the files
        if status == "Completed":
            log.info(f'[{mous_id}] Download status marked as completed...')
            log.info(f'[{mous_id}] Organizing downloaded files...')
            calibrated_products, _, _ = organize_downloaded_files(mous_id, tmpdir, download_dir, weblog_dir)

            # updating database with mous_directory and calibrated products
            with get_db_connection(db_path) as conn:
                # grab the mous_directory
                mous_directory_path = to_dir_mous_id(mous_id)
                set_mous_directory(conn, mous_id, mous_directory_path)
                set_calibrated_products(conn, mous_id, calibrated_products)

            log.info(f'[{mous_id}] Updating database status to completed')
            with get_db_connection(db_path) as conn:
                set_download_status_complete(conn, mous_id, timestamp=True)

            # removing temporary directory after files have been moved
            log.info(f'[{mous_id}] Removing temporary directory...')
            # shutil.rmtree(tmpdir)


    except Exception as e:
        log.info(f'[{mous_id}] Updating database status to completed')
        with get_db_connection(db_path) as conn:
            set_download_status_error(conn, mous_id)

        log.error(f'[{mous_id}] Download failed: {e}')
        raise
    
    log.info(f"✅ Completed download: {mous_id}")
