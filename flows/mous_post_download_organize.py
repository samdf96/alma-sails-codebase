"""
mous_post_download_organize.py
--------------------
Script for organizing downloaded files to temporary directories.

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
from typing import Optional  # noqa: E402

from alma_ops.config import (  # noqa: E402
    DATASETS_DIR,
    DB_PATH,
    SRDP_WEBLOG_DIR,
    to_platform_path,
    to_vm_path,
)
from alma_ops.db import (  # noqa: E402
    get_db_connection,
    get_pipeline_state_mous_directory,
    set_calibrated_products,
    set_download_status_complete,
    set_mous_directory,
)
from alma_ops.downloads.organize import organize_downloaded_files  # noqa: E402
from alma_ops.utils import to_dir_mous_id  # noqa: E402
from prefect import flow, get_run_logger, task  # noqa: E402

# =====================================================================
# Prefect Tasks
# =====================================================================

@task(name="Validate MOUS Downloaded Status")
def validate_mous_status(mous_id: str, db_path: str):
    """Check if MOUS download status is in 'downloaded' state."""
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
        
        if status != 'downloaded':
            log.warning(f"[{mous_id}] Status is '{status}', not 'downloaded'. Skipping organizing...")
            raise ValueError(f"MOUS {mous_id} has status '{status}', expected 'downloaded'")

        # get tmpdir location stored in the mous_directory column
        tmpdir = get_pipeline_state_mous_directory(conn, mous_id)
    
    log.info(f"[{mous_id}] Status is 'downloaded'.")
    log.info(f"[{mous_id}] Download tmpdir found to be: {tmpdir}.")
    return tmpdir

# =====================================================================
# Prefect Flows
# =====================================================================

@flow(name="Post Download Organize")
def post_download_organize(
    mous_id: str,
    db_path: Optional[str] = None,
    datasets_dir: Optional[str] = None,
    weblog_dir: Optional[str] = None,
):
    """Organize downloaded files to final locations."""
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f'[{mous_id}] db_path set as: {db_path}')
    datasets_dir = datasets_dir or DATASETS_DIR
    log.info(f'[{mous_id}] datasets_dir set as: {datasets_dir}')
    weblog_dir = weblog_dir or SRDP_WEBLOG_DIR
    log.info(f'[{mous_id}] weblog_dir set as: {weblog_dir}')

    # validate status is 'downloaded'
    log.info(f'[{mous_id}] Validating mous pipeline_state status...')
    tmpdir = validate_mous_status(mous_id, db_path)

    # change tmpdir to be vm-style so the organize files can work
    tmpdir = to_vm_path(tmpdir)

    # organizing downloaded files
    calibrated_products, _, _, mous_directory = organize_downloaded_files(mous_id, tmpdir, datasets_dir, weblog_dir, logger=log)

    # updating database with mous_directory and calibrated_products
    with get_db_connection(db_path) as conn:
        # convert mous_directory to platform path for database storage
        platform_mous_directory = to_platform_path(mous_directory)
        set_mous_directory(conn, mous_id, platform_mous_directory)
        
        # convert calibrated_products to platform paths for database storage
        platform_calibrated_products = to_platform_path(calibrated_products)
        set_calibrated_products(conn, mous_id, platform_calibrated_products)

    log.info(f'[{mous_id}] Updating database status to completed')
    with get_db_connection(db_path) as conn:
        set_download_status_complete(conn, mous_id, timestamp=True)

    # finally removing temporary directory
    log.info(f'[{mous_id}] Removing temporary directory...')
    shutil.rmtree(tmpdir)

    log.info(f"✅ Post Download Organize Completed: {mous_id}")