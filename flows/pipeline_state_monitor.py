"""
pipeline_state_monitor.py
--------------------
Script to monitor the central metadata database table: pipeline_state
and pass the information to the initiator script which will launch
appropriate flows to complete data pipeline steps.

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
import asyncio
import pandas as pd
from typing import Optional

from alma_ops.config import DB_PATH  # noqa: E402
from alma_ops.db import get_db_connection, db_fetch_all

from prefect import flow, get_run_logger, task  # noqa: E402
from prefect.deployments import run_deployment

# =====================================================================
# Prefect Tasks
# =====================================================================

@task(name="Download Trigger")
def trigger_download(mous_id: str):
    log = get_run_logger()
    flow_run = run_deployment(
        name="Download MOUS/mous-downloader",
        parameters={"mous_id": mous_id}
    )
    log.info(f"Triggered download for {mous_id}, flow_run_id: {flow_run.id}")
    return flow_run.id

# =====================================================================
# Prefect Flows
# =====================================================================

@flow(name="Database Monitor")
def database_monitor_flow(
    db_path: Optional[str] = None
):
    log = get_run_logger()

    # parsing input parameters
    log.info('Parsing input variables...')
    db_path = db_path or DB_PATH
    log.info(f'db_path set as: {db_path}')

    log.info(f"🚀 Starting database parsing...")

    # grab the entire pipeline_state table contents
    with get_db_connection(db_path) as conn:
        df = pd.read_sql("SELECT * FROM pipeline_state", conn)

    # log.info(f'Loaded Database; printing head...')
    # log.info(df.head())

    # downloads
    # ----------------
    for _, row in df.iterrows():
        # check for pending download status
        if row['download_status'] == 'pending' and isinstance(row.get('download_url'), str):
            mous_id = row['mous_id']
            trigger_download.submit(mous_id)  # trigger asynchronously
            log.info(f'Queued download trigger for {mous_id}')
        else:
            pass