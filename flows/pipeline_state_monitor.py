"""
pipeline_state_monitor.py
--------------------
Script to monitor the central metadata database table: `pipeline_state`
and pass the information to the initiator script which will launch
appropriate flows to complete data pipeline steps.

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
from typing import Optional

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

from alma_ops.config import DB_PATH
from alma_ops.db import get_db_connection

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Download Trigger")
def trigger_download(mous_id: str):
    """Triggers the mous_downloader flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Download MOUS/mous-downloader", parameters={"mous_id": mous_id}
    )
    log.info(f"Triggered download for {mous_id}, flow_run_id: {flow_run.id}")
    return flow_run.id


@task(name="Post-Download Organize Trigger")
def trigger_post_download_organizer(mous_id: str):
    """Triggers the post_download_organize flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Post Download Organize/post-download-organize",
        parameters={"mous_id": mous_id},
    )
    log.info(
        f"Triggered post-download organize for {mous_id}, flow_run_id: {flow_run.id}"
    )
    return flow_run.id


@task(name="Split Trigger")
def trigger_split(mous_id: str):
    """Triggers the mous_split flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Split MOUS/mous-split", parameters={"mous_id": mous_id}
    )
    log.info(f"Triggered split for {mous_id}, flow_run_id: {flow_run.id}")
    return flow_run.id


@task(name="Post-Split Organize Trigger")
def trigger_post_split_organizer(mous_id: str):
    """Triggers the post_split_organize flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Post Split Organize/post-split-organize",
        parameters={"mous_id": mous_id},
    )
    log.info(f"Triggered post-split organize for {mous_id}, flow_run_id: {flow_run.id}")
    return flow_run.id


def trigger_post_split_listobs(mous_id: str):
    """Triggers the post_split_listobs flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Post Split Listobs/post-split-listobs",
        parameters={"mous_id": mous_id},
    )
    log.info(f"Triggered post-split listobs for {mous_id}, flow_run_id: {flow_run.id}")
    return flow_run.id


# =====================================================================
# Prefect Flows
# =====================================================================


@flow(name="Database Monitor")
def database_monitor_flow(db_path: Optional[str] = None):
    log = get_run_logger()

    # parsing input parameters
    log.info("Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"db_path set as: {db_path}")

    log.info("🚀 Starting database parsing...")

    # grab the entire pipeline_state table contents
    # and save as a pandas dataframe
    with get_db_connection(db_path) as conn:
        df = pd.read_sql("SELECT * FROM pipeline_state", conn)

    # log.info(f"Loaded Database; printing head...")
    # log.info(df.head())

    # ===============
    # DOWNLOADS
    # ===============

    # mous_downloader
    # ------------------------------
    for _, row in df.iterrows():
        # check for pending download status
        if row["download_status"] == "pending" and isinstance(
            row.get("download_url"), str
        ):
            mous_id = row["mous_id"]
            trigger_download.submit(mous_id)  # trigger asynchronously
            log.info(f"Queued download trigger for {mous_id}")

    # mous_post_download_organizer
    # ------------------------------
    for _, row in df.iterrows():
        # check for downloaded download status
        if row["download_status"] == "downloaded":
            mous_id = row["mous_id"]
            trigger_post_download_organizer.submit(mous_id)  # trigger asynchronously
            log.info(f"Queued post-download organize trigger for {mous_id}")

    # ===============
    # SPLITS
    # ===============

    # mous_splitter
    # ------------------------------
    for _, row in df.iterrows():
        # check for complete download status and pending split status
        if (
            row["download_status"] == "complete"
            and row["pre_selfcal_split_status"] == "pending"
        ):
            mous_id = row["mous_id"]
            trigger_split.submit(mous_id)  # trigger asynchronously
            log.info(f"Queued split trigger for {mous_id}")

    # post_split_organize
    # ------------------------------
    for _, row in df.iterrows():
        # check for 'split' split status
        if row["pre_selfcal_split_status"] == "split":
            mous_id = row["mous_id"]
            trigger_post_split_organizer.submit(mous_id)  # trigger asynchronously
            log.info(f"Queued post-split organize trigger for {mous_id}")

    # post_split_listobs
    # ------------------------------
    for _, row in df.iterrows():
        # check for complete split status and pending listobs status
        if (
            row["pre_selfcal_split_status"] == "complete"
            and row["pre_selfcal_listobs_status"] == "pending"
        ):
            mous_id = row["mous_id"]
            trigger_post_split_listobs.submit(mous_id)  # trigger asynchronously
            log.info(f"Queued post-split listobs trigger for {mous_id}")

    # ===============
    # AUTOSELFCAL
    # ===============
