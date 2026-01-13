"""
pipeline_state_monitor.py
--------------------
Script to monitor the central metadata database table: `pipeline_state`
and spin up downstream flows as needed based on the current state of each MOUS,
in addition to the number of currently in_progress flows.

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
from alma_ops.config import DB_PATH
from alma_ops.db import get_db_connection
from prefect import flow, get_run_logger, task
from prefect.deployments import run_deployment

# =====================================================================
# Configuration Settings
# =====================================================================
MAX_CONCURRENT_DOWNLOAD_FLOWS = 1
MAX_CONCURRENT_POST_DOWNLOAD_ORGANIZE_FLOWS = 10
MAX_CONCURRENT_SPLIT_FLOWS = 5
MAX_CONCURRENT_POST_SPLIT_ORGANIZE_FLOWS = 10
MAX_CONCURRENT_POST_SPLIT_LISTOBS_FLOWS = 10
MAX_CONCURRENT_AUTOSELFCAL_PREP_FLOWS = 2
MAX_CONCURRENT_AUTOSELFCAL_FLOWS = 1
MAX_CONCURRENT_POST_AUTOSELFCAL_ORGANIZE_FLOWS = 1

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Download Trigger")
def trigger_download(mous_id: str):
    """Triggers the mous_downloader flow for the given MOUS ID."""
    log = get_run_logger()
    flow_run = run_deployment(
        name="Download MOUS/mous-download", parameters={"mous_id": mous_id}
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


@task(name="Post-Split Listobs Trigger")
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

    log.info("✅ Completed database parsing.")

    # ===============
    # DOWNLOADS
    # ===============

    # first gather number of in_progress download flows
    pending_download_flows = df[df["download_status"] == "pending"].shape[0]
    in_progress_download_flows = df[df["download_status"] == "in_progress"].shape[0]
    downloaded_download_flows = df[df["download_status"] == "downloaded"].shape[0]
    completed_download_flows = df[df["download_status"] == "complete"].shape[0]

    log.info("------------------------------------------")
    log.info(f"""Current Download Status:
---------------
Pending: {pending_download_flows}
In Progress: {in_progress_download_flows}
Downloaded: {downloaded_download_flows}
Completed: {completed_download_flows}""")

    if in_progress_download_flows >= MAX_CONCURRENT_DOWNLOAD_FLOWS:
        log.info(
            f"Maximum number of concurrent download flows reached ({in_progress_download_flows}/{MAX_CONCURRENT_DOWNLOAD_FLOWS}). Skipping download triggers."
        )
    else:
        if in_progress_download_flows == 0:
            log.info(
                "No download flows are currently in progress. Proceeding to queue new download triggers if url found."
            )
        else:
            log.info(
                f"Only {in_progress_download_flows} download flows are currently in progress. Proceeding to queue new download triggers if url found."
            )

        launched_download = False
        for _, row in df.iterrows():
            # check for pending download status
            if row["download_status"] == "pending" and isinstance(
                row.get("download_url"), str
            ):
                mous_id = row["mous_id"]
                trigger_download.submit(mous_id)  # trigger asynchronously
                log.info(f"Queued download trigger for {mous_id}")
                launched_download = True

        if not launched_download:
            log.info("No pending downloads with valid download URL found to trigger.")

    if downloaded_download_flows >= MAX_CONCURRENT_POST_DOWNLOAD_ORGANIZE_FLOWS:
        log.info(
            f"Maximum number of concurrent post-download organize flows reached ({downloaded_download_flows}/{MAX_CONCURRENT_POST_DOWNLOAD_ORGANIZE_FLOWS}). Skipping post-download organize triggers."
        )
    else:
        if downloaded_download_flows == 0:
            log.info(
                "No post-download organize flows are currently in progress. Proceeding to queue new post-download organize triggers."
            )
        else:
            log.info(
                f"Only {downloaded_download_flows} post-download organize flows are currently in progress. Proceeding to queue new post-download organize triggers."
            )

        launched_post_download_organize = False
        for _, row in df.iterrows():
            # check for downloaded download status
            if row["download_status"] == "downloaded":
                mous_id = row["mous_id"]
                trigger_post_download_organizer.submit(
                    mous_id
                )  # trigger asynchronously
                log.info(f"Queued post-download organize trigger for {mous_id}")
                launched_post_download_organize = True

        if not launched_post_download_organize:
            log.info("No MOUS found to trigger post-download organize.")

    log.info("------------------------------------------")

    # # ===============
    # # SPLITS
    # # ===============

    # first gather number of in_progress pre_selfcal_split flows
    pending_split_flows = df[df["pre_selfcal_split_status"] == "pending"].shape[0]
    in_progress_split_flows = df[df["pre_selfcal_split_status"] == "in_progress"].shape[
        0
    ]
    split_split_flows = df[df["pre_selfcal_split_status"] == "split"].shape[0]
    completed_split_flows = df[df["pre_selfcal_split_status"] == "complete"].shape[0]

    log.info(f"""Current Split Status:
---------------
Pending: {pending_split_flows}
In Progress: {in_progress_split_flows}
Split: {split_split_flows}
Completed: {completed_split_flows}""")

    if in_progress_split_flows >= MAX_CONCURRENT_SPLIT_FLOWS:
        log.info(
            f"Maximum number of concurrent split flows reached ({in_progress_split_flows}/{MAX_CONCURRENT_SPLIT_FLOWS}). Skipping split triggers."
        )
    else:
        if in_progress_split_flows == 0:
            log.info(
                "No split flows are currently in progress. Proceeding to queue new split triggers."
            )
        else:
            log.info(
                f"Only {in_progress_split_flows} split flows are currently in progress. Proceeding to queue new split triggers."
            )

        launched_split = False
        for _, row in df.iterrows():
            # check for complete download status and pending split status
            if (
                row["download_status"] == "complete"
                and row["pre_selfcal_split_status"] == "pending"
            ):
                mous_id = row["mous_id"]
                trigger_split.submit(mous_id)  # trigger asynchronously
                log.info(f"Queued split trigger for {mous_id}")
                launched_split = True

        if not launched_split:
            log.info("No MOUS found to trigger split.")

    if split_split_flows >= MAX_CONCURRENT_POST_SPLIT_ORGANIZE_FLOWS:
        log.info(
            f"Maximum number of concurrent post-split organize flows reached ({split_split_flows}/{MAX_CONCURRENT_POST_SPLIT_ORGANIZE_FLOWS}). Skipping post-split organize triggers."
        )
    else:
        if split_split_flows == 0:
            log.info(
                "No post-split organize flows are currently in progress. Proceeding to queue new post-split organize triggers."
            )
        else:
            log.info(
                f"Only {split_split_flows} post-split organize flows are currently in progress. Proceeding to queue new post-split organize triggers."
            )

        launched_post_split_organize = False
        for _, row in df.iterrows():
            # check for 'split' split status
            if row["pre_selfcal_split_status"] == "split":
                mous_id = row["mous_id"]
                trigger_post_split_organizer.submit(mous_id)  # trigger asynchronously
                log.info(f"Queued post-split organize trigger for {mous_id}")
                launched_post_split_organize = True

        if not launched_post_split_organize:
            log.info("No MOUS found to trigger post-split organize.")

    # first gather number of in_progress listobs flows
    pending_listobs_flows = df[df["pre_selfcal_listobs_status"] == "pending"].shape[0]
    in_progress_listobs_flows = df[
        df["pre_selfcal_listobs_status"] == "in_progress"
    ].shape[0]
    completed_listobs_flows = df[df["pre_selfcal_listobs_status"] == "complete"].shape[
        0
    ]

    log.info(f"""Current Listobs Status:
---------------
Pending: {pending_listobs_flows}
In Progress: {in_progress_listobs_flows}
Completed: {completed_listobs_flows}""")

    if in_progress_listobs_flows >= MAX_CONCURRENT_POST_SPLIT_LISTOBS_FLOWS:
        log.info(
            f"Maximum number of concurrent post-split listobs flows reached ({in_progress_listobs_flows}/{MAX_CONCURRENT_POST_SPLIT_LISTOBS_FLOWS}). Skipping post-split listobs triggers."
        )
    else:
        if in_progress_listobs_flows == 0:
            log.info(
                "No post-split listobs flows are currently in progress. Proceeding to queue new post-split listobs triggers."
            )
        else:
            log.info(
                f"Only {in_progress_listobs_flows} post-split listobs flows are currently in progress. Proceeding to queue new post-split listobs triggers."
            )

        launched_post_split_listobs = False
        for _, row in df.iterrows():
            # check for complete split status and pending listobs status
            if (
                row["pre_selfcal_split_status"] == "complete"
                and row["pre_selfcal_listobs_status"] == "pending"
            ):
                mous_id = row["mous_id"]
                trigger_post_split_listobs.submit(mous_id)  # trigger asynchronously
                log.info(f"Queued post-split listobs trigger for {mous_id}")
                launched_post_split_listobs = True

        if not launched_post_split_listobs:
            log.info("No MOUS found to trigger post-split listobs.")

    log.info("------------------------------------------")

    # # ===============
    # # AUTOSELFCAL
    # # ===============
