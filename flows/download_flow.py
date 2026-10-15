"""
download_flow.py
----------------
Prefect flow for downloading a MOUS from the NRAO SRDP service.

Launches a CANFAR headless session running scripts/headless_download.sh
inside the c-astroml container. The headless session runs manual_download.py,
which handles the wget2 download and advances the MOUS to 'downloaded'.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/download_flow.py --mous-id uid://A001/X.../X... --srdp-url https://...
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, get_run_logger, task

from alma_ops import db
from alma_ops.config import ASTROML_IMAGE, DB_PATH
from alma_ops.db import CANONICAL_PREFIX
from canfar_utils import create_checked, key_download, make_session, report_launch

# Canonical path to the headless entry script
_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_download.sh"


@task(name="validate-pending-state")
def validate_pending_state(mous_id: str, db_path: str) -> None:
    """Verify the MOUS is in 'pending' state before launching the download session."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)
    row = db.get_pipeline_state(conn, mous_id)
    conn.close()

    if row is None:
        raise ValueError(f"MOUS {mous_id} not found in database")

    state = row["current_state"]
    if state != "pending":
        raise ValueError(f"Expected state 'pending', got '{state}' for {mous_id} — aborting")

    logger.info(f"[{mous_id}] Confirmed pending")


@task(name="store-download-url")
def store_download_url(mous_id: str, srdp_url: str, db_path: str) -> None:
    """Persist the SRDP URL to the DB before the session launches."""
    conn = db.get_connection(db_path)
    db.update_pipeline_paths(conn, mous_id, download_url=srdp_url)
    conn.close()


@task(name="launch-download-session", retries=2, retry_delay_seconds=60)
def launch_download_session(mous_id: str, srdp_url: str, db_path: str) -> str:
    """Submit a c-astroml headless session to run the download."""
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    unit_key = key_download(mous_id)

    session = make_session()
    result = create_checked(
        session,
        unit_key,
        image=ASTROML_IMAGE,
        cmd=_HEADLESS_SCRIPT,
        args=f"{mous_id} {srdp_url} {canonical_db_path}",
    )

    return report_launch(logger, f"[{mous_id}]", "download", result)


@flow(name="download-mous", log_prints=True)
def download_mous_flow(
    mous_id: str,
    srdp_url: str,
    db_path: Optional[str] = None,
) -> None:
    """Download a single MOUS from the NRAO SRDP service via a CANFAR headless session.

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    srdp_url:
        Direct URL to the SRDP data product directory.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    """
    db_path = db_path or str(DB_PATH)

    validate_pending_state(mous_id, db_path)
    store_download_url(mous_id, srdp_url, db_path)
    launch_download_session(mous_id, srdp_url, db_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: download a MOUS from the NRAO SRDP service."
    )
    parser.add_argument("--mous-id",  required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--srdp-url", required=True, help="Direct SRDP data product URL")
    parser.add_argument("--db-path",  default=None,  help="Path to the SQLite database")
    args = parser.parse_args()

    download_mous_flow(
        mous_id=args.mous_id,
        srdp_url=args.srdp_url,
        db_path=args.db_path,
    )
