"""
split_flow.py
-------------
Prefect flow for splitting calibrated MOUS measurement sets to target-only science SPWs.

Launches a CANFAR headless session running scripts/headless_split.sh inside the
CASA pipeline container. The headless session runs manual_split.py (via casa -c),
which handles the split, listobs, and advances the MOUS to 'split_complete'.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/split_flow.py --mous-id uid://A001/X.../X...
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, get_run_logger, task

from alma_ops import db
from alma_ops.config import CASA_IMAGE, DB_PATH
from alma_ops.db import CANONICAL_PREFIX
from canfar_utils import make_session

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_split.sh"


@task(name="validate-downloaded-state")
def validate_downloaded_state(mous_id: str, db_path: str) -> None:
    """Verify the MOUS is in 'downloaded' state before launching the split session."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)
    row = db.get_pipeline_state(conn, mous_id)
    conn.close()

    if row is None:
        raise ValueError(f"MOUS {mous_id} not found in database")

    state = row["current_state"]
    if state != "downloaded":
        raise ValueError(f"Expected state 'downloaded', got '{state}' for {mous_id} — aborting")

    logger.info(f"[{mous_id}] Confirmed downloaded")


@task(name="launch-split-session", retries=2, retry_delay_seconds=60)
def launch_split_session(
    mous_id: str,
    db_path: str,
    use_staging: bool,
    spw_override: Optional[str] = None,
) -> str:
    """Submit a CASA pipeline headless session to run the split."""
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    job_name = f"split-{datetime.now().strftime('%Y%m%d-%H%M')}"

    args = f"{mous_id} {canonical_db_path}"
    if spw_override:
        args = f"{args} {spw_override}"

    session = make_session(use_staging)
    job_id = session.create(
        name=job_name,
        image=CASA_IMAGE,
        cmd=_HEADLESS_SCRIPT,
        args=args,
    )

    if not job_id:
        raise RuntimeError(f"[{mous_id}] CANFAR session launch returned no job ID")

    logger.info(f"[{mous_id}] Headless split session launched: {job_id[0]}"
                + (f" (SPW override: {spw_override})" if spw_override else ""))
    return job_id[0]


@flow(name="split-mous", log_prints=True)
def split_mous_flow(
    mous_id: str,
    use_staging: bool = False,
    db_path: Optional[str] = None,
    spw_override: Optional[str] = None,
) -> None:
    """Split a downloaded MOUS into target-only science SPW measurement sets.

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    use_staging:
        If True, submit the headless session to the CANFAR staging cluster.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    spw_override:
        Comma-separated SPW IDs to use instead of the alminer_metadata lookup,
        e.g. ``"21,23,25,27,29"``. Use when alminer reports wrong SPW indices
        for a given MOUS.
    """
    db_path = db_path or str(DB_PATH)

    validate_downloaded_state(mous_id, db_path)
    launch_split_session(mous_id, db_path, use_staging, spw_override)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: split a downloaded MOUS into target science SPW MSes."
    )
    parser.add_argument("--mous-id",     required=True,       help="MOUS ID (uid://A001/...)")
    parser.add_argument("--db-path",     default=None,        help="Path to the SQLite database")
    parser.add_argument("--use-staging", action="store_true", help="Submit to CANFAR staging cluster")
    parser.add_argument("--spw",         default=None,        help="Comma-separated SPW ID override, e.g. '21,23,25,27,29'")
    args = parser.parse_args()

    split_mous_flow(
        mous_id=args.mous_id,
        db_path=args.db_path,
        use_staging=args.use_staging,
        spw_override=args.spw,
    )
