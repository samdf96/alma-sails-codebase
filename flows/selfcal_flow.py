"""
selfcal_flow.py
---------------
Prefect flow for running autoselfcal on a split MOUS.

Marks the MOUS as 'selfcal_running' then launches a CANFAR headless session
running scripts/manual_selfcal.sh inside the CASA pipeline container. The
headless session handles mpicasa (or plain casa -c in flexible mode),
post-selfcal extraction, and advances the MOUS to 'selfcal_complete'.

The MOUS stays in 'selfcal_running' while the headless session runs.
If the session launch itself fails, the MOUS is transitioned to 'failed'.

Defaults to fixed mode (ncores/ram_gb pinned, mpicasa) unlike the other
flows' flexible-by-default -- auto_selfcal genuinely benefits from MPI
parallelism (unlike e.g. line-triage imaging or ADMIT, where extra cores
would sit unused), so the proven default is kept. Pass ncores=0 (or None
from Python) for flexible mode: no cores/RAM pinned, manual_selfcal.sh runs
plain casa -c and auto_selfcal's own MPIEnvironment check correctly falls
back to parallel=False.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/selfcal_flow.py --mous-id uid://A001/X.../X... --ncores 4
    python alma-sails-codebase/flows/selfcal_flow.py --mous-id uid://A001/X.../X... --ncores 0  # flexible
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

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/manual_selfcal.sh"

_DEFAULT_NCORES = 4
_DEFAULT_RAM_GB  = 16


@task(name="validate-split-complete-state")
def validate_split_complete(mous_id: str, db_path: str) -> None:
    """Verify the MOUS is in 'split_complete' state before launching selfcal."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)
    row = db.get_pipeline_state(conn, mous_id)
    conn.close()

    if row is None:
        raise ValueError(f"MOUS {mous_id} not found in database")

    state = row["current_state"]
    if state != "split_complete":
        raise ValueError(f"Expected state 'split_complete', got '{state}' for {mous_id} — aborting")

    logger.info(f"[{mous_id}] Confirmed split_complete")


@task(name="mark-selfcal-running")
def mark_selfcal_running(mous_id: str, db_path: str) -> None:
    """Advance the MOUS to 'selfcal_running' before submitting the headless session.

    This is written before the session launches so that if the Prefect process
    dies after submission, the 'selfcal_running' state signals that a job is
    already out there and prevents double-submission.
    """
    conn = db.get_connection(db_path)
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state="split_complete",
        to_state="selfcal_running",
        step="selfcal",
    )
    conn.close()


@task(name="launch-selfcal-session", retries=2, retry_delay_seconds=60)
def launch_selfcal_session(
    mous_id: str, db_path: str, ncores: Optional[int], ram_gb: Optional[int], use_staging: bool,
) -> str:
    """Submit a CASA pipeline headless session to run autoselfcal.

    ncores=None launches in flexible mode: no cores/RAM pinned on the
    session, and manual_selfcal.sh (told via --ncores 0) runs plain casa -c
    instead of mpicasa -- auto_selfcal's own MPIEnvironment check then
    correctly self-selects parallel=False. ncores set to an int launches in
    fixed mode: cores/RAM pinned via session.create(), and manual_selfcal.sh
    runs mpicasa -n ncores (auto_selfcal detects MPI and sets parallel=True).
    """
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    job_name = f"selfcal-{datetime.now().strftime('%Y%m%d-%H%M')}"
    args = f"--mous-id {mous_id} --db-path {canonical_db_path} --ncores {ncores if ncores is not None else 0}"

    session = make_session(use_staging)
    if ncores is None:
        job_id = session.create(name=job_name, image=CASA_IMAGE, cmd=_HEADLESS_SCRIPT, args=args)
    else:
        job_id = session.create(
            name=job_name, image=CASA_IMAGE, cores=ncores, ram=ram_gb, cmd=_HEADLESS_SCRIPT, args=args,
        )

    if not job_id:
        raise RuntimeError(f"[{mous_id}] CANFAR session launch returned no job ID")

    mode_desc = "flexible mode" if ncores is None else f"{ncores} cores, {ram_gb} GB RAM"
    logger.info(f"[{mous_id}] Headless selfcal session launched: {job_id[0]} ({mode_desc})")
    return job_id[0]


@flow(name="selfcal-mous", log_prints=True)
def selfcal_mous_flow(
    mous_id: str,
    ncores: Optional[int] = _DEFAULT_NCORES,
    ram_gb: Optional[int] = _DEFAULT_RAM_GB,
    use_staging: bool = False,
    db_path: Optional[str] = None,
) -> None:
    """Run autoselfcal on a split MOUS via a CANFAR headless session.

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    ncores:
        Number of MPI cores to allocate (default 4, matching auto_selfcal's
        genuine benefit from MPI parallelism). Pass 0 or None for flexible
        mode: no cores/RAM pinned, plain casa -c instead of mpicasa.
    ram_gb:
        RAM in GB to allocate to the headless session (default 16). Ignored
        in flexible mode.
    use_staging:
        If True, submit the headless session to the CANFAR staging cluster.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    """
    logger = get_run_logger()
    db_path = db_path or str(DB_PATH)
    if ncores == 0:
        ncores = None

    validate_split_complete(mous_id, db_path)
    mark_selfcal_running(mous_id, db_path)

    try:
        launch_selfcal_session(mous_id, db_path, ncores, ram_gb, use_staging)
    except Exception as e:
        logger.error(f"[{mous_id}] Session launch failed — recording failure: {e}")
        conn = db.get_connection(db_path)
        db.record_failure(
            conn,
            mous_id=mous_id,
            from_state="selfcal_running",
            error=str(e),
            step="selfcal",
        )
        conn.close()
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: run autoselfcal on a split MOUS."
    )
    parser.add_argument("--mous-id",     required=True,                       help="MOUS ID (uid://A001/...)")
    parser.add_argument("--ncores",      type=int, default=_DEFAULT_NCORES,   help=f"MPI cores (default {_DEFAULT_NCORES}); pass 0 for flexible mode (no cores/RAM pinned, no mpicasa)")
    parser.add_argument("--ram-gb",      type=int, default=_DEFAULT_RAM_GB,   help=f"RAM in GB (default {_DEFAULT_RAM_GB})")
    parser.add_argument("--db-path",     default=None,                        help="Path to the SQLite database")
    parser.add_argument("--use-staging", action="store_true",                  help="Submit to CANFAR staging cluster")
    args = parser.parse_args()

    selfcal_mous_flow(
        mous_id=args.mous_id,
        ncores=args.ncores,
        ram_gb=args.ram_gb,
        db_path=args.db_path,
        use_staging=args.use_staging,
    )
