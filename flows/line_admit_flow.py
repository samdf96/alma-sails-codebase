"""
line_admit_flow.py
--------------------
Prefect flow for ADMIT line identification on one completed line-triage
unit.

Launches a CANFAR headless session running scripts/headless_admit_lineid.sh
inside the CASA pipeline container. The headless session runs
manual_admit_lineid.py, which runs ADMIT's line-ID flow on the unit's
triage cube, writes identified lines to line_admit_detection, and advances
the line_admit_unit to 'complete' (or 'failed').

Unlike line_triage_flow.py, the line_admit_unit row DOES already exist (or
is trivially created) before this flow runs, since it keys 1:1 off an
existing line_triage_unit rather than being discovered dynamically from
cont.dat -- so this flow's pre-flight guard checks the parent
line_triage_unit is 'complete' and that no line_admit_unit for it is
already 'running'.

Defaults to flexible mode (no cores/RAM pinned) -- ADMIT has no MPI/parallel
awareness anywhere in its codebase (checked: no mpicasa/casampi/MPIEnvironment/
mpi4casa references), so extra cores would never be used. Pass ram_gb to pin
a larger RAM reservation only (never cores), same rationale as
line_triage_flow.py.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/line_admit_flow.py --mous-id uid://A001/X.../X... --source EC_53 --spw 3
    python alma-sails-codebase/flows/line_admit_flow.py --mous-id uid://A001/X.../X... --source EC_53 --spw 3 --ram-gb 32
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, get_run_logger, task

from alma_ops import db
from alma_ops.config import CASA_IMAGE, DB_PATH
from alma_ops.db import CANONICAL_PREFIX
from canfar_utils import create_checked, key_admit, make_session, report_launch

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_admit_lineid.sh"


@task(name="validate-admit-ready")
def validate_admit_ready(mous_id: str, source: str, spw: int, db_path: str) -> None:
    """Verify the parent line_triage_unit is 'complete' and no line_admit_unit
    for it is already 'running'."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)

    triage_row = db.fetch_one(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? AND spw_idx=?",
        (mous_id, source, spw),
    )
    if triage_row is None:
        conn.close()
        raise ValueError(f"No line_triage_unit for {mous_id} {source} spw={spw}")

    state = triage_row["current_state"]
    if state != "complete":
        conn.close()
        raise ValueError(f"Expected line_triage_unit state 'complete', got '{state}' for "
                          f"{mous_id} {source} spw={spw} — aborting")

    admit_row = db.get_line_admit_unit_for_triage_unit(conn, triage_row["line_triage_unit_id"])
    conn.close()

    if admit_row is not None and admit_row["current_state"] == "running":
        raise ValueError(f"ADMIT already running for {mous_id} {source} spw={spw} — aborting")

    logger.info(f"[{mous_id} {source} spw={spw}] Confirmed triage complete, no ADMIT run in flight")


@task(name="launch-admit-lineid-session", retries=2, retry_delay_seconds=60)
def launch_admit_lineid_session(
    mous_id: str,
    source: str,
    spw: int,
    db_path: str,
    force: bool,
    ram_gb: Optional[int],
) -> str:
    """Submit a CASA pipeline headless session to run ADMIT line identification.

    ram_gb=None (default) launches flexible: no cores/RAM pinned. ram_gb set
    to an int pins that much RAM only -- never cores, since ADMIT has no
    MPI/parallel awareness at all. Purely a CANFAR resource reservation.
    """
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    unit_key = key_admit(mous_id, source, spw)

    args = f"{mous_id} {source} {spw} {canonical_db_path} {'1' if force else ''}"

    kwargs = dict(image=CASA_IMAGE, cmd=_HEADLESS_SCRIPT, args=args)
    if ram_gb is not None:
        kwargs["ram"] = ram_gb

    session = make_session()
    result = create_checked(session, unit_key, **kwargs)

    mode_desc = "flexible mode" if ram_gb is None else f"{ram_gb} GB RAM pinned"
    return report_launch(logger, f"[{mous_id} {source} spw={spw}]", f"ADMIT ({mode_desc})", result)


@flow(name="admit-lineid", log_prints=True)
def admit_lineid_flow(
    mous_id: str,
    source: str,
    spw: int,
    force: bool = False,
    ram_gb: Optional[int] = None,
    db_path: Optional[str] = None,
) -> None:
    """Run ADMIT line identification for one line-triage unit via a CANFAR headless session.

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    source:
        Target/field name within this MOUS.
    spw:
        spw index of the line_triage_unit to run.
    force:
        Re-run even if the line_admit_unit is already 'complete'.
    ram_gb:
        RAM in GB to pin on the headless session. None (default) is flexible
        mode -- no cores/RAM pinned. Never pins cores; ADMIT has no
        MPI/parallel awareness.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    """
    db_path = db_path or str(DB_PATH)

    validate_admit_ready(mous_id, source, spw, db_path)
    launch_admit_lineid_session(mous_id, source, spw, db_path, force, ram_gb)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: run ADMIT line identification for one line-triage unit."
    )
    parser.add_argument("--mous-id",  required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--source",   required=True, help="Target/field name within this MOUS")
    parser.add_argument("--spw",      type=int, required=True, help="spw index of the line_triage_unit")
    parser.add_argument("--force",    action="store_true",    help="Re-run even if already complete")
    parser.add_argument("--ram-gb",   type=int, default=None,
                        help="Pin this much RAM on the session (default: flexible, no cores/RAM pinned)")
    parser.add_argument("--db-path",  default=None,            help="Path to the SQLite database")
    args = parser.parse_args()

    admit_lineid_flow(
        mous_id=args.mous_id,
        source=args.source,
        spw=args.spw,
        force=args.force,
        ram_gb=args.ram_gb,
        db_path=args.db_path,
    )
