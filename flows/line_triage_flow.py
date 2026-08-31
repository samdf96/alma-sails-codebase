"""
line_triage_flow.py
--------------------
Prefect flow for ADMIT-bound line-triage cube imaging of one MOUS/source.

Launches a CANFAR headless session running scripts/headless_line_triage_imaging.sh
inside the CASA pipeline container. The headless session runs
manual_line_triage_imaging.py, which classifies spws via cont.dat, images each
line-candidate spw as a dirty channel-binned cube, and advances each
line_triage_unit to 'complete' (or 'failed').

Unlike continuum imaging, this is not a step in the main pipeline_state FSM —
units are created dynamically from cont.dat classification inside the headless
session, so there is nothing to mark 'running' in advance. The flow only
validates that the MOUS is 'selfcal_complete' (cont.dat requires it) and that
no triage run for this (mous_id, source) is already in flight.

Defaults to flexible mode (no cores/RAM pinned) -- the dirty tclean cubes
this runs are niter=0 and don't have a parallel= path wired up the way
continuum imaging does, so extra cores would sit unused. Pass ram_gb to pin
a larger RAM reservation only (never cores/mpicasa) when a specific MOUS
needs more headroom -- see the OOM-killed line-triage jobs this option
exists for (state_transitions notes for SMM1/Ser-SMM3/Serpens_Main,
2026-07-03, recovered via reset_line_triage.py).

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/line_triage_flow.py --mous-id uid://A001/X.../X... --source EC_53
    python alma-sails-codebase/flows/line_triage_flow.py --mous-id uid://A001/X.../X... --source EC_53 --ram-gb 32
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
from alma_ops.imaging.line_triage_config import THERMAL_LINEWIDTH_FLOOR_KMS
from canfar_utils import create_checked, key_line_triage, make_session, report_launch

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_line_triage_imaging.sh"


@task(name="validate-triage-ready")
def validate_triage_ready(mous_id: str, source: str, db_path: str) -> None:
    """Verify the MOUS is 'selfcal_complete', the source is a known target, and
    no triage run for (mous_id, source) is already 'running'."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)

    row = db.get_pipeline_state(conn, mous_id)
    if row is None:
        conn.close()
        raise ValueError(f"MOUS {mous_id} not found in database")

    state = row["current_state"]
    if state != "selfcal_complete":
        conn.close()
        raise ValueError(f"Expected state 'selfcal_complete', got '{state}' for {mous_id} — aborting")

    if source not in db.get_unique_target_names(conn, mous_id):
        conn.close()
        raise ValueError(f"Source '{source}' not found among targets for {mous_id}")

    in_flight = [
        u for u in db.get_line_triage_units_for_mous(conn, mous_id, source)
        if u["current_state"] == "running"
    ]
    conn.close()

    if in_flight:
        spws = [u["spw_idx"] for u in in_flight]
        raise ValueError(f"Triage already running for {mous_id} {source} (spws {spws}) — aborting")

    logger.info(f"[{mous_id} {source}] Confirmed selfcal_complete, no triage in flight")


@task(name="launch-line-triage-session", retries=2, retry_delay_seconds=60)
def launch_line_triage_session(
    mous_id: str,
    source: str,
    db_path: str,
    linewidth_floor_kms: float,
    spw: Optional[int],
    force: bool,
    ram_gb: Optional[int],
    use_staging: bool,
) -> str:
    """Submit a CASA pipeline headless session to run line-triage imaging.

    ram_gb=None (default) launches flexible: no cores/RAM pinned. ram_gb set
    to an int pins that much RAM only -- never cores, since nothing in this
    script's tclean calls can use extra cores (no parallel= wired up, unlike
    continuum imaging). Purely a CANFAR resource reservation; the underlying
    script's behavior is identical either way.
    """
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    unit_key = key_line_triage(mous_id, source)

    args = f"{mous_id} {source} {canonical_db_path} {linewidth_floor_kms}"
    args = f"{args} {spw if spw is not None else ''}"
    args = f"{args} {'1' if force else ''}"

    kwargs = dict(image=CASA_IMAGE, cmd=_HEADLESS_SCRIPT, args=args)
    if ram_gb is not None:
        kwargs["ram"] = ram_gb

    session = make_session(use_staging)
    result = create_checked(session, unit_key, **kwargs)

    mode_desc = "flexible mode" if ram_gb is None else f"{ram_gb} GB RAM pinned"
    return report_launch(logger, f"[{mous_id} {source}]", f"line-triage ({mode_desc})", result)


@flow(name="line-triage-imaging", log_prints=True)
def line_triage_mous_flow(
    mous_id: str,
    source: str,
    linewidth_floor_kms: float = THERMAL_LINEWIDTH_FLOOR_KMS,
    spw: Optional[int] = None,
    force: bool = False,
    ram_gb: Optional[int] = None,
    use_staging: bool = False,
    db_path: Optional[str] = None,
) -> None:
    """Run ADMIT-bound line-triage imaging for one MOUS/source via a CANFAR headless session.

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    source:
        Target/field name within this MOUS.
    linewidth_floor_kms:
        Narrowest protected line FWHM in km/s (default from line_triage_config).
    spw:
        Restrict to a single spw index. None (default) images all line-candidate spws.
    force:
        Re-image spws whose line_triage_unit is already 'complete'.
    ram_gb:
        RAM in GB to pin on the headless session. None (default) is flexible
        mode -- no cores/RAM pinned. Never pins cores; nothing in this
        script's tclean calls can use extra cores.
    use_staging:
        If True, submit the headless session to the CANFAR staging cluster.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    """
    db_path = db_path or str(DB_PATH)

    validate_triage_ready(mous_id, source, db_path)
    launch_line_triage_session(mous_id, source, db_path, linewidth_floor_kms, spw, force, ram_gb, use_staging)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: run ADMIT-bound line-triage imaging for one MOUS/source."
    )
    parser.add_argument("--mous-id",  required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--source",   required=True, help="Target/field name within this MOUS")
    parser.add_argument("--linewidth-floor-kms", type=float, default=THERMAL_LINEWIDTH_FLOOR_KMS,
                        help=f"Narrowest protected line FWHM in km/s (default {THERMAL_LINEWIDTH_FLOOR_KMS})")
    parser.add_argument("--spw",      type=int, default=None, help="Restrict to a single spw index")
    parser.add_argument("--force",    action="store_true",    help="Re-image spws already complete")
    parser.add_argument("--ram-gb",   type=int, default=None,
                        help="Pin this much RAM on the session (default: flexible, no cores/RAM pinned)")
    parser.add_argument("--db-path",  default=None,            help="Path to the SQLite database")
    parser.add_argument("--use-staging", action="store_true", help="Submit to CANFAR staging cluster")
    args = parser.parse_args()

    line_triage_mous_flow(
        mous_id=args.mous_id,
        source=args.source,
        linewidth_floor_kms=args.linewidth_floor_kms,
        spw=args.spw,
        force=args.force,
        ram_gb=args.ram_gb,
        db_path=args.db_path,
        use_staging=args.use_staging,
    )
