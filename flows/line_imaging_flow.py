"""
line_imaging_flow.py
-----------------------
Prefect flow for deep (niter>0) line imaging of one human-confirmed ADMIT
detection.

Launches a CANFAR headless session running scripts/headless_line_imaging.sh
inside the CASA pipeline container. The headless session runs
manual_line_imaging.py, which resolves the parent triage/MOUS chain, computes
a frequency window centered on the confirmed line, runs
tclean -> impbcor -> exportfits, and advances the line_imaging_unit to
'complete' (or 'failed').

Like line_admit_flow.py, the line_imaging_unit row doesn't necessarily exist
before this flow runs (it's created via get_or_create inside the headless
session) -- so this flow's pre-flight guard checks the detection's
review_status is 'confirmed' and that no line_imaging_unit for it is already
'running', and writes nothing to the DB itself before submitting.

Defaults to flexible mode (no cores/RAM pinned), like imaging_flow.py, not
line_triage_flow.py/line_admit_flow.py's RAM-only pattern -- unlike triage
imaging or ADMIT, run_line_imaging genuinely threads parallel= through to
tclean, so cores can demonstrably help here. Pass ncores>1 for fixed mode
(mpicasa), RAM defaults to 16 GB unless ram_gb is set explicitly.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/line_imaging_flow.py --detection-id 2
    python alma-sails-codebase/flows/line_imaging_flow.py --detection-id 2 --ncores 4
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
from alma_ops.imaging.continuum_config import DEFAULT_ROBUST
from canfar_utils import create_checked, key_line_imaging, make_session, report_launch, resource_desc

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_line_imaging.sh"

_PARALLEL_RAM_GB = 16  # default RAM when ncores > 1 and ram_gb not specified


@task(name="validate-line-imaging-ready")
def validate_line_imaging_ready(detection_id: int, db_path: str) -> None:
    """Verify the detection is 'confirmed' and no line_imaging_unit for it is
    already 'running'."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)

    detection = db.get_line_admit_detection(conn, detection_id)
    if detection is None:
        conn.close()
        raise ValueError(f"No line_admit_detection_id={detection_id} found")

    if detection["review_status"] != "confirmed":
        conn.close()
        raise ValueError(
            f"detection {detection_id} has review_status='{detection['review_status']}' — "
            f"expected 'confirmed'"
        )

    unit_row = db.get_line_imaging_unit_for_detection(conn, detection_id)
    conn.close()

    if unit_row is not None and unit_row["current_state"] == "running":
        raise ValueError(f"Line imaging already running for detection {detection_id} — aborting")

    logger.info(f"[detection {detection_id}] Confirmed, no line-imaging run in flight")


@task(name="launch-line-imaging-session", retries=2, retry_delay_seconds=60)
def launch_line_imaging_session(
    detection_id: int,
    db_path: str,
    force: bool,
    ncores: Optional[int],
    ram_gb: Optional[int],
    use_staging: bool,
    robust: Optional[float] = None,
) -> str:
    """Submit a CASA pipeline headless session to run deep line imaging.

    ncores=None launches in flexible mode: no cores/RAM pinned, and
    manual_line_imaging.py (told via ncores=0) runs plain casa -c. ncores>1
    launches in fixed mode: cores/RAM pinned via session.create(), and
    manual_line_imaging.py runs mpicasa -n ncores with parallel=True threaded
    through to tclean.
    """
    logger = get_run_logger()

    canonical_db_path = db.to_canonical(db_path)
    # robust=None means "let the headless script use its default"; the key has
    # to name the value that will actually be used, or a None launch and an
    # explicit-0.5 launch would look like different units to the guard.
    unit_key = key_line_imaging(
        detection_id, DEFAULT_ROBUST if robust is None else robust)

    args = (f"{detection_id} {canonical_db_path} {'1' if force else ''} "
            f"{ncores if ncores is not None else 0}"
            f"{'' if robust is None else f' {robust}'}")

    kwargs = dict(image=CASA_IMAGE, cmd=_HEADLESS_SCRIPT, args=args)
    if ncores is not None:
        kwargs["cores"] = ncores
    effective_ram = ram_gb if ram_gb is not None else (
        _PARALLEL_RAM_GB if ncores is not None else None)
    if effective_ram is not None:
        kwargs["ram"] = effective_ram

    session = make_session(use_staging)
    result = create_checked(session, unit_key, **kwargs)

    mode_desc = resource_desc(ncores, effective_ram)
    return report_launch(logger, f"[detection {detection_id}]", f"line-imaging ({mode_desc})", result)


@flow(name="line-imaging", log_prints=True)
def line_imaging_flow(
    detection_id: int,
    force: bool = False,
    ncores: Optional[int] = None,
    ram_gb: Optional[int] = None,
    robust: Optional[float] = None,
    use_staging: bool = False,
    db_path: Optional[str] = None,
) -> None:
    """Run deep line imaging for one confirmed ADMIT detection via a CANFAR
    headless session.

    Parameters
    ----------
    detection_id:
        line_admit_detection_id. Must already have review_status='confirmed'.
    force:
        Re-run even if the line_imaging_unit is already 'complete'. Required to
        image a detection at a second robust value, since the unit is already
        'complete' from the first -- imaging_products gains a second entry
        rather than replacing the first.
    robust:
        Briggs robust weighting. None (default) leaves it to
        manual_line_imaging.py's own default of 0.5. Output filenames carry the
        value (_r05 / _r20), so several weightings coexist without overwriting
        each other.
    ncores:
        MPI cores to allocate. None (default) is flexible mode -- no
        cores/RAM pinned, plain casa -c. >1 enables parallel tclean via
        mpicasa; RAM defaults to 16 GB unless ram_gb is set explicitly.
    ram_gb:
        RAM in GB to pin on the headless session, independently of ncores --
        pinning RAM alone is valid. Defaults to 16 GB when ncores is set.
    use_staging:
        If True, submit the headless session to the CANFAR staging cluster.
    db_path:
        Path to the pipeline SQLite database (defaults to config.DB_PATH).
    """
    db_path = db_path or str(DB_PATH)

    validate_line_imaging_ready(detection_id, db_path)
    launch_line_imaging_session(detection_id, db_path, force, ncores, ram_gb,
                                use_staging, robust)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: run deep line imaging for one confirmed ADMIT detection."
    )
    parser.add_argument("--detection-id", type=int, required=True, help="line_admit_detection_id")
    parser.add_argument("--force",    action="store_true",    help="Re-run even if already complete")
    parser.add_argument("--ncores",   type=int, default=None,
                        help="MPI cores (omit for flexible mode; >1 enables parallel tclean)")
    parser.add_argument("--ram-gb",   type=int, default=None,
                        help="RAM in GB to pin (independent of --ncores; defaults to 16 when ncores is set)")
    parser.add_argument("--robust",   type=float, default=None,
                        help="Briggs robust weighting (default: 0.5). Output names "
                             "carry it as _r05/_r20, so weightings do not overwrite.")
    parser.add_argument("--db-path",  default=None,            help="Path to the SQLite database")
    parser.add_argument("--use-staging", action="store_true", help="Submit to CANFAR staging cluster")
    args = parser.parse_args()

    line_imaging_flow(
        detection_id=args.detection_id,
        force=args.force,
        ncores=args.ncores,
        ram_gb=args.ram_gb,
        robust=args.robust,
        db_path=args.db_path,
        use_staging=args.use_staging,
    )
