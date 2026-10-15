"""
imaging_flow.py
---------------
Prefect flow for continuum imaging of one imaging unit.

Validates that the imaging unit is 'ready', builds a JSON payload from the
database, marks the unit 'running', then launches a CANFAR headless CASA
session running scripts/headless_imaging.sh.  The headless session performs
tclean + impbcor + exportfits and advances the unit to 'complete' or 'failed'.

The flow exits immediately after session launch (fire-and-forget).  If the
session launch itself fails, the flow records 'failed' in the DB.

Deployment:
    prefect deploy --all   (from the flows/ directory)

Direct invocation (VM shell, for testing):
    python alma-sails-codebase/flows/imaging_flow.py --source EC_53 --band 6 --config 12m

Invoke deployed flow:
    prefect run deployment continuum-imaging --param source=EC_53 --param band=6 --param config=12m
"""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, get_run_logger, task

from alma_ops import db
from alma_ops.imaging.imaging_payload import (
    build_imaging_payload as _build_imaging_payload,
)
from alma_ops.config import CASA_IMAGE, CONTINUUM_IMAGING_DIR, DB_PATH
from alma_ops.db import CANONICAL_PREFIX
from canfar_utils import (create_checked, ensure_unit_free, key_imaging, make_session,
                          report_launch, resource_desc)

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_imaging.sh"

_PARALLEL_RAM_GB = 16  # default RAM when ncores > 1 and ram_gb not specified


@task(name="check-unit-not-running")
def check_unit_not_running(source: str, band: str, config: str, robust: float) -> None:
    """Abort if a CANFAR session is already imaging this unit.

    Runs before validate_imaging_ready, which deletes existing products under
    force=True -- and deletes them at the exact image base a running session is
    writing to. Discovering the conflict later, at launch time, would leave that
    session's working set already destroyed.
    """
    logger = get_run_logger()
    unit_key = key_imaging(source, band, config, robust)
    ensure_unit_free(make_session(), unit_key)
    logger.info(f"[{source} B{band} {config}] No session running on {unit_key}")


@task(name="validate-imaging-ready")
def validate_imaging_ready(
    source: str, band: str, config: str, robust: float, db_path: str, force: bool = False
) -> tuple:
    """Verify the imaging unit is runnable. Returns (imaging_unit_id, current_state)."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)
    unit = db.get_imaging_unit(conn, source, band, config)

    if unit is None:
        conn.close()
        raise ValueError(f"No imaging unit found for source='{source}' band={band} config={config}")

    imaging_unit_id = unit["imaging_unit_id"]
    ips = db.get_imaging_pipeline_state(conn, imaging_unit_id)

    if ips is None:
        conn.close()
        raise ValueError(f"No imaging_pipeline_state row for imaging_unit_id={imaging_unit_id}")

    state = ips["current_state"]
    if state not in ("ready", "complete", "failed"):
        conn.close()
        raise ValueError(
            f"Cannot run imaging: unit is in state '{state}' for {source} B{band} {config} — "
            f"expected 'ready', 'complete', or 'failed'"
        )

    existing_products = json.loads(ips["imaging_products"] or "[]")
    if isinstance(existing_products, dict):
        existing_products = [existing_products]
    matching = [e for e in existing_products if e.get("robust") == robust]
    if matching:
        if not force:
            conn.close()
            raise ValueError(
                f"Products for robust={robust} already exist for {source} B{band} {config} — "
                f"pass force=True to overwrite."
            )
        for entry in matching:
            for key in ("image_pbcor_fits", "image_fits", "image_pbcor", "image"):
                path = entry.get(key)
                if path:
                    p = Path(db.to_local(path))
                    if p.exists():
                        shutil.rmtree(str(p)) if p.is_dir() else p.unlink()
        db.remove_imaging_products_entry(conn, imaging_unit_id, robust)
        logger.info(f"[{source} B{band} {config}] force=True: removed existing products for robust={robust}")

    conn.close()
    logger.info(f"[{source} B{band} {config}] State '{state}', proceeding with robust={robust}")
    return imaging_unit_id, state


@task(name="build-imaging-payload")
def build_imaging_payload(
    imaging_unit_id: int,
    source: str,
    band: str,
    config: str,
    robust: float,
    run_id: str,
    ncores: Optional[int],
    db_path: str,
) -> dict:
    """Thin wrapper over alma_ops.imaging.imaging_payload.build_imaging_payload.

    The implementation is shared with batch_continuum_reimage.py, which launches
    CANFAR sessions directly with no Prefect server; keeping one copy avoids the
    two drifting apart on subtleties like the is_mosaic determination.
    """
    return _build_imaging_payload(
        imaging_unit_id=imaging_unit_id, source=source, band=band, config=config,
        robust=robust, run_id=run_id, ncores=ncores, db_path=db_path,
    )


@task(name="write-imaging-payload")
def write_payload_file(payload: dict) -> str:
    """Write the payload JSON to the imaging directory. Returns the canonical path."""
    imaging_dir = Path(db.to_local(payload["imaging_dir"]))
    imaging_dir.mkdir(parents=True, exist_ok=True)

    payload_path = imaging_dir / "imaging_payload.json"
    with open(payload_path, "w") as fh:
        json.dump(payload, fh, indent=2)

    return db.to_canonical(str(payload_path))


@task(name="mark-imaging-running")
def mark_imaging_running(
    imaging_unit_id: int, from_state: str, run_id: str, db_path: str
) -> None:
    """Advance the imaging unit to 'running' before submitting the headless session."""
    conn = db.get_connection(db_path)
    db.record_imaging_transition(
        conn,
        imaging_unit_id=imaging_unit_id,
        from_state=from_state,
        to_state="running",
        run_id=run_id,
        note="submitted to CANFAR",
        step="start",
    )
    conn.close()


@task(name="launch-imaging-session", retries=2, retry_delay_seconds=60)
def launch_imaging_session(
    payload_path: str,
    source: str,
    band: str,
    config: str,
    robust: float,
    ncores: Optional[int],
    ram_gb: Optional[int],
) -> str:
    """Submit the headless CASA imaging session to CANFAR.

    ncores=None launches in flexible mode (no cores/RAM specified).
    ncores>1 enables parallel tclean via mpicasa; RAM defaults to _PARALLEL_RAM_GB.
    """
    logger = get_run_logger()

    unit_key = key_imaging(source, band, config, robust)
    session  = make_session()

    _stdout = io.StringIO()
    _stderr = io.StringIO()

    # cores and RAM are pinned independently. Pinning RAM alone is the useful
    # case for a job that OOMs without benefiting from parallel tclean -- it
    # used to be silently ignored, since ram_gb was only read inside the
    # ncores branch.
    kwargs = dict(image=CASA_IMAGE, cmd=_HEADLESS_SCRIPT, args=payload_path)
    if ncores is not None:
        kwargs["cores"] = ncores
    effective_ram = ram_gb if ram_gb is not None else (
        _PARALLEL_RAM_GB if ncores is not None else None)
    if effective_ram is not None:
        kwargs["ram"] = effective_ram
    logger.info(f"[{source} B{band} {config}] Attempting session launch "
                f"({resource_desc(ncores, effective_ram)}) ...")

    try:
        with contextlib.redirect_stdout(_stdout), contextlib.redirect_stderr(_stderr):
            result = create_checked(session, unit_key, **kwargs)
    except Exception as exc:
        _captured = (_stdout.getvalue() + _stderr.getvalue()).strip()
        msg = f"session launch failed for {source} B{band} {config}: {exc}"
        if _captured:
            msg += f"\nSession output:\n{_captured}"
        raise RuntimeError(msg) from exc

    _captured = (_stdout.getvalue() + _stderr.getvalue()).strip()
    if _captured:
        logger.warning(f"[{source} B{band} {config}] Session output:\n{_captured}")

    return report_launch(logger, f"[{source} B{band} {config}]", "imaging", result)


@flow(name="continuum-imaging", log_prints=True)
def imaging_unit_flow(
    source: str,
    band: str,
    config: str,
    robust: float = 0.5,
    ncores: Optional[int] = None,
    ram_gb: Optional[int] = None,
    force: bool = False,
    db_path: Optional[str] = None,
) -> None:
    """Run continuum imaging for one imaging unit via a CANFAR headless session.

    Parameters
    ----------
    source:
        Target/source name matching target_name in the imaging_unit table.
    band:
        Band number as a string ('3', '6', '7', etc.).
    config:
        Configuration type ('12m', '7m', 'tp', 'combined').
    robust:
        Briggs robust weighting (default 0.5).
    ncores:
        CPU cores for the headless session. None (default) = flexible mode, no
        cores/RAM pinned. 1 = single-core fixed. >1 = parallel tclean via mpicasa,
        RAM defaults to 16 GB unless ram_gb is set explicitly.
    ram_gb:
        RAM in GB to pin on the session, independently of ncores -- pinning
        RAM alone is valid and is the right knob for a job that OOMs without
        benefiting from parallel tclean. Defaults to 16 GB when
        ncores > 1 and not specified.
    force:
        If True, delete existing products for the given robust value and re-image.
    db_path:
        Path to serpens_v2.db (defaults to config.DB_PATH).
    """
    logger = get_run_logger()
    db_path = db_path or str(DB_PATH)
    run_id  = str(uuid.uuid4())

    check_unit_not_running(source, band, config, robust)
    imaging_unit_id, current_state = validate_imaging_ready(source, band, config, robust, db_path, force)
    payload                        = build_imaging_payload(imaging_unit_id, source, band, config, robust, run_id, ncores, db_path)
    payload_path                   = write_payload_file(payload)
    mark_imaging_running(imaging_unit_id, current_state, run_id, db_path)

    try:
        launch_imaging_session(payload_path, source, band, config, robust, ncores, ram_gb)
    except Exception as e:
        logger.error(f"[{source} B{band} {config}] Session launch failed — recording failure: {e}")
        conn = db.get_connection(db_path)
        db.record_imaging_failure(
            conn,
            imaging_unit_id=imaging_unit_id,
            from_state="running",
            error=str(e),
            run_id=run_id,
            step="launch",
        )
        conn.close()
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prefect flow: run continuum imaging for one imaging unit."
    )
    parser.add_argument("--source",     required=True,                       help="Source/target name")
    parser.add_argument("--band",       required=True,                       help="Band number (e.g. '6')")
    parser.add_argument("--config",     required=True,                       help="Config type: 12m, 7m, tp, combined")
    parser.add_argument("--robust",     type=float, default=0.5,             help="Briggs robust (default 0.5)")
    parser.add_argument("--ncores",  type=int, default=None, help="CPU cores (omit for flexible mode; >1 enables parallel tclean)")
    parser.add_argument("--ram-gb",  type=int, default=None, help="RAM in GB to pin (independent of --ncores; defaults to 16 when ncores is set)")
    parser.add_argument("--force",       action="store_true",                 help="Delete existing products and re-image")
    parser.add_argument("--db-path",    default=None,                        help="Path to serpens_v2.db")
    args = parser.parse_args()

    imaging_unit_flow(
        source=args.source,
        band=args.band,
        config=args.config,
        robust=args.robust,
        ncores=args.ncores,
        ram_gb=args.ram_gb,
        force=args.force,
        db_path=args.db_path,
    )