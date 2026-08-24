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
import re
import shutil
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, get_run_logger, task

from alma_ops import db
from alma_ops.config import CASA_IMAGE, CONTINUUM_IMAGING_DIR, DB_PATH
from alma_ops.db import CANONICAL_PREFIX
from canfar_utils import make_session

_HEADLESS_SCRIPT = f"{CANONICAL_PREFIX}/alma-sails-codebase/scripts/headless_imaging.sh"

_PARALLEL_RAM_GB = 16  # default RAM when ncores > 1 and ram_gb not specified


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
    """Query the DB and assemble the JSON payload for headless_continuum_imaging.py."""
    logger = get_run_logger()
    conn = db.get_connection(db_path)

    mous_rows = db.get_mous_for_imaging_unit(conn, imaging_unit_id)
    mous_list = []
    skipped = []
    is_mosaic = False

    for row in mous_rows:
        mous_id  = row["mous_id"]
        ms_paths = db.get_selfcal_products(conn, mous_id, continuum_subtracted=False)
        mous_dir = db.get_mous_directory(conn, mous_id)

        if not ms_paths:
            skipped.append(f"{mous_id} (no selfcal products)")
            continue

        cont_dat = mous_dir / "auto_selfcal" / "cont.dat" if mous_dir else None
        if cont_dat is None or not cont_dat.exists():
            skipped.append(f"{mous_id} (cont.dat missing)")
            continue

        # source_field_name/field_ids are usually NULL -- only set for imaging
        # units that select a subset of fields out of a shared-name mosaic
        # MOUS (see migrate_add_field_selection_columns.py).
        source_field_name = row["source_field_name"]
        field_ids = json.loads(row["field_ids"]) if row["field_ids"] else None

        mous_list.append({
            "mous_id":           mous_id,
            "ms_paths":          [db.to_canonical(str(p)) for p in ms_paths],
            "cont_dat_path":     db.to_canonical(str(cont_dat)),
            "source_field_name": source_field_name,
            "field_ids":         field_ids,
        })

        # Always look up is_mosaic via the imaging unit's own target_name.
        # source_field_name is the parent MOUS's MS field name (e.g.
        # "Serpens_Main_and_Serpens_South") — inheriting its mosaic flag would
        # incorrectly mark a single extracted field as a mosaic and cause
        # compute_mosaic_imaging_params to span the full parent footprint.
        target_row = next(
            (
                r for r in db.get_mous_targets(conn, mous_id)
                if r["alma_source_name"] == source
            ),
            None,
        )
        if target_row is not None and target_row["is_mosaic"] == "T":
            is_mosaic = True

    conn.close()

    if skipped:
        for s in skipped:
            logger.warning(f"[{source} B{band} {config}] Skipped MOUS: {s}")

    if not mous_list:
        raise ValueError(
            f"No usable MOUS entries for {source} B{band} {config} — "
            "check selfcal_products and cont.dat in DB."
        )

    imaging_dir = db.to_canonical(
        str(CONTINUUM_IMAGING_DIR / source / f"B{band}_{config}")
    )

    parallel = ncores is not None and ncores > 1
    payload = {
        "imaging_unit_id": imaging_unit_id,
        "field_name":      source,
        "imaging_dir":     imaging_dir,
        "db_path":         db.to_canonical(db_path),
        "robust":          robust,
        "run_id":          run_id,
        "mous_list":       mous_list,
        "ncores":          ncores or 1,
        "parallel":        parallel,
        "is_mosaic":       is_mosaic,
    }

    logger.info(
        f"[{source} B{band} {config}] Payload built: "
        f"{len(mous_list)} MOUS(es), imaging_dir={imaging_dir}, is_mosaic={is_mosaic}"
    )
    return payload


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
    ncores: Optional[int],
    ram_gb: Optional[int],
    use_staging: bool,
) -> str:
    """Submit the headless CASA imaging session to CANFAR.

    ncores=None launches in flexible mode (no cores/RAM specified).
    ncores>1 enables parallel tclean via mpicasa; RAM defaults to _PARALLEL_RAM_GB.
    """
    logger = get_run_logger()

    _safe = lambda s: re.sub(r"[^a-zA-Z0-9-]", "-", s)
    job_name = f"imaging-{_safe(source)}-B{band}-{_safe(config)}-{datetime.now().strftime('%Y%m%d-%H%M')}"
    session  = make_session(use_staging)

    _stdout = io.StringIO()
    _stderr = io.StringIO()

    if ncores is None:
        logger.info(f"[{source} B{band} {config}] Attempting session launch (flexible mode) ...")
    else:
        effective_ram = ram_gb if ram_gb is not None else _PARALLEL_RAM_GB
        logger.info(
            f"[{source} B{band} {config}] Attempting session launch: "
            f"{ncores} cores, {effective_ram} GB RAM, parallel={ncores > 1} ..."
        )

    try:
        with contextlib.redirect_stdout(_stdout), contextlib.redirect_stderr(_stderr):
            if ncores is None:
                job_id = session.create(
                    name=job_name,
                    image=CASA_IMAGE,
                    cmd=_HEADLESS_SCRIPT,
                    args=payload_path,
                )
            else:
                job_id = session.create(
                    name=job_name,
                    image=CASA_IMAGE,
                    cores=ncores,
                    ram=effective_ram,
                    cmd=_HEADLESS_SCRIPT,
                    args=payload_path,
                )
    except Exception as exc:
        _captured = (_stdout.getvalue() + _stderr.getvalue()).strip()
        msg = f"session.create() raised for {source} B{band} {config}: {exc}"
        if _captured:
            msg += f"\nSession output:\n{_captured}"
        raise RuntimeError(msg) from exc

    _captured = (_stdout.getvalue() + _stderr.getvalue()).strip()
    if _captured:
        logger.warning(f"[{source} B{band} {config}] Session output:\n{_captured}")

    if not job_id:
        msg = f"CANFAR session launch returned no job ID for {source} B{band} {config}"
        if _captured:
            msg += f"\nSession output:\n{_captured}"
        raise RuntimeError(msg)

    logger.info(f"[{source} B{band} {config}] Session launched successfully: {job_id[0]}")
    return job_id[0]


@flow(name="continuum-imaging", log_prints=True)
def imaging_unit_flow(
    source: str,
    band: str,
    config: str,
    robust: float = 0.5,
    ncores: Optional[int] = None,
    ram_gb: Optional[int] = None,
    force: bool = False,
    use_staging: bool = False,
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
        RAM in GB. Only used when ncores is not None. Defaults to 16 GB when
        ncores > 1 and not specified.
    force:
        If True, delete existing products for the given robust value and re-image.
    use_staging:
        If True, submit to the CANFAR staging cluster.
    db_path:
        Path to serpens_v2.db (defaults to config.DB_PATH).
    """
    logger = get_run_logger()
    db_path = db_path or str(DB_PATH)
    run_id  = str(uuid.uuid4())

    imaging_unit_id, current_state = validate_imaging_ready(source, band, config, robust, db_path, force)
    payload                        = build_imaging_payload(imaging_unit_id, source, band, config, robust, run_id, ncores, db_path)
    payload_path                   = write_payload_file(payload)
    mark_imaging_running(imaging_unit_id, current_state, run_id, db_path)

    try:
        launch_imaging_session(payload_path, source, band, config, ncores, ram_gb, use_staging)
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
    parser.add_argument("--ram-gb",  type=int, default=None, help="RAM in GB (default 16 when ncores>1, ignored in flexible mode)")
    parser.add_argument("--force",       action="store_true",                 help="Delete existing products and re-image")
    parser.add_argument("--db-path",    default=None,                        help="Path to serpens_v2.db")
    parser.add_argument("--use-staging", action="store_true",                help="Submit to CANFAR staging cluster")
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
        use_staging=args.use_staging,
    )