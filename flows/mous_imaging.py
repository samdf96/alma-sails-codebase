"""
mous_imaging.py
---------------
Prefect flow for ALMA-SAILS continuum imaging.

Launches a headless CASA session that runs run_continuum_imaging.py for a
single (source_name, band, config_type) imaging job.  Each imaging job
combines all selfcal-complete MOUS for that grouping into a single tclean
multi-MS run.

Usage:
------
# Deployments from the alma-sails-codebase are made via:
prefect deploy --all  # from the /flows directory

Overview of Prefect Flow:
-------------------------
1. Validate that the imaging_state row exists and has status 'pending'.
2. Query get_imaging_mous_list to find contributing MOUS (MS + cont.dat paths).
3. Write a JSON payload file into the imaging_dir.
4. Launch a headless CASA session running run_imaging.sh <payload.json>.
   The driver script updates the DB to 'complete' or 'error' on exit.
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
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from alma_ops.config import (
    CASA_IMAGE_PIPE,
    DB_PATH,
    IMAGING_ROOT_DIR,
    PROJECT_ROOT,
    DualPath,
)
from alma_ops.db import (
    get_db_connection,
    get_imaging_mous_list,
    get_imaging_state_record,
    update_imaging_state_record,
)
from canfar.sessions import Session
from prefect import flow, get_run_logger, task

# =====================================================================
# Configuration for Headless Session
# =====================================================================
NCORES = 4
MEM_GB = 16

# =====================================================================
# Prefect Tasks
# =====================================================================


@task(name="Validate Imaging Job Status")
def validate_imaging_status(
    source_name: str,
    band: str,
    config_type: str,
    db_path: str,
) -> None:
    """Validate that the imaging_state row exists and is 'pending'.

    Raises
    ------
    ValueError
        If the row is missing or not in 'pending' status.
    """
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        row = get_imaging_state_record(conn, source_name, band, config_type)

    if not row:
        raise ValueError(
            f"No imaging_state row found for {source_name} Band{band} {config_type}. "
            "Run seed_imaging_state.py first."
        )

    status = row["status"]
    if status != "pending":
        raise ValueError(
            f"Imaging job {source_name} Band{band} {config_type} has status "
            f"'{status}', expected 'pending'."
        )

    log.info(f"[{source_name} B{band} {config_type}] imaging_state validated as 'pending'.")


@task(name="Build Imaging Payload")
def build_imaging_payload(
    source_name: str,
    band: str,
    config_type: str,
    field_name: str,
    imaging_dir: str,
    db_path: str,
    robust: float,
) -> dict:
    """Query DB for contributing MOUS and assemble the JSON payload dict.

    Each MOUS entry carries:
      - mous_id
      - ms_path   : the selfcal *_targets.ms (platform path, first in list)
      - cont_dat_path: {mous_directory}/auto_selfcal/cont.dat (platform path)

    Returns
    -------
    dict
        Payload ready to be JSON-serialised and passed to run_continuum_imaging.py.

    Raises
    ------
    ValueError
        If no contributing MOUS are found.
    """
    log = get_run_logger()

    with get_db_connection(db_path) as conn:
        mous_rows = get_imaging_mous_list(conn, source_name, band, config_type)

    if not mous_rows:
        raise ValueError(
            f"No selfcal-complete MOUS found for {source_name} Band{band} {config_type}."
        )

    mous_list = []
    for row in mous_rows:
        mous_id = row["mous_id"]

        # selfcal_products_nonsub_path is stored as a JSON list of platform paths
        raw = row["selfcal_products_nonsub_path"]
        ms_paths: list[str] = json.loads(raw) if isinstance(raw, str) else raw
        if not ms_paths:
            log.warning(f"  [{mous_id}] selfcal_products_nonsub_path is empty — skipping.")
            continue

        # Use the first (and usually only) *_targets.ms
        ms_path = ms_paths[0]

        # cont.dat lives alongside auto_selfcal products: {mous_dir}/auto_selfcal/cont.dat
        mous_dir_platform = row["mous_directory"]  # already a platform path
        cont_dat_path = str(Path(mous_dir_platform) / "auto_selfcal" / "cont.dat")

        # Verify cont.dat exists on the VM mount
        cont_dat_vm = DualPath(cont_dat_path).vm
        if not cont_dat_vm.exists():
            log.warning(f"  [{mous_id}] cont.dat not found at {cont_dat_vm} — skipping.")
            continue

        mous_list.append(
            {
                "mous_id": mous_id,
                "ms_path": ms_path,
                "cont_dat_path": cont_dat_path,
            }
        )
        log.info(f"  [{mous_id}] ms={Path(ms_path).name}  cont.dat={cont_dat_vm.parent.name}")

    if not mous_list:
        raise ValueError(
            f"After filtering, no usable MOUS remain for "
            f"{source_name} Band{band} {config_type}."
        )

    payload = {
        "source_name":  source_name,
        "band":         band,
        "config_type":  config_type,
        "field_name":   field_name,
        "imaging_dir":  imaging_dir,
        "db_path":      DualPath(db_path).platform,
        "robust":       robust,
        "mous_list":    mous_list,
    }

    log.info(
        f"[{source_name} B{band} {config_type}] "
        f"payload built with {len(mous_list)} MOUS."
    )
    return payload


@task(name="Launch Imaging Headless Session", retries=2, retry_delay_seconds=20)
def launch_imaging_headless_session(
    source_name: str,
    band: str,
    config_type: str,
    payload_path: str,
    log_dir: str,
) -> str:
    """Submit the CASA imaging job to a CANFAR headless session.

    Parameters
    ----------
    payload_path : str
        Platform path to the JSON payload file.
    log_dir : str
        Platform path to the directory where CASA logs will be written.

    Returns
    -------
    str
        The submitted job ID.
    """
    log = get_run_logger()

    run_imaging_sh = DualPath(
        Path(PROJECT_ROOT)
        / "alma-sails-codebase"
        / "alma_ops"
        / "imaging"
        / "run_imaging.sh"
    )

    job_name = (
        f"casa-{datetime.now().strftime('%Y%m%d-%H%M')}"
        f"-imaging-{source_name}-B{band}-{config_type}"
    )

    log.info(
        f"[{source_name} B{band} {config_type}] Launching headless session: "
        f"{NCORES} cores, {MEM_GB} GB"
    )

    session = Session()
    job_id = session.create(
        name=job_name,
        image=CASA_IMAGE_PIPE,
        cores=NCORES,
        ram=MEM_GB,
        cmd=run_imaging_sh.platform,
        args=f"{payload_path} {log_dir}",
    )

    if not job_id:
        raise RuntimeError("Headless session launch returned no job ID.")

    log.info(f"[{source_name} B{band} {config_type}] Job ID: {job_id[0]}")
    return job_id[0]


# =====================================================================
# Prefect Flow
# =====================================================================


@flow(name="Continuum Imaging Job")
def continuum_imaging_flow(
    source_name: str,
    band: str,
    config_type: str,
    field_name: Optional[str] = None,
    robust: float = 0.5,
    db_path: Optional[str] = None,
) -> None:
    """Prefect flow to launch continuum imaging for one (source, band, config) job.

    Parameters
    ----------
    source_name : str
        ALMA source name matching the imaging_state row, e.g. 'EC_53'.
    band : str
        ALMA band, e.g. '6'.
    config_type : str
        Array configuration type: '12m', '7m', or 'tp'.
    field_name : str, optional
        Field name as it appears in cont.dat.  Defaults to source_name.
    robust : float
        Briggs robust parameter (default 0.5).
    db_path : str, optional
        Path to the SQLite database.  Defaults to the configured DB_PATH.
    """
    log = get_run_logger()

    db_path = db_path or str(DB_PATH)
    field_name = field_name or source_name

    log.info(
        f"[{source_name} B{band} {config_type}] "
        f"Starting continuum imaging flow  robust={robust}"
    )

    # 1. Validate DB state
    validate_imaging_status(source_name, band, config_type, db_path)

    # 2. Resolve imaging directory (DualPath: DB stores platform path, we need VM for mkdir)
    imaging_dir = DualPath(IMAGING_ROOT_DIR / source_name / f"Band{band}_{config_type}")
    imaging_dir.vm.mkdir(parents=True, exist_ok=True)
    log.info(f"[{source_name} B{band} {config_type}] imaging_dir: {imaging_dir.vm}")

    # 3. Mark as 'running' in DB
    with get_db_connection(db_path) as conn:
        update_imaging_state_record(
            conn,
            source_name,
            band,
            config_type,
            status="running",
            imaging_dir=imaging_dir.platform,
        )

    try:
        # 4. Build payload
        payload = build_imaging_payload(
            source_name=source_name,
            band=band,
            config_type=config_type,
            field_name=field_name,
            imaging_dir=imaging_dir.platform,
            db_path=db_path,
            robust=robust,
        )

        # 5. Write payload JSON into the imaging directory on the VM
        payload_filename = f"{source_name}_B{band}_{config_type}_payload.json"
        payload_path_vm  = imaging_dir.vm / payload_filename
        payload_path_vm.write_text(json.dumps(payload, indent=2))
        log.info(f"[{source_name} B{band} {config_type}] payload written: {payload_path_vm}")

        # 6. Log directory for CASA output (platform path for headless session)
        log_dir = DualPath(imaging_dir.vm / "logs")
        log_dir.vm.mkdir(parents=True, exist_ok=True)

        # 7. Launch headless session
        launch_imaging_headless_session(
            source_name=source_name,
            band=band,
            config_type=config_type,
            payload_path=DualPath(payload_path_vm).platform,
            log_dir=log_dir.platform,
        )

    except Exception as e:
        log.error(f"[{source_name} B{band} {config_type}] Imaging flow failed: {e}")
        with get_db_connection(db_path) as conn:
            update_imaging_state_record(
                conn,
                source_name,
                band,
                config_type,
                status="error",
                last_error_message=str(e),
            )
        raise
