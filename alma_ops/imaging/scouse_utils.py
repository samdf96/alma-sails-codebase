"""
scouse_utils.py
---------------
DB helpers and utilities for the SCOUSE + ACORNS spectral decomposition
pipeline. No CASA dependency — all functions run from the VM shell or inside
the structure_id_env venv (Python 3.10).

FSM states (in order):
    pending → prep_complete → stage1_complete → stage2_complete →
    stage3_complete → stage4_complete → complete

current_state is always the last completed stage. A run of the next stage is tracked
in the job_* columns (stage_job): job_status 'submitted' (batch_scouse.py launched a
headless session), 'running', or 'failed' (failure_count is incremented; the unit stays
in its last completed state until the job is cleared with reset_scouse.py --clear-job).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import socket
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from alma_ops import db
from alma_ops.config import DB_PATH, SCOUSE_DIR

SCOUSE_FSM_ORDER = [
    "pending",
    "prep_complete",
    "stage1_complete",
    "stage2_complete",
    "stage3_complete",
    "stage4_complete",
    "complete",
]

# Stage name (as scouse_run.sh --stage takes it) -> state it needs, and state it leaves.
SCOUSE_STAGE_REQUIRES = {
    "1": "prep_complete", "2": "stage1_complete", "3": "stage2_complete",
    "4": "stage3_complete", "acorns": "stage4_complete",
}
SCOUSE_STAGE_ADVANCES = {
    "1": "stage1_complete", "2": "stage2_complete", "3": "stage3_complete",
    "4": "stage4_complete", "acorns": "complete",
}
# Stages batch_scouse.py launches as headless sessions; stage 1 stays an interactive GUI.
SCOUSE_HEADLESS_STAGES = ("2", "3", "4", "acorns")

JOB_ACTIVE_STATUSES = ("submitted", "running")


@contextmanager
def scouse_connection(db_path):
    """Open a DB connection for the duration of one read or one write, then close.

    A connection must never be held open across a scousepy stage. The stages run
    for minutes (stage 3/4) to hours (stage 2), and the project DB is reached
    through two mounts that cannot see each other's locks -- the VM via sshfs
    (SFTP has no byte-range locking at all) and containers via /arc. A connection
    left open across that window keeps a stale free-page list; if another client
    writes meanwhile, both can hand out the same physical page and cross-link
    B-tree pages, which corrupts the file rather than raising an error.
    """
    conn = db.get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


# ACORNS defaults, chosen from the 2026-11-03 parameter tests on S68Nc HC3N(10-9).
# Components wider than ACORNS_SIGMA_SPLIT_KMS (scousepy Gaussian sigma) are mostly
# outflow-broadened gas; clustering them apart from the narrow, quiescent components
# keeps them out of the filament/streamer structures. min_height defaults to this many
# times the mean fitted-amplitude error (the ACORNS documentation's choice), see
# default_min_height.
ACORNS_SIGMA_SPLIT_KMS = 0.6
ACORNS_MIN_HEIGHT_ERR_MULT = 3.0


def default_min_height(catalogue) -> float:
    """ACORNS min_height: ACORNS_MIN_HEIGHT_ERR_MULT x the mean amplitude error of the real fits.

    catalogue is acorns_input.dat as loaded by np.loadtxt (0=ncomps, 3=amplitude,
    4=err_amplitude). No-fit rows (ncomps 0, all zeros) and fits whose amplitude error
    exceeds the amplitude are left out: a single failed fit can carry an error of
    ~1e12 and drag the mean with it (Serpens_Emb8N H2CO, 2026-11-06), which makes no
    leaf prominent enough to stand alone and flattens the hierarchy.
    """
    import numpy as np

    amp, err = catalogue[:, 3], catalogue[:, 4]
    real = (catalogue[:, 0] > 0) & np.isfinite(err) & (err <= amp)
    if not real.any():
        real = catalogue[:, 0] > 0
    return float(ACORNS_MIN_HEIGHT_ERR_MULT * np.mean(err[real]))


def acorns_min_radius_arcsec(bmaj_arcsec: float, bmin_arcsec: float) -> float:
    """Radius of a circle with the area of one beam: ACORNS's minimum structure size."""
    return math.sqrt(1.1331 * bmaj_arcsec * bmin_arcsec / math.pi)


def component_sets(dataarr, sigma_split_kms: float) -> dict:
    """Boolean masks over the ACORNS data array's columns: {'narrow': ..., 'broad': ...}.

    Row 5 of dataarr is the scousepy component width, i.e. the Gaussian sigma.
    """
    sig = dataarr[5]
    return {"narrow": sig <= sigma_split_kms, "broad": sig > sigma_split_kms}


def safe_label(line_label: str) -> str:
    """Convert a line label to a filesystem-safe directory name.

    Examples: "HC3N(10-9)" → "HC3N_10-9", "N2H+(1-0)" → "N2H+_1-0",
    "13CN(N=2-1,J=3/2-1/2)" → "13CN_N_2-1_J_3_2-1_2"
    """
    return re.sub(r"[^A-Za-z0-9+.\-]+", "_", line_label).strip("_")


def scouse_output_dir_path(source_name: str, line_label: str) -> Path:
    """Return the (local) canonical output directory for a SCOUSE unit."""
    return SCOUSE_DIR / source_name / safe_label(line_label)


def cube_stem(cube_fits_path: str) -> str:
    """Return the FITS file stem scousepy uses as its subdirectory name.

    scousepy concatenates outputdir + filename (no path separator), so the
    stem must not include any directory component — only the bare filename
    without .fits.
    """
    name = Path(cube_fits_path).name
    return name[:-len(".fits")] if name.endswith(".fits") else name


def register_scouse_unit(
    conn,
    source_name: str,
    line_label: str,
    cube_fits_path: str,
) -> int:
    """Get or create a scouse_unit row. Returns scouse_unit_id.

    Parameters
    ----------
    conn:
        Open DB connection.
    source_name:
        Protostar / target name (e.g. "Per-emb-2").
    line_label:
        Transition label (e.g. "HC3N(10-9)").
    cube_fits_path:
        Canonical (/arc/...) path to the final line cube FITS file.
    """
    now = datetime.now(timezone.utc).isoformat()
    output_dir = db.to_canonical(str(scouse_output_dir_path(source_name, line_label)))

    existing = db.fetch_one(
        conn,
        "SELECT scouse_unit_id FROM scouse_unit WHERE source_name=? AND line_label=?",
        (source_name, line_label),
    )
    if existing is not None:
        unit_id = existing["scouse_unit_id"]
        conn.execute(
            "UPDATE scouse_unit SET cube_fits_path=?, output_dir=? WHERE scouse_unit_id=?",
            (cube_fits_path, output_dir, unit_id),
        )
        return unit_id

    cur = conn.execute(
        """
        INSERT INTO scouse_unit (source_name, line_label, cube_fits_path, output_dir, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source_name, line_label, cube_fits_path, output_dir, now),
    )
    unit_id = cur.lastrowid

    conn.execute(
        """
        INSERT INTO scouse_pipeline_state
            (scouse_unit_id, source_name, line_label, current_state, updated_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (unit_id, source_name, line_label, now),
    )
    return unit_id


def get_scouse_state(conn, source_name: str, line_label: str):
    """Fetch the scouse_pipeline_state row for a unit, or None if not registered."""
    return db.fetch_one(
        conn,
        "SELECT * FROM scouse_pipeline_state WHERE source_name=? AND line_label=?",
        (source_name, line_label),
    )


_UNIT_SQL = (
    "SELECT sps.*, su.cube_fits_path, su.output_dir FROM scouse_pipeline_state sps "
    "JOIN scouse_unit su USING (scouse_unit_id) "
)


def fetch_scouse_unit(conn, unit_id: int | None = None, source_name: str | None = None,
                      line_label: str | None = None):
    """A unit's scouse_pipeline_state row joined with its cube path and output dir.

    Looked up by unit_id when given, else by (source_name, line_label). None if absent.
    """
    if unit_id is not None:
        return db.fetch_one(conn, _UNIT_SQL + "WHERE sps.scouse_unit_id=?", (unit_id,))
    return db.fetch_one(conn, _UNIT_SQL + "WHERE sps.source_name=? AND sps.line_label=?",
                        (source_name, line_label))


def add_stage_args(parser: argparse.ArgumentParser) -> None:
    """The unit-selection and DB options every stage script takes."""
    sel = parser.add_argument_group("unit (--unit-id, or --source with --line-label)")
    sel.add_argument("--unit-id", type=int, default=None, help="scouse_unit_id")
    sel.add_argument("--source", default=None)
    sel.add_argument("--line-label", default=None)
    parser.add_argument("--db-path", default=str(DB_PATH))
    # Set by headless_scouse.sh: this run is the session batch_scouse.py submitted.
    parser.add_argument("--batch-job", action="store_true", help=argparse.SUPPRESS)


def load_stage_unit(args, stage: str):
    """Look up the unit a stage script was pointed at and check it is ready for `stage`.

    Exits with an [ERROR] if the unit is missing or not in the state the stage needs.
    For a batch job the mismatch is also recorded as a failed job, since nobody is
    watching that session's output.
    """
    if args.unit_id is None and not (args.source and args.line_label):
        sys.exit("[ERROR] give --unit-id, or both --source and --line-label")
    with scouse_connection(args.db_path) as conn:
        row = fetch_scouse_unit(conn, args.unit_id, args.source, args.line_label)
        if row is None:
            who = f"id={args.unit_id}" if args.unit_id is not None else \
                f"source='{args.source}' line='{args.line_label}'"
            print(f"[ERROR] No SCOUSE unit for {who}.")
            if args.unit_id is not None:
                print("        --unit-id is the scouse_unit_id prep printed, not the detection id.")
            sys.exit(1)
        expected = SCOUSE_STAGE_REQUIRES[stage]
        if row["current_state"] != expected:
            msg = f"expected state '{expected}', got '{row['current_state']}'"
            if args.batch_job:
                fail_scouse_job(conn, row["scouse_unit_id"], stage, msg)
            print(f"[ERROR] {row['source_name']} / {row['line_label']}: {msg}.")
            sys.exit(1)
    return row


def advance_scouse_state(
    conn,
    scouse_unit_id: int,
    source_name: str,
    line_label: str,
    from_state: str,
    to_state: str,
    note: str | None = None,
) -> None:
    """Validate current state and advance to to_state.

    Raises ValueError if the current DB state does not match from_state.
    """
    now = datetime.now(timezone.utc).isoformat()
    row = db.fetch_one(
        conn,
        "SELECT current_state FROM scouse_pipeline_state WHERE scouse_unit_id=?",
        (scouse_unit_id,),
    )
    if row is None:
        raise RuntimeError(f"scouse_pipeline_state row missing for id={scouse_unit_id}")
    actual = row["current_state"]
    if actual != from_state:
        raise ValueError(
            f"[{source_name} / {line_label}] expected state '{from_state}', got '{actual}'"
        )

    conn.execute(
        "UPDATE scouse_pipeline_state SET current_state=?, updated_at=? WHERE scouse_unit_id=?",
        (to_state, now, scouse_unit_id),
    )
    conn.execute(
        """
        INSERT INTO scouse_state_transitions
            (scouse_unit_id, source_name, line_label, from_state, to_state, at, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (scouse_unit_id, source_name, line_label, from_state, to_state, now, note),
    )
    conn.commit()


def update_scouse_parameters(conn, scouse_unit_id: int, parameters: dict) -> None:
    """Write (or replace) the parameters JSON for a SCOUSE unit."""
    conn.execute(
        "UPDATE scouse_pipeline_state SET parameters_json=? WHERE scouse_unit_id=?",
        (json.dumps(parameters), scouse_unit_id),
    )
    conn.commit()


def update_scouse_payload(conn, scouse_unit_id: int, **kwargs) -> None:
    """Update one or more tracking columns in scouse_pipeline_state.

    Accepted keyword arguments: n_fitted_components, n_acorns_clusters,
    acorns_output_path, failure_count.
    """
    allowed = {"n_fitted_components", "n_acorns_clusters", "acorns_output_path", "failure_count"}
    bad = set(kwargs) - allowed
    if bad:
        raise ValueError(f"Unknown payload fields: {bad}")
    if not kwargs:
        return
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [scouse_unit_id]
    conn.execute(
        f"UPDATE scouse_pipeline_state SET {sets} WHERE scouse_unit_id=?", vals
    )
    conn.commit()


def import_scouse(headless: bool):
    """Import and return scousepy's scouse class, with a usable matplotlib backend.

    scousepy/scouse.py calls mpl.use('qt5agg') when imported, overriding MPLBACKEND and
    any earlier matplotlib.use(). Without a display, Qt then aborts the whole process
    when the first figure is made (no Python exception, so nothing gets recorded), so
    headless runs switch back to Agg straight after the import, before any figure exists.
    """
    import matplotlib
    matplotlib.rcParams["font.family"] = "DejaVu Sans"
    from scousepy import scouse
    if headless:
        matplotlib.use("Agg", force=True)
    return scouse


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_job_id() -> str:
    """job_id recorded for a stage run from a terminal rather than a batch session."""
    return f"local:{socket.gethostname()}:{os.getpid()}"


def mark_job_submitted(conn, scouse_unit_id: int, stage: str) -> None:
    """batch_scouse.py, just before session.create(): claim the unit for `stage`.

    Written before the session exists so the session can never start without finding
    it; the session id is filled in afterwards with set_job_id.
    """
    conn.execute(
        "UPDATE scouse_pipeline_state SET job_stage=?, job_id=NULL, job_status='submitted', "
        "job_submitted_at=?, job_note=NULL WHERE scouse_unit_id=?",
        (stage, _now(), scouse_unit_id),
    )
    conn.commit()


def set_job_id(conn, scouse_unit_id: int, stage: str, job_id: str) -> None:
    """Record the session id of a submitted job, leaving its status alone (it may
    already have started)."""
    conn.execute(
        "UPDATE scouse_pipeline_state SET job_id=? WHERE scouse_unit_id=? AND job_stage=? "
        "AND job_status IN ('submitted', 'running')",
        (job_id, scouse_unit_id, stage),
    )
    conn.commit()


def clear_scouse_job(conn, scouse_unit_id: int) -> None:
    """Empty the job columns: nothing is in flight for this unit."""
    conn.execute(
        "UPDATE scouse_pipeline_state SET job_stage=NULL, job_id=NULL, job_status=NULL, "
        "job_submitted_at=NULL, job_note=NULL WHERE scouse_unit_id=?",
        (scouse_unit_id,),
    )
    conn.commit()


def fail_scouse_job(conn, scouse_unit_id: int, stage: str, note: str) -> None:
    """Mark the unit's `stage` job failed and count it. current_state is unchanged."""
    conn.execute(
        "UPDATE scouse_pipeline_state SET job_stage=?, job_status='failed', job_note=?, "
        "failure_count=failure_count+1 WHERE scouse_unit_id=?",
        (stage, note[:1000], scouse_unit_id),
    )
    conn.commit()


@contextmanager
def stage_job(args, row, stage: str):
    """Track one stage run in the job columns for as long as the block runs.

    On entry the job is marked running: a batch job takes over the 'submitted' claim
    batch_scouse.py wrote for it; a terminal run records local_job_id() and refuses to
    start while another run of this unit is submitted or running. On a clean exit the
    columns are cleared; on any exception, sys.exit and Ctrl-C included, the job is
    marked failed with the reason and the exception re-raised.
    """
    unit_id = row["scouse_unit_id"]
    with scouse_connection(args.db_path) as conn:
        cur = db.fetch_one(
            conn, "SELECT job_stage, job_id, job_status FROM scouse_pipeline_state "
                  "WHERE scouse_unit_id=?", (unit_id,))
        status, jstage, jid = cur["job_status"], cur["job_stage"], cur["job_id"]
        if args.batch_job:
            if not (status == "submitted" and jstage == stage):
                print(f"[ERROR] batch job for stage {stage}, but the unit's job is "
                      f"stage={jstage} status={status} id={jid}; not starting.")
                sys.exit(1)
            conn.execute(
                "UPDATE scouse_pipeline_state SET job_status='running', job_note=? "
                "WHERE scouse_unit_id=?", (f"started on {socket.gethostname()}", unit_id))
        else:
            if status in JOB_ACTIVE_STATUSES:
                print(f"[ERROR] stage {jstage} of this unit is already {status} (job {jid}). "
                      f"If that run is gone, clear it with reset_scouse.py --clear-job.")
                sys.exit(1)
            conn.execute(
                "UPDATE scouse_pipeline_state SET job_stage=?, job_id=?, job_status='running', "
                "job_submitted_at=?, job_note=NULL WHERE scouse_unit_id=?",
                (stage, local_job_id(), _now(), unit_id))
        conn.commit()
    try:
        yield
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            note = "interrupted (Ctrl-C)"
        elif isinstance(exc, SystemExit):
            note = f"exited with status {exc.code}"
        else:
            note = f"{type(exc).__name__}: {exc}"
        with scouse_connection(args.db_path) as conn:
            fail_scouse_job(conn, unit_id, stage, note)
        print(f"[ERROR] stage {stage} failed ({note}); recorded as a failed job. "
              f"Clear it with reset_scouse.py --clear-job before batch_scouse.py retries.",
              flush=True)
        raise
    with scouse_connection(args.db_path) as conn:
        clear_scouse_job(conn, unit_id)


def write_coverage_config(output_dir: Path, stem: str, params: dict) -> None:
    """Write (or patch) coverage.config with parameters from the DB.

    scousepy reads coverage.config before showing the stage-1 GUI. Without
    this file, it would use its own defaults and ignore our stored parameters.
    The file is written to {output_dir}/{stem}/config_files/coverage.config.
    """
    config_path = output_dir / stem / "config_files" / "coverage.config"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    vel_range   = params["vel_range"]
    wsaa        = params["wsaa"]
    mask_below  = params["mask_below"]
    nrefine     = params.get("nrefine", 1)

    if not config_path.exists():
        config_path.write_text(
            f"[DEFAULT]\n"
            f"vel_range = {vel_range}\n"
            f"wsaa = {wsaa}\n"
            f"mask_below = {mask_below}\n"
            f"nrefine = {nrefine}\n"
        )
        return

    text = config_path.read_text()
    for pattern, replacement in {
        r"^vel_range\s*=.*$":  f"vel_range = {vel_range}",
        r"^wsaa\s*=.*$":       f"wsaa = {wsaa}",
        r"^mask_below\s*=.*$": f"mask_below = {mask_below}",
        r"^nrefine\s*=.*$":    f"nrefine = {nrefine}",
    }.items():
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    config_path.write_text(text)


# Settings the stage-1 GUI can change; read back so the DB records what was used.
COVERAGE_GUI_KEYS = ("vel_range", "wsaa", "mask_below", "nrefine", "fillfactor",
                     "spacing", "totalsaas", "totalspec")


def read_coverage_config(output_dir: Path, stem: str) -> dict:
    """Return the stage-1 settings scousepy saved to coverage.config.

    The GUI writes the user's final choices (SAA size, mask threshold, ...) to
    this file only. Copying them into parameters_json keeps the DB accurate and
    stops a stage-1 re-run from overwriting them with the prep defaults via
    write_coverage_config.
    """
    import ast
    import configparser

    parser = configparser.ConfigParser()
    parser.read(output_dir / stem / "config_files" / "coverage.config")
    section = parser["DEFAULT"]
    return {key: ast.literal_eval(section[key]) for key in COVERAGE_GUI_KEYS if key in section}


def scousepy_config_path(output_dir: Path, stem: str) -> Path:
    """Return the path to scousepy.config for a unit."""
    return output_dir / stem / "config_files" / "scousepy.config"


def set_scousepy_njobs(output_dir: Path, stem: str, njobs: int) -> None:
    """Set njobs (stage 3's parallel workers) in scousepy.config.

    scousepy's run_setup always writes njobs = 3; stage 3 reads it back from the file.
    """
    path = scousepy_config_path(output_dir, stem)
    text, n = re.subn(r"(?m)^njobs\s*=.*$", f"njobs = {int(njobs)}", path.read_text())
    if n != 1:
        raise RuntimeError(f"expected one 'njobs =' line in {path}, found {n}")
    path.write_text(text)


def cube_x_east_sign(cube_fits_path: str) -> int:
    """+1 if the cube's pixel x axis runs east, -1 if it runs west (CDELT1 < 0, the norm)."""
    from astropy.io import fits

    return 1 if fits.getheader(cube_fits_path)["CDELT1"] > 0 else -1


# Fewest components a velocity-gradient plane fit is attempted on.
VGRAD_MIN_COMPONENTS = 5


def cluster_geometry(
    x_pix: "np.ndarray",
    y_pix: "np.ndarray",
    vels: "np.ndarray",
    pixel_size_arcsec: float,
    x_east_sign: int,
) -> dict:
    """Sky shape and velocity gradient of one cluster's components.

    Every component is one point at its spectrum's pixel, unweighted: a pixel fitted
    with three Gaussians counts three times and amplitude plays no part. Positions are
    sky offsets in arcsec (east, north) from the cluster's mean position.

    Parameters
    ----------
    x_pix, y_pix:
        Component pixel positions.
    vels:
        Component centroid velocities (km/s).
    pixel_size_arcsec:
        Pixel scale (arcsec/pixel), the same on both axes.
    x_east_sign:
        +1 if pixel x increases eastward, -1 if westward (see cube_x_east_sign).

    Returns
    -------
    dict with the scouse_cluster columns:
        spatial_extent_arcsec  2 x sigma of the positions along the major axis
        major_length_arcsec    5th-95th percentile range along the major axis
                               (0.9 x the full length for a uniformly filled rod)
        minor_width_arcsec     5th-95th percentile range along the minor axis
        aspect_ratio           sigma_major / sigma_minor (None if sigma_minor is 0)
        position_angle_deg     major axis, degrees east of north in [0, 180)
        vgrad_kms_per_arcsec   magnitude of the plane fit v = v0 + a*east + b*north
        vgrad_pa_deg           direction of increasing velocity, degrees east of north
                               in [0, 360)
        vgrad_resid_kms        rms velocity residual about the plane
        vgrad_r2               fraction of the velocity variance the plane explains
        vgrad_misalign_deg     angle between the gradient and the major axis, [0, 90]
    The vgrad_* values are None below VGRAD_MIN_COMPONENTS components.
    """
    import numpy as np

    east  = x_east_sign * np.asarray(x_pix, float) * pixel_size_arcsec
    north = np.asarray(y_pix, float) * pixel_size_arcsec
    east  = east - east.mean()
    north = north - north.mean()
    vels  = np.asarray(vels, float)

    eigvals, eigvecs = np.linalg.eigh(np.cov(np.vstack([east, north])))
    major, minor = eigvecs[:, 1], eigvecs[:, 0]
    sig_major = float(np.sqrt(max(eigvals[1], 0.0)))
    sig_minor = float(np.sqrt(max(eigvals[0], 0.0)))
    along  = east * major[0] + north * major[1]
    across = east * minor[0] + north * minor[1]
    pa = float(np.degrees(np.arctan2(major[0], major[1])) % 180.0)

    out = {
        "spatial_extent_arcsec": 2.0 * sig_major,
        "major_length_arcsec":   float(np.subtract(*np.percentile(along, [95, 5]))),
        "minor_width_arcsec":    float(np.subtract(*np.percentile(across, [95, 5]))),
        "aspect_ratio":          sig_major / sig_minor if sig_minor > 0 else None,
        "position_angle_deg":    pa,
        "vgrad_kms_per_arcsec":  None,
        "vgrad_pa_deg":          None,
        "vgrad_resid_kms":       None,
        "vgrad_r2":              None,
        "vgrad_misalign_deg":    None,
    }
    if len(vels) < VGRAD_MIN_COMPONENTS:
        return out

    design = np.column_stack([np.ones_like(east), east, north])
    coef, *_ = np.linalg.lstsq(design, vels, rcond=None)
    resid = vels - design @ coef
    ss_tot = float(np.sum((vels - vels.mean()) ** 2))
    grad_pa = float(np.degrees(np.arctan2(coef[1], coef[2])) % 360.0)
    misalign = abs(grad_pa - pa) % 180.0
    out.update(
        vgrad_kms_per_arcsec=float(np.hypot(coef[1], coef[2])),
        vgrad_pa_deg=grad_pa,
        vgrad_resid_kms=float(np.sqrt(np.mean(resid ** 2))),
        vgrad_r2=1.0 - float(np.sum(resid ** 2)) / ss_tot if ss_tot > 0 else None,
        vgrad_misalign_deg=min(misalign, 180.0 - misalign),
    )
    return out


GEOMETRY_COLUMNS = (
    "spatial_extent_arcsec", "major_length_arcsec", "minor_width_arcsec",
    "aspect_ratio", "position_angle_deg",
    "vgrad_kms_per_arcsec", "vgrad_pa_deg", "vgrad_resid_kms", "vgrad_r2", "vgrad_misalign_deg",
)


def insert_scouse_clusters(
    conn,
    scouse_unit_id: int,
    source_name: str,
    line_label: str,
    acorns_obj,
    dataarr: "np.ndarray",
    pixel_size_arcsec: float,
    x_east_sign: int,
    component_set: str,
) -> int:
    """Parse ACORNS output and bulk-insert one row per cluster into scouse_cluster.

    Parameters
    ----------
    acorns_obj:
        The Acorns object returned by Acorns.process().
    dataarr:
        The (6, N) array passed to Acorns.process(). ACORNS fixes the meaning of
        the first five rows (see acorns.py "For PPV data - construct the array in
        the following way"), so the rows are
        [x_pix, y_pix, amplitude_jybeam, err_amplitude_jybeam, vel_kms, sigma_kms]
        -- note rows 2 and 4 are intensity and velocity respectively, NOT the
        other way round.
    pixel_size_arcsec:
        Pixel scale in arcsec/pixel used to convert positions to arcsec.
    x_east_sign:
        +1 if pixel x increases eastward, -1 if westward (see cube_x_east_sign).
    component_set:
        'narrow' or 'broad' (the component subset this ACORNS run clustered; see
        component_sets).

    Notes
    -----
    acorns_obj.clusters holds every node of the ACORNS hierarchy -- top-level
    structures and the sub-structures nested inside them. A parent's
    cluster_members includes its descendants', so summing n_components over all
    rows double-counts. parent_acorns_cluster_id/is_top_level record the
    hierarchy so queries can restrict to distinct structures (is_top_level=1).
    Shape and gradient columns come from cluster_geometry.

    Returns
    -------
    int
        Number of rows inserted.
    """
    import numpy as np

    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for cid, cluster in acorns_obj.clusters.items():
        members = cluster.cluster_members          # indices into dataarr columns
        n = int(cluster.number_of_members)

        x_pix  = dataarr[0, members]
        y_pix  = dataarr[1, members]
        amps   = dataarr[2, members]
        vels   = dataarr[4, members]
        sigmas = dataarr[5, members]   # scousepy 'width' = Gaussian sigma, not FWHM

        geom = cluster_geometry(x_pix, y_pix, vels, pixel_size_arcsec, x_east_sign)

        antecedent = cluster.antecedent
        parent_id  = int(antecedent.cluster_idx) if antecedent is not None else None

        rows.append((
            scouse_unit_id, source_name, line_label, component_set,
            int(cid), n,
            float(np.mean(x_pix)), float(np.mean(y_pix)),
            float(np.mean(vels)), float(np.min(vels)), float(np.max(vels)), float(np.std(vels)),
            float(np.max(amps)), float(np.mean(sigmas)),
            *(geom[c] for c in GEOMETRY_COLUMNS),
            parent_id, 1 if parent_id is None else 0,
            now,
        ))

    columns = (
        "scouse_unit_id", "source_name", "line_label", "component_set",
        "acorns_cluster_id", "n_components",
        "mean_x_pix", "mean_y_pix",
        "mean_vel_kms", "vel_range_min_kms", "vel_range_max_kms", "vel_disp_kms",
        "peak_amplitude_jybeam", "mean_sigma_kms",
        *GEOMETRY_COLUMNS,
        "parent_acorns_cluster_id", "is_top_level",
        "created_at",
    )
    conn.executemany(
        f"INSERT OR REPLACE INTO scouse_cluster ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' * len(columns))})",
        rows,
    )
    conn.commit()
    return len(rows)


def clear_acorns_results(conn, scouse_unit_id: int, params: dict) -> int:
    """Remove a unit's ACORNS results from the DB; returns the cluster rows deleted.

    Deletes its scouse_cluster rows, drops _acorns_outputs from params (updated in
    place and written back) and clears the ACORNS payload columns. Does not commit.
    """
    n = conn.execute(
        "DELETE FROM scouse_cluster WHERE scouse_unit_id=?", (scouse_unit_id,)
    ).rowcount
    params.pop("_acorns_outputs", None)
    conn.execute(
        "UPDATE scouse_pipeline_state SET parameters_json=?, n_fitted_components=NULL, "
        "n_acorns_clusters=NULL, acorns_output_path=NULL WHERE scouse_unit_id=?",
        (json.dumps(params), scouse_unit_id),
    )
    return n
