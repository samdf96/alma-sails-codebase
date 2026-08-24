"""
scouse_utils.py
---------------
DB helpers and utilities for the SCOUSE + ACORNS spectral decomposition
pipeline. No CASA dependency — all functions run from the VM shell or inside
the structure_id_env venv (Python 3.10).

FSM states (in order):
    pending → prep_complete → stage1_complete → stage2_complete →
    stage3_complete → stage4_complete → acorns_complete → complete
Any state can also transition to failed.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from alma_ops import db
from alma_ops.config import SCOUSE_DIR

SCOUSE_FSM_ORDER = [
    "pending",
    "prep_complete",
    "stage1_complete",
    "stage2_complete",
    "stage3_complete",
    "stage4_complete",
    "complete",
]

SCOUSE_INTERACTIVE_STAGES = {"stage1_complete", "stage2_complete", "stage4_complete"}


def safe_label(line_label: str) -> str:
    """Convert a line label to a filesystem-safe directory name.

    Examples: "HC3N(10-9)" → "HC3N_10-9", "N2H+(1-0)" → "N2H+_1-0"
    """
    return line_label.replace("(", "_").replace(")", "")


def scouse_output_dir_path(source_name: str, line_label: str) -> Path:
    """Return the (local) canonical output directory for a SCOUSE unit."""
    return SCOUSE_DIR / source_name / safe_label(line_label)


def cube_stem(cube_fits_path: str) -> str:
    """Return the FITS file stem scousepy uses as its subdirectory name.

    scousepy concatenates outputdir + filename (no path separator), so the
    stem must not include any directory component — only the bare filename
    without .fits.
    """
    return Path(cube_fits_path).name.removesuffix(".fits")


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
        return existing["scouse_unit_id"]

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


def scousepy_config_path(output_dir: Path, stem: str) -> Path:
    """Return the path to scousepy.config for a unit."""
    return output_dir / stem / "config_files" / "scousepy.config"
