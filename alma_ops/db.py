"""
db.py
-----
Database access layer for the ALMA-SAILS pipeline.

Responsibilities:
  - Connection management
  - Path translation (canonical ↔ local) at the DB boundary
  - Low-level query primitives
  - MOUS and imaging unit reads
  - FSM transition writes (record_transition, record_imaging_transition)
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# Path translation
# =============================================================================

CANONICAL_PREFIX = "/arc/projects/ALMA-SAILS"
LOCAL_PREFIX     = "/home/ubuntu/canfar_arc/projects/ALMA-SAILS"


def _local_prefix() -> str:
    """Detect which path prefix applies in the current execution environment."""
    if os.path.exists(CANONICAL_PREFIX):
        return CANONICAL_PREFIX      # running on CANFAR (container or headless session)
    return LOCAL_PREFIX              # running on VM via sshfs mount


def to_local(path: str) -> str:
    """Translate a canonical /arc/... path to the locally-correct prefix."""
    return path.replace(CANONICAL_PREFIX, _local_prefix())


def to_canonical(path: str | Path) -> str:
    """Translate any project path to canonical /arc/... form for DB storage."""
    return str(path).replace(LOCAL_PREFIX, CANONICAL_PREFIX)


def _translate_path_field(value: str | None) -> Path | list[Path] | None:
    """Parse and translate a single path field or JSON array of paths from the DB."""
    if value is None:
        return None
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [Path(to_local(p)) for p in parsed]
    except (json.JSONDecodeError, TypeError):
        pass
    return Path(to_local(value))


# =============================================================================
# Connection management
# =============================================================================

def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Open a connection to the pipeline database.

    Parameters
    ----------
    db_path:
        Path to the SQLite database. Defaults to DB_PATH from config.
    """
    if db_path is None:
        from alma_ops.config import DB_PATH
        db_path = DB_PATH
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Context manager that commits on success and rolls back on any exception."""
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# =============================================================================
# Low-level primitives
# =============================================================================

def fetch_one(conn: sqlite3.Connection, query: str, params: tuple = ()) -> sqlite3.Row | None:
    return conn.execute(query, params).fetchone()


def fetch_all(conn: sqlite3.Connection, query: str, params: tuple = ()) -> list[sqlite3.Row]:
    return conn.execute(query, params).fetchall()


def parse_json_safe(value):
    """Parse a JSON string, returning the original value if it is not valid JSON."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def require_state(current_state: str, recoverable_states: set[str], label: str = "") -> None:
    """Print a standard error and exit(1) if current_state is not recoverable.

    Shared precondition check for reset_*.py recovery scripts, so the guard
    and its error message stay identical across all of them rather than each
    script re-implementing its own (slightly different) version.
    """
    if current_state in recoverable_states:
        return
    prefix = f"{label}: " if label else ""
    print(f"[ERROR] {prefix}expected state in {recoverable_states}, got '{current_state}' — aborting")
    sys.exit(1)


# =============================================================================
# MOUS reads
# =============================================================================

def get_mous(conn: sqlite3.Connection, mous_id: str) -> sqlite3.Row | None:
    """Fetch the mous table row for a given MOUS ID."""
    return fetch_one(conn, "SELECT * FROM mous WHERE mous_id=?", (mous_id,))


def get_pipeline_state(conn: sqlite3.Connection, mous_id: str) -> sqlite3.Row | None:
    """Fetch the pipeline_state row for a given MOUS ID."""
    return fetch_one(conn, "SELECT * FROM pipeline_state WHERE mous_id=?", (mous_id,))


def get_mous_targets(conn: sqlite3.Connection, mous_id: str) -> list[sqlite3.Row]:
    """Fetch all target rows for a given MOUS ID."""
    return fetch_all(conn, "SELECT * FROM targets WHERE mous_id=?", (mous_id,))


def get_unique_target_names(conn: sqlite3.Connection, mous_id: str) -> list[str]:
    """Return all unique target names for a given MOUS ID."""
    rows = fetch_all(
        conn,
        "SELECT DISTINCT alma_source_name FROM targets WHERE mous_id=?",
        (mous_id,),
    )
    return [r["alma_source_name"] for r in rows]


def get_mous_directory(conn: sqlite3.Connection, mous_id: str) -> Path | None:
    """Return the translated local path to the MOUS working directory."""
    row = get_pipeline_state(conn, mous_id)
    if row is None or row["mous_directory"] is None:
        return None
    return Path(to_local(row["mous_directory"]))


def get_split_products(conn: sqlite3.Connection, mous_id: str) -> list[Path]:
    """Return translated local paths to the split measurement sets."""
    row = get_pipeline_state(conn, mous_id)
    if row is None:
        return []
    result = _translate_path_field(row["split_products"])
    if result is None:
        return []
    return result if isinstance(result, list) else [result]


def get_selfcal_products(conn: sqlite3.Connection, mous_id: str, continuum_subtracted: bool = False) -> list[Path]:
    """Return translated local paths to selfcal output measurement sets."""
    row = get_pipeline_state(conn, mous_id)
    if row is None:
        return []
    col = "selfcal_products_sub" if continuum_subtracted else "selfcal_products"
    result = _translate_path_field(row[col])
    if result is None:
        return []
    return result if isinstance(result, list) else [result]


def get_mous_by_state(conn: sqlite3.Connection, state: str) -> list[sqlite3.Row]:
    """Return all pipeline_state rows currently in the given state."""
    return fetch_all(
        conn,
        "SELECT * FROM pipeline_state WHERE current_state=? ORDER BY mous_id",
        (state,),
    )


def get_mous_science_spws(conn: sqlite3.Connection, mous_id: str) -> list[int]:
    """Return the sorted list of science SPW numbers for a MOUS.

    SPW numbers are parsed from the obs_id suffix in alminer_metadata,
    which encodes them as e.g. 'uid://A001/X.../X....source.Name.spw.18'.
    These are the SPW indices in the calibrated (pre-split) measurement set.
    """
    rows = fetch_all(
        conn,
        "SELECT DISTINCT obs_id FROM alminer_metadata WHERE mous_id=?",
        (mous_id,),
    )
    _spw_re = re.compile(r"\.spw\.(\d+)$")
    spws = set()
    for row in rows:
        m = _spw_re.search(row["obs_id"] or "")
        if m:
            spws.add(int(m.group(1)))
    return sorted(spws)


# =============================================================================
# MOUS FSM write operations
# =============================================================================

def record_transition(
    conn: sqlite3.Connection,
    mous_id: str,
    from_state: str | None,
    to_state: str,
    run_id: str | None = None,
    note: str | None = None,
    payload: dict | None = None,
    step: str | None = None,
) -> int:
    """Atomically advance a MOUS to a new state and record the transition.

    Writes to state_transitions and updates pipeline_state in a single
    transaction. If a payload is provided, also writes a run_log entry.

    Parameters
    ----------
    mous_id:
        The MOUS being transitioned.
    from_state:
        The state being left. None for the initial seeded entry.
    to_state:
        The state being entered.
    run_id:
        Optional identifier tying log entries to a single pipeline run.
    note:
        Optional short note stored on the transition row.
    payload:
        Optional dict logged to run_log. Paths should be in canonical form.
    step:
        Label for the run_log entry. Defaults to to_state if not provided.

    Returns
    -------
    int
        The row ID of the new state_transitions entry.
    """
    now = datetime.now(timezone.utc).isoformat()
    effective_run_id = run_id or f"manual_{now}"

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO state_transitions (mous_id, from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (mous_id, from_state, to_state, now, effective_run_id, note),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE pipeline_state
            SET current_state=?, updated_at=?, last_transition_id=?
            WHERE mous_id=?
            """,
            (to_state, now, transition_id, mous_id),
        )

        if payload is not None:
            conn.execute(
                """
                INSERT INTO run_log (mous_id, run_id, step, at, payload, level)
                VALUES (?, ?, ?, ?, ?, 'INFO')
                """,
                (mous_id, effective_run_id, step or to_state, now, json.dumps(payload)),
            )

    return transition_id


def record_failure(
    conn: sqlite3.Connection,
    mous_id: str,
    from_state: str,
    error: str,
    run_id: str | None = None,
    step: str | None = None,
) -> int:
    """Transition a MOUS to the failed state and increment failure_count."""
    now = datetime.now(timezone.utc).isoformat()
    effective_run_id = run_id or f"manual_{now}"

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO state_transitions (mous_id, from_state, to_state, at, run_id, note)
            VALUES (?, ?, 'failed', ?, ?, ?)
            """,
            (mous_id, from_state, now, effective_run_id, step),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE pipeline_state
            SET current_state='failed', updated_at=?, last_transition_id=?,
                failure_count = failure_count + 1
            WHERE mous_id=?
            """,
            (now, transition_id, mous_id),
        )

        conn.execute(
            """
            INSERT INTO run_log (mous_id, run_id, step, at, payload, level)
            VALUES (?, ?, ?, ?, ?, 'ERROR')
            """,
            (mous_id, effective_run_id, step or "failed", now, json.dumps({"error": error})),
        )

    return transition_id


def update_pipeline_paths(
    conn: sqlite3.Connection,
    mous_id: str,
    **path_fields: str | list[str] | Path | list[Path],
) -> None:
    """Update path columns on pipeline_state, canonicalizing all values before storage.

    Accepts keyword arguments matching pipeline_state path columns:
    mous_directory, split_products, selfcal_products, selfcal_products_sub.
    Lists are serialized to JSON.
    """
    if not path_fields:
        return

    def canonicalize(v):
        if isinstance(v, list):
            return json.dumps([to_canonical(p) for p in v])
        return to_canonical(v)

    serialized = {k: canonicalize(v) for k, v in path_fields.items()}
    cols   = ", ".join(f"{k}=?" for k in serialized)
    values = list(serialized.values()) + [mous_id]

    with transaction(conn):
        conn.execute(f"UPDATE pipeline_state SET {cols} WHERE mous_id=?", values)


# =============================================================================
# Imaging reads
# =============================================================================

def get_imaging_unit(
    conn: sqlite3.Connection,
    target_name: str,
    band: str,
    config_type: str,
) -> sqlite3.Row | None:
    """Fetch the imaging_unit row for a given (target, band, config_type)."""
    return fetch_one(
        conn,
        "SELECT * FROM imaging_unit WHERE target_name=? AND band=? AND config_type=?",
        (target_name, band, config_type),
    )


def get_imaging_pipeline_state(
    conn: sqlite3.Connection, imaging_unit_id: int
) -> sqlite3.Row | None:
    """Fetch the imaging_pipeline_state row for a given imaging unit ID."""
    return fetch_one(
        conn,
        "SELECT * FROM imaging_pipeline_state WHERE imaging_unit_id=?",
        (imaging_unit_id,),
    )


def get_imaging_units_for_mous(conn: sqlite3.Connection, mous_id: str) -> list[sqlite3.Row]:
    """Return all imaging units that this MOUS contributes data to."""
    return fetch_all(
        conn,
        """
        SELECT iu.*
        FROM imaging_unit iu
        JOIN mous_imaging_unit miu ON iu.imaging_unit_id = miu.imaging_unit_id
        WHERE miu.mous_id=?
        ORDER BY iu.target_name, iu.band, iu.config_type
        """,
        (mous_id,),
    )


def get_mous_for_imaging_unit(conn: sqlite3.Connection, imaging_unit_id: int) -> list[sqlite3.Row]:
    """Return all MOUS pipeline_state rows contributing to a given imaging unit.

    Each row also carries miu.source_field_name and miu.field_ids (both
    usually NULL) -- the per-(mous, imaging_unit) CASA field selection
    override used when an imaging unit images only a subset of fields out
    of a shared-name mosaic MOUS. See imaging_utils.run_continuum_imaging.
    """
    return fetch_all(
        conn,
        """
        SELECT ps.*, miu.source_field_name, miu.field_ids
        FROM pipeline_state ps
        JOIN mous_imaging_unit miu ON ps.mous_id = miu.mous_id
        WHERE miu.imaging_unit_id=?
        ORDER BY ps.mous_id
        """,
        (imaging_unit_id,),
    )


def get_imaging_units_by_state(conn: sqlite3.Connection, state: str) -> list[sqlite3.Row]:
    """Return all imaging_pipeline_state rows currently in the given state."""
    return fetch_all(
        conn,
        """
        SELECT * FROM imaging_pipeline_state
        WHERE current_state=?
        ORDER BY target_name, band, config_type
        """,
        (state,),
    )


def is_imaging_unit_ready(conn: sqlite3.Connection, imaging_unit_id: int) -> bool:
    """Return True if all contributing MOUSes are selfcal_complete."""
    rows = get_mous_for_imaging_unit(conn, imaging_unit_id)
    if not rows:
        return False
    return all(r["current_state"] == "selfcal_complete" for r in rows)


# =============================================================================
# Imaging FSM write operations
# =============================================================================

def record_imaging_transition(
    conn: sqlite3.Connection,
    imaging_unit_id: int,
    from_state: str | None,
    to_state: str,
    run_id: str | None = None,
    note: str | None = None,
    payload: dict | None = None,
    step: str | None = None,
    imaging_products: dict | None = None,
) -> int:
    """Atomically advance an imaging unit to a new state and record the transition."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_imaging_pipeline_state(conn, imaging_unit_id)
    target_name = row["target_name"] if row else None
    band        = row["band"]        if row else None
    config_type = row["config_type"] if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO imaging_state_transitions
                (imaging_unit_id, target_name, band, config_type, from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (imaging_unit_id, target_name, band, config_type, from_state, to_state, now, run_id, note),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE imaging_pipeline_state
            SET current_state=?, updated_at=?, last_transition_id=?
            WHERE imaging_unit_id=?
            """,
            (to_state, now, transition_id, imaging_unit_id),
        )

        if imaging_products is not None:
            row = conn.execute(
                "SELECT imaging_products FROM imaging_pipeline_state WHERE imaging_unit_id=?",
                (imaging_unit_id,),
            ).fetchone()
            existing = json.loads(row[0]) if row and row[0] else []
            if isinstance(existing, dict):
                existing = [existing]
            existing.append(imaging_products)
            conn.execute(
                "UPDATE imaging_pipeline_state SET imaging_products=? WHERE imaging_unit_id=?",
                (json.dumps(existing), imaging_unit_id),
            )

        if payload is not None:
            conn.execute(
                """
                INSERT INTO imaging_run_log
                    (imaging_unit_id, target_name, band, config_type, run_id, step, at, payload, level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'INFO')
                """,
                (imaging_unit_id, target_name, band, config_type,
                 run_id, step or to_state, now, json.dumps(payload)),
            )

    return transition_id


def record_imaging_failure(
    conn: sqlite3.Connection,
    imaging_unit_id: int,
    from_state: str,
    error: str,
    run_id: str | None = None,
    step: str | None = None,
) -> int:
    """Transition an imaging unit to failed and increment failure_count."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_imaging_pipeline_state(conn, imaging_unit_id)
    target_name = row["target_name"] if row else None
    band        = row["band"]        if row else None
    config_type = row["config_type"] if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO imaging_state_transitions
                (imaging_unit_id, target_name, band, config_type, from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, 'failed', ?, ?, ?)
            """,
            (imaging_unit_id, target_name, band, config_type, from_state, now, run_id, step),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE imaging_pipeline_state
            SET current_state='failed', updated_at=?, last_transition_id=?,
                failure_count = failure_count + 1
            WHERE imaging_unit_id=?
            """,
            (now, transition_id, imaging_unit_id),
        )

        conn.execute(
            """
            INSERT INTO imaging_run_log
                (imaging_unit_id, target_name, band, config_type, run_id, step, at, payload, level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ERROR')
            """,
            (imaging_unit_id, target_name, band, config_type,
             run_id, step or "failed", now, json.dumps({"error": error})),
        )

    return transition_id


# =============================================================================
# Line-triage reads
# =============================================================================
#
# Line triage (ADMIT-bound dirty cubes) gets its own FSM, parallel to the
# imaging one, but keyed by (mous_id, target_name, spw_idx) rather than
# (target_name, band, config_type) — it runs per-MOUS, per-spw, since spw
# coverage is not guaranteed to match across different MOUSes of the same
# target (different projects/cycles routinely use different correlator setups).

def get_or_create_line_triage_unit(
    conn: sqlite3.Connection,
    mous_id: str,
    target_name: str,
    spw_idx: int,
) -> int:
    """Return the line_triage_unit_id for (mous_id, target_name, spw_idx), creating it (and its 'ready' pipeline_state row) if absent."""
    row = fetch_one(
        conn,
        "SELECT line_triage_unit_id FROM line_triage_unit WHERE mous_id=? AND target_name=? AND spw_idx=?",
        (mous_id, target_name, spw_idx),
    )
    if row is not None:
        return row["line_triage_unit_id"]

    now = datetime.now(timezone.utc).isoformat()
    with transaction(conn):
        cur = conn.execute(
            "INSERT INTO line_triage_unit (mous_id, target_name, spw_idx) VALUES (?, ?, ?)",
            (mous_id, target_name, spw_idx),
        )
        unit_id = cur.lastrowid
        conn.execute(
            """
            INSERT INTO line_triage_pipeline_state
                (line_triage_unit_id, mous_id, target_name, spw_idx, current_state, updated_at)
            VALUES (?, ?, ?, ?, 'ready', ?)
            """,
            (unit_id, mous_id, target_name, spw_idx, now),
        )
    return unit_id


def get_line_triage_pipeline_state(
    conn: sqlite3.Connection, line_triage_unit_id: int
) -> sqlite3.Row | None:
    """Fetch the line_triage_pipeline_state row for a given line_triage_unit_id."""
    return fetch_one(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE line_triage_unit_id=?",
        (line_triage_unit_id,),
    )


def get_line_triage_units_for_mous(
    conn: sqlite3.Connection, mous_id: str, target_name: str | None = None
) -> list[sqlite3.Row]:
    """Return all line_triage_pipeline_state rows for a MOUS, optionally filtered by target_name."""
    if target_name is not None:
        return fetch_all(
            conn,
            "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? ORDER BY spw_idx",
            (mous_id, target_name),
        )
    return fetch_all(
        conn,
        "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? ORDER BY target_name, spw_idx",
        (mous_id,),
    )


# =============================================================================
# Line-triage FSM write operations
# =============================================================================

def record_line_triage_transition(
    conn: sqlite3.Connection,
    line_triage_unit_id: int,
    from_state: str | None,
    to_state: str,
    run_id: str | None = None,
    note: str | None = None,
    payload: dict | None = None,
    step: str | None = None,
    triage_products: dict | None = None,
) -> int:
    """Atomically advance a line-triage unit to a new state and record the transition."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_line_triage_pipeline_state(conn, line_triage_unit_id)
    mous_id     = row["mous_id"]     if row else None
    target_name = row["target_name"] if row else None
    spw_idx     = row["spw_idx"]     if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO line_triage_state_transitions
                (line_triage_unit_id, mous_id, target_name, spw_idx, from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (line_triage_unit_id, mous_id, target_name, spw_idx, from_state, to_state, now, run_id, note),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE line_triage_pipeline_state
            SET current_state=?, updated_at=?, last_transition_id=?
            WHERE line_triage_unit_id=?
            """,
            (to_state, now, transition_id, line_triage_unit_id),
        )

        if triage_products is not None:
            row = conn.execute(
                "SELECT triage_products FROM line_triage_pipeline_state WHERE line_triage_unit_id=?",
                (line_triage_unit_id,),
            ).fetchone()
            existing = json.loads(row[0]) if row and row[0] else []
            if isinstance(existing, dict):
                existing = [existing]
            existing.append(triage_products)
            conn.execute(
                "UPDATE line_triage_pipeline_state SET triage_products=? WHERE line_triage_unit_id=?",
                (json.dumps(existing), line_triage_unit_id),
            )

        if payload is not None:
            conn.execute(
                """
                INSERT INTO line_triage_run_log
                    (line_triage_unit_id, mous_id, target_name, spw_idx, run_id, step, at, payload, level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'INFO')
                """,
                (line_triage_unit_id, mous_id, target_name, spw_idx,
                 run_id, step or to_state, now, json.dumps(payload)),
            )

    return transition_id


def record_line_triage_failure(
    conn: sqlite3.Connection,
    line_triage_unit_id: int,
    from_state: str,
    error: str,
    run_id: str | None = None,
    step: str | None = None,
) -> int:
    """Transition a line-triage unit to failed and increment failure_count."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_line_triage_pipeline_state(conn, line_triage_unit_id)
    mous_id     = row["mous_id"]     if row else None
    target_name = row["target_name"] if row else None
    spw_idx     = row["spw_idx"]     if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO line_triage_state_transitions
                (line_triage_unit_id, mous_id, target_name, spw_idx, from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, 'failed', ?, ?, ?)
            """,
            (line_triage_unit_id, mous_id, target_name, spw_idx, from_state, now, run_id, step),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE line_triage_pipeline_state
            SET current_state='failed', updated_at=?, last_transition_id=?,
                failure_count = failure_count + 1
            WHERE line_triage_unit_id=?
            """,
            (now, transition_id, line_triage_unit_id),
        )

        conn.execute(
            """
            INSERT INTO line_triage_run_log
                (line_triage_unit_id, mous_id, target_name, spw_idx, run_id, step, at, payload, level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ERROR')
            """,
            (line_triage_unit_id, mous_id, target_name, spw_idx,
             run_id, step or "failed", now, json.dumps({"error": error})),
        )

    return transition_id


# =============================================================================
# ADMIT line-identification FSM — one line_admit_unit per completed
# line_triage_unit (1:1, not a fan-out). mous_id/target_name/spw_idx are
# denormalized from the parent line_triage_unit for query convenience,
# matching the line-triage tables' own pattern.
# =============================================================================

def get_or_create_line_admit_unit(conn: sqlite3.Connection, line_triage_unit_id: int) -> int:
    """Return the line_admit_unit_id for a line_triage_unit_id, creating it
    (and its 'ready' pipeline_state row) if absent. The parent
    line_triage_unit must already exist and be 'complete'."""
    row = fetch_one(
        conn,
        "SELECT line_admit_unit_id FROM line_admit_unit WHERE line_triage_unit_id=?",
        (line_triage_unit_id,),
    )
    if row is not None:
        return row["line_admit_unit_id"]

    triage_row = get_line_triage_pipeline_state(conn, line_triage_unit_id)
    if triage_row is None:
        raise ValueError(f"No line_triage_unit_id={line_triage_unit_id} found")

    now = datetime.now(timezone.utc).isoformat()
    with transaction(conn):
        cur = conn.execute(
            "INSERT INTO line_admit_unit (line_triage_unit_id) VALUES (?)",
            (line_triage_unit_id,),
        )
        unit_id = cur.lastrowid
        conn.execute(
            """
            INSERT INTO line_admit_pipeline_state
                (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                 current_state, updated_at)
            VALUES (?, ?, ?, ?, ?, 'ready', ?)
            """,
            (unit_id, line_triage_unit_id, triage_row["mous_id"], triage_row["target_name"],
             triage_row["spw_idx"], now),
        )
    return unit_id


def get_line_admit_pipeline_state(conn: sqlite3.Connection, line_admit_unit_id: int) -> sqlite3.Row | None:
    """Fetch the line_admit_pipeline_state row for a given line_admit_unit_id."""
    return fetch_one(
        conn,
        "SELECT * FROM line_admit_pipeline_state WHERE line_admit_unit_id=?",
        (line_admit_unit_id,),
    )


def get_line_admit_unit_for_triage_unit(conn: sqlite3.Connection, line_triage_unit_id: int) -> sqlite3.Row | None:
    """Fetch the line_admit_pipeline_state row for a given parent line_triage_unit_id, if any."""
    return fetch_one(
        conn,
        "SELECT * FROM line_admit_pipeline_state WHERE line_triage_unit_id=?",
        (line_triage_unit_id,),
    )


def record_line_admit_transition(
    conn: sqlite3.Connection,
    line_admit_unit_id: int,
    from_state: str | None,
    to_state: str,
    run_id: str | None = None,
    note: str | None = None,
    payload: dict | None = None,
    step: str | None = None,
    admit_products: dict | None = None,
) -> int:
    """Atomically advance a line-admit unit to a new state and record the transition."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_line_admit_pipeline_state(conn, line_admit_unit_id)
    line_triage_unit_id = row["line_triage_unit_id"] if row else None
    mous_id              = row["mous_id"]              if row else None
    target_name           = row["target_name"]           if row else None
    spw_idx                = row["spw_idx"]                if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO line_admit_state_transitions
                (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                 from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
             from_state, to_state, now, run_id, note),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE line_admit_pipeline_state
            SET current_state=?, updated_at=?, last_transition_id=?
            WHERE line_admit_unit_id=?
            """,
            (to_state, now, transition_id, line_admit_unit_id),
        )

        if admit_products is not None:
            conn.execute(
                "UPDATE line_admit_pipeline_state SET admit_products=? WHERE line_admit_unit_id=?",
                (json.dumps(admit_products), line_admit_unit_id),
            )

        if payload is not None:
            conn.execute(
                """
                INSERT INTO line_admit_run_log
                    (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                     run_id, step, at, payload, level)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'INFO')
                """,
                (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                 run_id, step or to_state, now, json.dumps(payload)),
            )

    return transition_id


def record_line_admit_failure(
    conn: sqlite3.Connection,
    line_admit_unit_id: int,
    from_state: str,
    error: str,
    run_id: str | None = None,
    step: str | None = None,
) -> int:
    """Transition a line-admit unit to failed and increment failure_count."""
    now = datetime.now(timezone.utc).isoformat()

    row = get_line_admit_pipeline_state(conn, line_admit_unit_id)
    line_triage_unit_id = row["line_triage_unit_id"] if row else None
    mous_id              = row["mous_id"]              if row else None
    target_name           = row["target_name"]           if row else None
    spw_idx                = row["spw_idx"]                if row else None

    with transaction(conn):
        cur = conn.execute(
            """
            INSERT INTO line_admit_state_transitions
                (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                 from_state, to_state, at, run_id, note)
            VALUES (?, ?, ?, ?, ?, ?, 'failed', ?, ?, ?)
            """,
            (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
             from_state, now, run_id, step),
        )
        transition_id = cur.lastrowid

        conn.execute(
            """
            UPDATE line_admit_pipeline_state
            SET current_state='failed', updated_at=?, last_transition_id=?,
                failure_count = failure_count + 1
            WHERE line_admit_unit_id=?
            """,
            (now, transition_id, line_admit_unit_id),
        )

        conn.execute(
            """
            INSERT INTO line_admit_run_log
                (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
                 run_id, step, at, payload, level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ERROR')
            """,
            (line_admit_unit_id, line_triage_unit_id, mous_id, target_name, spw_idx,
             run_id, step or "failed", now, json.dumps({"error": error})),
        )

    return transition_id


def insert_line_admit_detections(
    conn: sqlite3.Connection, line_admit_unit_id: int, detections: list[dict],
) -> None:
    """Bulk-insert parsed lltable.N.json rows as line_admit_detection rows.

    Each dict is expected to have keys matching ADMIT's LineList table
    columns: frequency, uid, formula, name, transition, velocity, peakrms,
    peakintensity, startchan, endchan, blend (see alma_ops.imaging.line_admit_utils
    for the parser that produces these dicts from lltable.N.json)."""
    with transaction(conn):
        for d in detections:
            conn.execute(
                """
                INSERT INTO line_admit_detection
                    (line_admit_unit_id, frequency_ghz, uid, formula, species_name, transition,
                     velocity_kms, peakrms_sigma, peakintensity_jybeam, startchan, endchan, blend_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (line_admit_unit_id, d["frequency"], d["uid"], d.get("formula"), d.get("name"),
                 d.get("transition"), d.get("velocity"), d.get("peakrms"), d.get("peakintensity"),
                 d.get("startchan"), d.get("endchan"), d.get("blend")),
            )


def get_line_admit_detections(
    conn: sqlite3.Connection, line_admit_unit_id: int | None = None, review_status: str | None = None,
) -> list[sqlite3.Row]:
    """Fetch line_admit_detection rows, optionally filtered by unit and/or review_status."""
    query = "SELECT * FROM line_admit_detection WHERE 1=1"
    params: list = []
    if line_admit_unit_id is not None:
        query += " AND line_admit_unit_id=?"
        params.append(line_admit_unit_id)
    if review_status is not None:
        query += " AND review_status=?"
        params.append(review_status)
    query += " ORDER BY line_admit_unit_id, frequency_ghz"
    return fetch_all(conn, query, tuple(params))


def update_detection_review(
    conn: sqlite3.Connection, line_admit_detection_id: int, review_status: str, note: str | None = None,
) -> None:
    """Set review_status ('confirmed' | 'rejected' | 'pending') on one detection."""
    now = datetime.now(timezone.utc).isoformat()
    with transaction(conn):
        conn.execute(
            """
            UPDATE line_admit_detection
            SET review_status=?, reviewed_at=?, reviewed_note=?
            WHERE line_admit_detection_id=?
            """,
            (review_status, now, note, line_admit_detection_id),
        )


def get_line_admit_detection(conn: sqlite3.Connection, line_admit_detection_id: int) -> sqlite3.Row | None:
    """Fetch a single line_admit_detection row by id."""
    return fetch_one(
        conn,
        "SELECT * FROM line_admit_detection WHERE line_admit_detection_id=?",
        (line_admit_detection_id,),
    )


