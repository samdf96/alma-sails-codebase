"""
db.py
--------------------
Database operations for ALMA MOUS and pipeline state management.
"""

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------

import json
import re
import sqlite3
from contextlib import contextmanager

# =====================================================================
# Fetching and executing database operations
# =====================================================================


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Establishes a connection to a database.

    Parameters
    ----------
    db_path : str
        Path to the sqlite database.

    Returns
    -------
    sqlite3.Connection
        An open connection to the on-disk database.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_transaction(conn: sqlite3.Connection):
    """Commits any pending transactions to the database.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    """
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def db_fetch_one(
    conn: sqlite3.Connection, query: str, params: tuple = ()
) -> sqlite3.Row | None:
    """Fetches a single row, given the query and parameters.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    query : str
        The SQL query to execute.
    params : tuple, optional
        Parameters to substitute into the SQL query, by default ().

    Returns
    -------
    sqlite3.Row | None
        A single row from the query result, or None if no rows match.
    """
    cur = conn.execute(query, params)
    return cur.fetchone()


def db_fetch_all(
    conn: sqlite3.Connection, query: str, params: tuple = ()
) -> list[sqlite3.Row]:
    """Fetches all rows, given the query and parameters.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    query : str
        The SQL query to execute.
    params : tuple, optional
        Parameters to substitute into the SQL query, by default ().

    Returns
    -------
    list
        A list of all the sqlite3.Row objects from the query result.
    """
    cur = conn.execute(query, params)
    return cur.fetchall()


def db_execute(
    conn: sqlite3.Connection, query: str, params: tuple = (), commit: bool = False
):
    """Executes the SQL query.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    query : str
        The SQL query to execute.
    params : tuple, optional
        Parameters to substitute into the SQL query, by default ().
    commit : bool, optional
        Whether to commit the transaction, by default False
    """
    conn.execute(query, params)
    if commit:
        conn.commit()


def parse_json_safe(value):
    """Parses a JSON string safely, returning None if the input is None,
    or returning the original value if it is not valid JSON.
    """
    if value is None:
        return None

    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


# =====================================================================
# Record Getters/Setters
# =====================================================================


def get_mous_record(conn: sqlite3.Connection, mous_id: str) -> sqlite3.Row | None:
    """Fetches the row from the mous table for a given mous_id.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    sqlite3.Row | None
        A single row from the mous table, or None if no rows match.
    """
    row = db_fetch_one(
        conn,
        "SELECT * FROM mous WHERE mous_id=?",
        (mous_id,),
    )
    return row if row else None


def get_pipeline_state_record(
    conn: sqlite3.Connection, mous_id: str
) -> sqlite3.Row | None:
    """Fetches the row from the pipeline_state table for a given mous_id.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    sqlite3.Row | None
        A single row from the pipeline_state table, or None if no rows match.
    """
    row = db_fetch_one(
        conn,
        "SELECT * FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )
    return row if row else None


def update_pipeline_state_record(conn: sqlite3.Connection, mous_id: str, **fields):
    """Updates one or more fields in the pipeline_state table.
    Automatically JSON-serializes lists and dictionaries for TEXT-based fields.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.
    **fields
        Key-value pairs of fields to update.
    """
    if not fields:
        return

    # serialize lists and dicts to JSON strings
    serialized_fields = {
        k: json.dumps(v) if isinstance(v, (list, dict)) else v
        for k, v in fields.items()
    }

    cols = ", ".join(f"{k}=?" for k in serialized_fields)
    values = list(serialized_fields.values()) + [mous_id]

    with db_transaction(conn):
        conn.execute(f"UPDATE pipeline_state SET {cols} WHERE mous_id=?", values)


def update_mous_record(conn: sqlite3.Connection, mous_id: str, **fields):
    """Updates one or more fields in the mous table.
    Automatically JSON-serializes lists and dictionaries for TEXT-based fields.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.
    **fields
        Key-value pairs of fields to update.
    """
    if not fields:
        return

    # serialize lists and dicts to JSON strings
    serialized_fields = {
        k: json.dumps(v) if isinstance(v, (list, dict)) else v
        for k, v in fields.items()
    }

    cols = ", ".join(f"{k}=?" for k in serialized_fields)
    values = list(serialized_fields.values()) + [mous_id]

    with db_transaction(conn):
        conn.execute(f"UPDATE mous SET {cols} WHERE mous_id=?", values)


def get_pipeline_state_record_column_value(
    conn: sqlite3.Connection, mous_id: str, column: str
) -> dict | list | None:
    """Fetches a column value from the pipeline_state table for a given mous_id.
    Attemps a json.loads() to parse the information from the specified column to catch
    any JSON-valid data, or returns the raw value if not JSON.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.
    column : str
        The column name to fetch.

    Returns
    -------
    The parsed JSON object from the specified column, or None if no rows match.

    Raises
    ------
    ValueError
        If the specified column contains invalid JSON.
    """
    row = get_pipeline_state_record(conn, mous_id)

    if not row:
        return None

    raw = row[column]

    return parse_json_safe(raw)


# =====================================================================
# Getters
# =====================================================================


def get_mous_targets(conn: sqlite3.Connection, mous_id: str) -> list[sqlite3.Row]:
    """Fetches all target rows for a given mous_id.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    list[sqlite3.Row]
        A list of all target rows for the given mous_id.
    """
    return db_fetch_all(conn, "SELECT * FROM targets WHERE mous_id=?", (mous_id,))


def get_unique_target_names(conn: sqlite3.Connection, mous_id: str) -> list[str]:
    """Fetches all unique target names for a given mous_id.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    list[str]
        A list of unique target names for the given mous_id.
    """
    rows = db_fetch_all(
        conn,
        "SELECT DISTINCT alma_source_name FROM targets WHERE mous_id=?",
        (mous_id,),
    )
    return [r["alma_source_name"] for r in rows]


def get_mous_asdms_from_targets(conn: sqlite3.Connection, mous_id: str) -> list[str]:
    """Fetches all unique ASDM UIDs from the targets table for a given mous_id.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    list[str]
        A list of unique ASDM UIDs for the given mous_id.
    """
    rows = db_fetch_all(
        conn, "SELECT DISTINCT asdm_uid FROM targets WHERE mous_id=?", (mous_id,)
    )
    return [r["asdm_uid"] for r in rows if r["asdm_uid"]]


def get_mous_spw_mapping(
    conn: sqlite3.Connection, mous_id: str
) -> dict[str, list[int]]:
    """Constructs a mapping of source names to their associated spectral windows (SPWs)
    for a given mous_id.

    Will apply any user-defined SPW remapping if present.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    dict[str, list[int]]
        A dictionary mapping source names to lists of associated SPWs.
    """
    rows = db_fetch_all(
        conn, "SELECT alma_source_name, obs_id FROM targets WHERE mous_id=?", (mous_id,)
    )

    # setting spw pattern matching
    spw_pattern = re.compile(r"\.spw\.(\d+)$")
    spw_map = {}

    # checks if any user-defined spw remapping is set for the MOUS
    spw_remap = get_spw_remap(conn, mous_id)

    for source, obs_id in rows:
        m = spw_pattern.search(obs_id or "")

        if not m:
            continue

        spw = int(m.group(1))

        # remap if needed
        if spw_remap and spw in spw_remap:
            spw = spw_remap[spw]

        spw_map.setdefault(source, set()).add(spw)

    return {k: sorted(v) for k, v in spw_map.items()}


def get_spw_remap(conn: sqlite3.Connection, mous_id: str) -> dict | None:
    """Fetches the SPW remapping dictionary for a given mous_id, if it exists.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open connection to the on-disk database.
    mous_id : str
        The MOUS ID to query.

    Returns
    -------
    dict | None
        A dictionary mapping original SPW integers to remapped SPW integers,
        or None if no remapping is defined.

    Raises
    ------
    ValueError
        If the raw_data_spectral_remap entry contains invalid JSON.
    """
    row = db_fetch_one(
        conn,
        "SELECT raw_data_spectral_remap FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )

    if not row:
        return None

    value = row["raw_data_spectral_remap"]
    if value is None:
        return None

    try:
        raw = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON in raw_data_spectral_remap") from e

    return {int(k): int(v) for k, v in raw.items()}
