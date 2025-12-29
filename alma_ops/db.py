# alma_ops/db.py
import json
import re
import sqlite3
from contextlib import contextmanager


def get_db_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_transaction(conn):
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def db_fetch_one(conn, query, params=()):
    cur = conn.execute(query, params)
    return cur.fetchone()


def db_fetch_all(conn, query, params=()):
    cur = conn.execute(query, params)
    return cur.fetchall()


def db_execute(conn, query, params=(), commit=False):
    conn.execute(query, params)
    if commit:
        conn.commit()


# ---------------------
# mous and pipeline state getters
# ---------------------


def get_mous_record(conn, mous_id: str):
    return db_fetch_one(conn, "SELECT * FROM mous WHERE mous_id=?", (mous_id,))


def get_pipeline_state_record(conn, mous_id: str):
    return db_fetch_one(
        conn, "SELECT * FROM pipeline_state WHERE mous_id=?", (mous_id,)
    )


def get_pipeline_state_download_url(conn, mous_id: str) -> str:
    row = db_fetch_one(
        conn, "SELECT download_url FROM pipeline_state WHERE mous_id=?", (mous_id,)
    )
    return row["download_url"] if row else None


def get_pipeline_state_mous_directory(conn, mous_id: str) -> str:
    row = db_fetch_one(
        conn, "SELECT mous_directory FROM pipeline_state WHERE mous_id=?", (mous_id,)
    )
    return row["mous_directory"] if row else None


def get_pipeline_state_calibrated_products(
    conn, mous_id: str
) -> str | list[str] | None:
    row = db_fetch_one(
        conn,
        "SELECT calibrated_products FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )
    if row and row["calibrated_products"]:
        try:
            return json.loads(row["calibrated_products"])
        except json.JSONDecodeError:
            return row["calibrated_products"]
    return None


def get_mous_targets(conn, mous_id: str):
    return db_fetch_all(conn, "SELECT * FROM targets WHERE mous_id=?", (mous_id,))


def get_unique_target_names(conn, mous_id: str):
    rows = db_fetch_all(
        conn,
        "SELECT DISTINCT alma_source_name FROM targets WHERE mous_id=?",
        (mous_id,),
    )
    return [r["alma_source_name"] for r in rows]


def get_mous_asdms_from_targets(conn, mous_id: str):
    rows = db_fetch_all(
        conn, "SELECT DISTINCT asdm_uid FROM targets WHERE mous_id=?", (mous_id,)
    )
    return [r["asdm_uid"] for r in rows if r["asdm_uid"]]


def get_mous_expected_asdms(conn, mous_id: str) -> int:
    row = db_fetch_one(conn, "SELECT num_asdms FROM mous WHERE mous_id=?", (mous_id,))
    if row is None or row["num_asdms"] is None:
        return 0
    return int(row["num_asdms"])


def get_mous_spw_mapping(conn, mous_id: str) -> dict[str, list[int]]:
    rows = db_fetch_all(
        conn, "SELECT alma_source_name, obs_id FROM targets WHERE mous_id=?", (mous_id,)
    )

    # setting spw pattern matching
    spw_pattern = re.compile(r"\.spw\.(\d+)$")
    spw_map = {}

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


def get_spw_remap(conn, mous_id: str) -> dict:
    row = db_fetch_one(
        conn,
        "SELECT raw_data_spectral_remap FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )

    if not row:
        return None

    # check if remap exists, else return None
    if row["raw_data_spectral_remap"] is not None:
        try:
            raw = json.loads(row["raw_data_spectral_remap"])
            return {int(k): int(v) for k, v in raw.items()}

        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in raw_data_spectral_remap")

    else:
        raw = None


def get_pipeline_state_preferred_datacolumn(conn, mous_id: str):
    row = db_fetch_one(
        conn,
        "SELECT preferred_datacolumn FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )

    if not row:
        raise ValueError("MOUS ID not found in pipeline_state table")

    # return the preferred_datacolumn value
    return row["preferred_datacolumn"]


def get_pipeline_state_split_products_path(
    conn, mous_id: str
) -> str | list[str] | None:
    row = db_fetch_one(
        conn,
        "SELECT split_products_path FROM pipeline_state WHERE mous_id=?",
        (mous_id,),
    )
    if row and row["split_products_path"]:
        try:
            return json.loads(row["split_products_path"])
        except json.JSONDecodeError:
            return row["split_products_path"]
    return None


# ---------------------
# mous and pipeline state setters
# ---------------------


def set_download_status_pending(conn, mous_id: str):
    """Sets only the status to pending."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET download_status=?
            WHERE mous_id=?
        """,
            ("pending", mous_id),
        )


def set_download_status_error(conn, mous_id: str):
    """Sets only the status to error."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET download_status=?
            WHERE mous_id=?
        """,
            ("error", mous_id),
        )


def set_download_status_in_progress(conn, mous_id: str, timestamp=False):
    """Sets only the status to in_progress (with optional timestamp)."""

    if timestamp:
        with db_transaction(conn):
            conn.execute(
                """
                UPDATE pipeline_state 
                SET download_status=?,
                download_started_at = CURRENT_TIMESTAMP
                WHERE mous_id=?
            """,
                ("in_progress", mous_id),
            )
    else:
        with db_transaction(conn):
            conn.execute(
                """
                UPDATE pipeline_state 
                SET download_status=?
                WHERE mous_id=?
            """,
                ("in_progress", mous_id),
            )


def set_download_status_complete(conn, mous_id: str, timestamp=False):
    """Sets only the status to complete (with optional timestamp)."""

    if timestamp:
        with db_transaction(conn):
            conn.execute(
                """
                UPDATE pipeline_state 
                SET download_status=?,
                download_completed_at = CURRENT_TIMESTAMP
                WHERE mous_id=?
            """,
                ("complete", mous_id),
            )
    else:
        with db_transaction(conn):
            conn.execute(
                """
                UPDATE pipeline_state 
                SET download_status=?
                WHERE mous_id=?
            """,
                ("complete", mous_id),
            )


def set_calibrated_products(conn, mous_id: str, calibrated_products: str | list):
    """Sets the calibrated products entry for a given mous_id."""

    # Convert to JSON string if it's a list
    if isinstance(calibrated_products, list):
        products_str = json.dumps(calibrated_products)
    else:
        products_str = calibrated_products

    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET calibrated_products=?
            WHERE mous_id=?
        """,
            (products_str, mous_id),
        )


def set_mous_directory(conn, mous_id: str, mous_directory: str):
    """Sets the mous directory entry for a given mous_id."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET mous_directory=?
            WHERE mous_id=?
        """,
            (mous_directory, mous_id),
        )


def set_pre_selfcal_split_status_pending(conn, mous_id: str):
    """Sets only the status to pending."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_split_status=?
            WHERE mous_id=?
        """,
            ("pending", mous_id),
        )


def set_pre_selfcal_split_status_error(conn, mous_id: str):
    """Sets only the status to error."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_split_status=?
            WHERE mous_id=?
        """,
            ("error", mous_id),
        )


def set_pre_selfcal_split_status_in_progress(conn, mous_id: str):
    """Sets only the status to in_progress."""

    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_split_status=?
            WHERE mous_id=?
        """,
            ("in_progress", mous_id),
        )


def set_pre_selfcal_split_status_complete(conn, mous_id: str):
    """Sets only the status to complete."""

    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_split_status=?
            WHERE mous_id=?
        """,
            ("complete", mous_id),
        )


def set_pre_selfcal_listobs_status_pending(conn, mous_id: str):
    """Sets only the status to pending."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_listobs_status=?
            WHERE mous_id=?
        """,
            ("pending", mous_id),
        )


def set_pre_selfcal_listobs_status_error(conn, mous_id: str):
    """Sets only the status to error."""
    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_listobs_status=?
            WHERE mous_id=?
        """,
            ("error", mous_id),
        )


def set_pre_selfcal_listobs_status_in_progress(conn, mous_id: str):
    """Sets only the status to in_progress."""

    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_listobs_status=?
            WHERE mous_id=?
        """,
            ("in_progress", mous_id),
        )


def set_pre_selfcal_listobs_status_complete(conn, mous_id: str):
    """Sets only the status to complete."""

    with db_transaction(conn):
        conn.execute(
            """
            UPDATE pipeline_state 
            SET pre_selfcal_listobs_status=?
            WHERE mous_id=?
        """,
            ("complete", mous_id),
        )
