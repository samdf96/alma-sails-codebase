# alma_ops/db.py
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime


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
# MOUS getters
# ---------------------


def get_mous_record(conn, mous_id: str):
    return db_fetch_one(conn, "SELECT * FROM mous WHERE mous_id=?", (mous_id,))


def get_mous_download_url(conn, mous_id: str) -> str:
    row = db_fetch_one(
        conn, "SELECT download_url FROM mous WHERE mous_id=?", (mous_id,)
    )
    return row["download_url"] if row else None


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


def get_mous_spw_mapping(conn, mous_id: str) -> dict:
    rows = db_fetch_all(
        conn, "SELECT alma_source_name, obs_id FROM targets WHERE mous_id=?", (mous_id,)
    )
    spw_pattern = re.compile(r"\.spw\.(\d+)$")
    spw_map = {}
    for source, obs_id in rows:
        m = spw_pattern.search(obs_id or "")
        if m:
            spw_map.setdefault(source, set()).add(int(m.group(1)))
    return {k: sorted(v) for k, v in spw_map.items()}
