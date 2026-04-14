"""
tests/test_flows/conftest.py
-----------------------------
Fixtures for Layer 2 (flow integration) tests.

DualPath strategy
-----------------
Flow code constructs DualPath objects from paths that normally must start with
either VM_MOUNT_PREFIX or PLATFORM_PREFIX.  In tests we monkeypatch both
constants to "/" so that DualPath accepts any absolute path and becomes a
no-op passthrough:

    DualPath("/tmp/pytest/.../test.db").platform  →  "/tmp/pytest/.../test.db"
    DualPath("/tmp/pytest/.../test.db").vm        →  Path("/tmp/pytest/.../test.db")

Session mocking
---------------
Actual headless session launches are blocked by patching
``canfar.sessions.Session.create`` with unittest.mock.patch.
"""

import json
import sqlite3

import pytest

from alma_ops.utils import to_dir_mous_id

# Reuse the shared test MOUS ID defined in the top-level conftest
from conftest import TEST_MOUS_ID

# ---------------------------------------------------------------------------
# Minimal DB schema for flow tests (only the columns the flows actually touch)
# ---------------------------------------------------------------------------

_DB_SCHEMA = """
    PRAGMA foreign_keys = OFF;

    CREATE TABLE mous (
        mous_id TEXT PRIMARY KEY
    );

    CREATE TABLE pipeline_state (
        mous_id                   TEXT PRIMARY KEY,
        download_status           TEXT DEFAULT 'pending',
        download_started_at       TEXT,
        download_completed_at     TEXT,
        pre_selfcal_split_status  TEXT DEFAULT 'pending',
        pre_selfcal_listobs_status TEXT DEFAULT 'pending',
        selfcal_status            TEXT DEFAULT 'pending',
        selfcal_started_at        TEXT,
        selfcal_completed_at      TEXT,
        mous_directory            TEXT DEFAULT '[]',
        calibrated_products       TEXT DEFAULT '[]',
        split_products_path       TEXT DEFAULT '[]',
        preferred_datacolumn      TEXT,
        raw_data_spectral_remap   TEXT
    );

    CREATE TABLE targets (
        target_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        mous_id          TEXT,
        alma_source_name TEXT,
        obs_id           TEXT
    );
"""


def _make_db(db_path, mous_dir):
    """Create the test database with a single MOUS row."""
    conn = sqlite3.connect(db_path)
    conn.executescript(_DB_SCHEMA)
    conn.execute("INSERT INTO mous (mous_id) VALUES (?)", (TEST_MOUS_ID,))
    conn.execute(
        "INSERT INTO pipeline_state (mous_id, mous_directory) VALUES (?, ?)",
        (TEST_MOUS_ID, str(mous_dir)),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Base fixture: monkeypatched DualPath + directory tree
# ---------------------------------------------------------------------------


@pytest.fixture
def flow_env(tmp_path, monkeypatch):
    """Base fixture for all flow integration tests.

    Monkeypatches VM_MOUNT_PREFIX and PLATFORM_PREFIX to "/" so DualPath acts
    as a passthrough for any absolute path.  Creates a tmp_path-rooted
    directory tree and SQLite database.

    Yields a dict:
        db_path       – str path to the test database
        datasets_dir  – str path to the datasets root
        mous_dir      – Path to the per-MOUS directory
    """
    monkeypatch.setattr("alma_ops.config.VM_MOUNT_PREFIX", "/")
    monkeypatch.setattr("alma_ops.config.PLATFORM_PREFIX", "/")

    datasets_dir = tmp_path / "datasets"
    mous_dir = datasets_dir / to_dir_mous_id(TEST_MOUS_ID)
    mous_dir.mkdir(parents=True)

    db_path = tmp_path / "test.db"
    _make_db(str(db_path), mous_dir)

    return {
        "db_path": str(db_path),
        "datasets_dir": str(datasets_dir),
        "mous_dir": mous_dir,
    }


# ---------------------------------------------------------------------------
# Per-flow state fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def split_flow_env(flow_env):
    """DB state ready for split_mous_flow: download complete, split pending.

    Also inserts two targets with SPW obs_ids so get_mous_spw_mapping works.
    """
    db_path = flow_env["db_path"]
    mous_dir = flow_env["mous_dir"]
    cal_path = str(mous_dir / "calibrated.ms")

    conn = sqlite3.connect(db_path)
    conn.execute(
        """UPDATE pipeline_state
           SET download_status=?, pre_selfcal_split_status=?, calibrated_products=?
           WHERE mous_id=?""",
        ("complete", "pending", json.dumps([cal_path]), TEST_MOUS_ID),
    )
    conn.executemany(
        "INSERT INTO targets (mous_id, obs_id) VALUES (?, ?)",
        [
            (TEST_MOUS_ID, "uid://A002/Xabc/X1.spw.17"),
            (TEST_MOUS_ID, "uid://A002/Xabc/X2.spw.19"),
        ],
    )
    conn.commit()
    conn.close()
    return flow_env


@pytest.fixture
def listobs_flow_env(flow_env):
    """DB state ready for post_split_listobs_flow: split complete, listobs pending."""
    db_path = flow_env["db_path"]
    mous_dir = flow_env["mous_dir"]
    cal_path = str(mous_dir / "calibrated.ms")
    split_path = str(mous_dir / "splits" / "calibrated_targets.ms")

    conn = sqlite3.connect(db_path)
    conn.execute(
        """UPDATE pipeline_state
           SET pre_selfcal_split_status=?, pre_selfcal_listobs_status=?,
               calibrated_products=?, split_products_path=?
           WHERE mous_id=?""",
        (
            "complete",
            "pending",
            json.dumps([cal_path]),
            json.dumps([split_path]),
            TEST_MOUS_ID,
        ),
    )
    conn.commit()
    conn.close()
    return flow_env


@pytest.fixture
def autoselfcal_prep_flow_env(flow_env):
    """DB state ready for autoselfcal_prep_flow: listobs complete, selfcal pending."""
    db_path = flow_env["db_path"]
    mous_dir = flow_env["mous_dir"]
    split_path = str(mous_dir / "splits" / "calibrated_targets.ms")

    conn = sqlite3.connect(db_path)
    conn.execute(
        """UPDATE pipeline_state
           SET pre_selfcal_listobs_status=?, selfcal_status=?, split_products_path=?
           WHERE mous_id=?""",
        ("complete", "pending", json.dumps([split_path]), TEST_MOUS_ID),
    )
    conn.commit()
    conn.close()
    return flow_env


@pytest.fixture
def autoselfcal_flow_env(flow_env):
    """DB state ready for autoselfcal_mous_flow: selfcal_status='prepped'.

    Also creates the auto_selfcal directory on disk (the flow asserts it exists).
    """
    db_path = flow_env["db_path"]
    mous_dir = flow_env["mous_dir"]

    autoselfcal_dir = mous_dir / "auto_selfcal"
    autoselfcal_dir.mkdir()

    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE pipeline_state SET selfcal_status=? WHERE mous_id=?",
        ("prepped", TEST_MOUS_ID),
    )
    conn.commit()
    conn.close()
    return flow_env