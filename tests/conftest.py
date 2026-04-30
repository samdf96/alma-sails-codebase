"""
conftest.py
-----------
Shared fixtures for the alma-sails-codebase test suite.
"""

import json
import sqlite3

import pytest


# A stable, fake MOUS ID used across all tests that need one.
TEST_MOUS_ID = "uid://A001/X1234/Xtest"


@pytest.fixture
def test_db(tmp_path):
    """Minimal SQLite database with mous, pipeline_state, and targets tables.

    Inserts one MOUS row (TEST_MOUS_ID) into both mous and pipeline_state so
    foreign-key relationships are satisfied.  Tables only contain the columns
    required by the functions under test — additional columns can be added here
    if future tests need them.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        PRAGMA foreign_keys = OFF;

        CREATE TABLE mous (
            mous_id TEXT PRIMARY KEY
        );

        CREATE TABLE pipeline_state (
            mous_id                  TEXT PRIMARY KEY,
            download_status          TEXT DEFAULT 'pending',
            pre_selfcal_split_status TEXT DEFAULT 'pending',
            selfcal_status           TEXT DEFAULT 'pending',
            mous_directory           TEXT DEFAULT '[]',
            calibrated_products      TEXT DEFAULT '[]',
            split_products_path      TEXT DEFAULT '[]',
            preferred_datacolumn     TEXT,
            raw_data_spectral_remap  TEXT,
            FOREIGN KEY (mous_id) REFERENCES mous(mous_id)
        );

        CREATE TABLE targets (
            target_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            mous_id           TEXT,
            alma_source_name  TEXT,
            obs_id            TEXT,
            asdm_uid          TEXT
        );
    """)

    conn.execute("INSERT INTO mous (mous_id) VALUES (?)", (TEST_MOUS_ID,))
    conn.execute(
        "INSERT INTO pipeline_state (mous_id) VALUES (?)", (TEST_MOUS_ID,)
    )
    conn.commit()
    conn.close()

    return str(db_path)


@pytest.fixture
def test_db_with_targets(test_db):
    """Extends test_db with a set of target rows for SPW mapping tests.

    Inserts four targets whose obs_id values encode SPW numbers 17, 19, 21, 25
    via the ``.spw.<N>`` suffix pattern used by get_mous_spw_mapping.
    """
    conn = sqlite3.connect(test_db)
    targets = [
        (TEST_MOUS_ID, "Serp_SMM1", "uid://A002/Xabc/X1.spw.17", "uid://A002/Xabc/X1"),
        (TEST_MOUS_ID, "Serp_SMM1", "uid://A002/Xabc/X2.spw.19", "uid://A002/Xabc/X1"),
        (TEST_MOUS_ID, "Serp_SMM1", "uid://A002/Xabc/X3.spw.21", "uid://A002/Xabc/X1"),
        (TEST_MOUS_ID, "Serp_SMM1", "uid://A002/Xabc/X4.spw.25", "uid://A002/Xabc/X1"),
    ]
    conn.executemany(
        "INSERT INTO targets (mous_id, alma_source_name, obs_id, asdm_uid) VALUES (?, ?, ?, ?)",
        targets,
    )
    conn.commit()
    conn.close()
    return test_db