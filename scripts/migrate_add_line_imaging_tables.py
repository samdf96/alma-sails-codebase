"""
migrate_add_line_imaging_tables.py
------------------------------------
One-off migration: adds the deep line-imaging FSM tables (one run per
human-confirmed line_admit_detection, producing a real niter>0 clean of that
specific line) to serpens_v2.db.

Run once from the VM shell:

    python alma-sails-codebase/scripts/migrate_add_line_imaging_tables.py

Idempotent — uses CREATE TABLE IF NOT EXISTS, safe to re-run.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH

DDL = """
CREATE TABLE IF NOT EXISTS line_imaging_unit (
    line_imaging_unit_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_detection_id INTEGER NOT NULL UNIQUE REFERENCES line_admit_detection(line_admit_detection_id)
);

CREATE TABLE IF NOT EXISTS line_imaging_pipeline_state (
    line_imaging_unit_id    INTEGER PRIMARY KEY REFERENCES line_imaging_unit(line_imaging_unit_id),
    line_admit_detection_id INTEGER NOT NULL,
    mous_id                 TEXT NOT NULL,
    target_name             TEXT NOT NULL,
    spw_idx                 INTEGER NOT NULL,
    species_name            TEXT,
    frequency_ghz            REAL NOT NULL,
    current_state             TEXT NOT NULL DEFAULT 'ready',
    updated_at                  TEXT NOT NULL,
    job_id                        TEXT,
    failure_count                   INTEGER NOT NULL DEFAULT 0,
    last_transition_id                INTEGER REFERENCES line_imaging_state_transitions(id),
    imaging_products                    TEXT
);

CREATE TABLE IF NOT EXISTS line_imaging_state_transitions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    line_imaging_unit_id     INTEGER NOT NULL,
    line_admit_detection_id  INTEGER NOT NULL,
    mous_id                  TEXT NOT NULL,
    target_name              TEXT NOT NULL,
    spw_idx                  INTEGER NOT NULL,
    species_name             TEXT,
    frequency_ghz             REAL,
    from_state                  TEXT,
    to_state                      TEXT NOT NULL,
    at                              TEXT NOT NULL,
    run_id                            TEXT,
    note                                TEXT
);

CREATE TABLE IF NOT EXISTS line_imaging_run_log (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    line_imaging_unit_id     INTEGER NOT NULL,
    line_admit_detection_id  INTEGER NOT NULL,
    mous_id                  TEXT NOT NULL,
    target_name              TEXT NOT NULL,
    spw_idx                  INTEGER NOT NULL,
    species_name             TEXT,
    frequency_ghz             REAL,
    run_id                       TEXT NOT NULL,
    step                            TEXT NOT NULL,
    at                                TEXT NOT NULL,
    payload                             TEXT,
    level                                 TEXT NOT NULL DEFAULT 'INFO'
);
"""


def main() -> None:
    conn = db.get_connection(DB_PATH)
    conn.executescript(DDL)
    conn.commit()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'line_imaging_%'"
    ).fetchall()
    conn.close()
    print(f"[OK] line-imaging tables present: {[t['name'] for t in tables]}")


if __name__ == "__main__":
    main()
