"""
migrate_add_scouse_tables.py
-----------------------------
One-off migration: adds the SCOUSE + ACORNS FSM tables to serpens_v2.db.

Run once from the VM shell:

    python alma-sails-codebase/scripts/migrate_add_scouse_tables.py

Idempotent — uses CREATE TABLE IF NOT EXISTS, safe to re-run.

Schema overview
---------------
scouse_unit
    One row per (source_name, line_label) pair to be decomposed. Keyed by a
    natural composite unique constraint rather than a foreign key to a single
    parent table — a SCOUSE unit may draw on a confirmed line imaging product
    but the exact product path is recorded directly rather than via an FK.

scouse_pipeline_state
    FSM state for each unit. Stores parameters (JSON), component / cluster
    counts, and the canonical path to the ACORNS output file.

    States: pending → prep_complete → stage1_complete → stage2_complete →
            stage3_complete → stage4_complete → acorns_complete → complete
    Any state can also transition to failed.

scouse_state_transitions
    Full audit log of every state change with timestamp and optional note.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH

DDL = """
CREATE TABLE IF NOT EXISTS scouse_unit (
    scouse_unit_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT NOT NULL,
    line_label      TEXT NOT NULL,
    cube_fits_path  TEXT NOT NULL,
    output_dir      TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_name, line_label)
);

CREATE TABLE IF NOT EXISTS scouse_pipeline_state (
    scouse_unit_id       INTEGER PRIMARY KEY REFERENCES scouse_unit(scouse_unit_id),
    source_name          TEXT NOT NULL,
    line_label           TEXT NOT NULL,
    current_state        TEXT NOT NULL DEFAULT 'pending',
    updated_at           TEXT NOT NULL,
    parameters_json      TEXT,
    n_fitted_components  INTEGER,
    n_acorns_clusters    INTEGER,
    acorns_output_path   TEXT,
    failure_count        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scouse_state_transitions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scouse_unit_id  INTEGER NOT NULL REFERENCES scouse_unit(scouse_unit_id),
    source_name     TEXT NOT NULL,
    line_label      TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT NOT NULL,
    at              TEXT NOT NULL,
    note            TEXT
);
"""


def main() -> None:
    conn = db.get_connection(DB_PATH)
    conn.executescript(DDL)
    conn.commit()
    tables = [
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'scouse_%'"
        ).fetchall()
    ]
    conn.close()
    print(f"[OK] scouse tables present: {tables}")


if __name__ == "__main__":
    main()
