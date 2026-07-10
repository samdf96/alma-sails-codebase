"""
migrate_add_admit_tables.py
-----------------------------
One-off migration: adds the ADMIT line-identification FSM tables (parallel
to the line-triage FSM tables, one ADMIT run per completed line_triage_unit)
to serpens_v2.db, plus a vlsr_kms column on targets so ADMIT's line
identification uses a real per-target velocity instead of its own internal
demo-source-name lookup table.

Run once from the VM shell:

    python alma-sails-codebase/scripts/migrate_add_admit_tables.py

Idempotent — uses CREATE TABLE IF NOT EXISTS and checks for the column
before adding it, safe to re-run.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH

DDL = """
CREATE TABLE IF NOT EXISTS line_admit_unit (
    line_admit_unit_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    line_triage_unit_id INTEGER NOT NULL UNIQUE REFERENCES line_triage_unit(line_triage_unit_id)
);

CREATE TABLE IF NOT EXISTS line_admit_pipeline_state (
    line_admit_unit_id  INTEGER PRIMARY KEY REFERENCES line_admit_unit(line_admit_unit_id),
    line_triage_unit_id INTEGER NOT NULL,
    mous_id             TEXT NOT NULL,
    target_name         TEXT NOT NULL,
    spw_idx             INTEGER NOT NULL,
    current_state       TEXT NOT NULL DEFAULT 'ready',
    updated_at          TEXT NOT NULL,
    job_id              TEXT,
    failure_count       INTEGER NOT NULL DEFAULT 0,
    last_transition_id  INTEGER REFERENCES line_admit_state_transitions(id),
    admit_products      TEXT
);

CREATE TABLE IF NOT EXISTS line_admit_state_transitions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id   INTEGER NOT NULL,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    from_state           TEXT,
    to_state             TEXT NOT NULL,
    at                   TEXT NOT NULL,
    run_id               TEXT,
    note                 TEXT
);

CREATE TABLE IF NOT EXISTS line_admit_run_log (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id   INTEGER NOT NULL,
    line_triage_unit_id  INTEGER NOT NULL,
    mous_id              TEXT NOT NULL,
    target_name          TEXT NOT NULL,
    spw_idx              INTEGER NOT NULL,
    run_id               TEXT NOT NULL,
    step                 TEXT NOT NULL,
    at                   TEXT NOT NULL,
    payload              TEXT,
    level                TEXT NOT NULL DEFAULT 'INFO'
);

CREATE TABLE IF NOT EXISTS line_admit_detection (
    line_admit_detection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    line_admit_unit_id      INTEGER NOT NULL REFERENCES line_admit_unit(line_admit_unit_id),
    frequency_ghz           REAL NOT NULL,
    uid                     TEXT NOT NULL,
    formula                 TEXT,
    species_name            TEXT,
    transition               TEXT,
    velocity_kms              REAL,
    peakrms_sigma               REAL,
    peakintensity_jybeam          REAL,
    startchan                     INTEGER,
    endchan                        INTEGER,
    blend_id                       INTEGER,
    review_status                   TEXT NOT NULL DEFAULT 'pending',
    reviewed_at                      TEXT,
    reviewed_note                     TEXT
);
"""


def _add_vlsr_column(conn) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(targets)").fetchall()}
    if "vlsr_kms" in cols:
        print("[OK] targets.vlsr_kms already present")
        return
    conn.execute("ALTER TABLE targets ADD COLUMN vlsr_kms REAL")
    conn.execute("UPDATE targets SET vlsr_kms = 8.5")
    conn.commit()
    print("[OK] targets.vlsr_kms added and backfilled to 8.5 km/s for all rows")


def main() -> None:
    conn = db.get_connection(DB_PATH)
    _add_vlsr_column(conn)
    conn.executescript(DDL)
    conn.commit()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'line_admit_%'"
    ).fetchall()
    conn.close()
    print(f"[OK] line-admit tables present: {[t['name'] for t in tables]}")


if __name__ == "__main__":
    main()
