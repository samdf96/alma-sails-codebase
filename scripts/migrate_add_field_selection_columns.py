"""
migrate_add_field_selection_columns.py
-----------------------------------------
One-off migration: adds two nullable columns to mous_imaging_unit so a single
shared-name mosaic MOUS can back multiple imaging units, each pinned to a
specific subset of CASA field IDs rather than the whole mosaic field.

    source_field_name  TEXT  -- actual CASA field name in this MOUS's MS
                               (used for cont.dat lookup + msmd field
                               resolution). NULL means "same as
                               imaging_unit.target_name" -- the behaviour
                               every existing row already has today.
    field_ids           TEXT  -- JSON-encoded list of ints, e.g. "[59]" or
                               "[12,34]". NULL means "select the whole field
                               by name" -- the behaviour every existing row
                               already has today.

Both columns are NULL by default, so every pre-existing mous_imaging_unit
row (the 123+ single-pointing imaging units already imaged) is completely
unaffected.

Run once from the VM shell:

    python alma-sails-codebase/scripts/migrate_add_field_selection_columns.py

Idempotent -- checks PRAGMA table_info before adding either column, safe to
re-run.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH

NEW_COLUMNS = {
    "source_field_name": "TEXT",
    "field_ids": "TEXT",
}


def main() -> None:
    conn = db.get_connection(DB_PATH)
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(mous_imaging_unit)")}

    for col, col_type in NEW_COLUMNS.items():
        if col in existing:
            print(f"[skip] mous_imaging_unit.{col} already exists")
            continue
        conn.execute(f"ALTER TABLE mous_imaging_unit ADD COLUMN {col} {col_type}")
        print(f"[OK] added mous_imaging_unit.{col} {col_type}")

    conn.commit()
    cols = [row["name"] for row in conn.execute("PRAGMA table_info(mous_imaging_unit)")]
    conn.close()
    print(f"[OK] mous_imaging_unit columns: {cols}")


if __name__ == "__main__":
    main()
