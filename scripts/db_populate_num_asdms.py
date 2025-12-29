"""
update_asdm_counts.py
---------------------
Update the 'num_asdms' column in the 'mous' table using
a manually curated JSON metadata file.

The JSON file should look like this:
{
    "uid://A002/X628157/X2d": 2,
    "uid://A002/X628157/X35": 1,
    "uid://A001/X13e/X1f4": 1
}

Usage:
    python run_database_update_asdm_counts.py --db serpens_main.db --json serpens_main_asdm_metadata.json
"""
# ruff: noqa: E402

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops when running from scripts/)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------

import argparse
import json
import sqlite3
from pathlib import Path

from alma_ops.config import DB_PATH
from alma_ops.logging import get_logger
from tabulate import tabulate

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = get_logger("db_populate_num_asdms")


def load_asdm_metadata(json_path: Path) -> dict:
    """Load manual ASDM counts from a JSON file."""
    if not json_path.exists():
        raise FileNotFoundError(f"❌ JSON metadata file not found: {json_path}")
    with open(json_path, "r") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(
            f"JSON structure must be a dictionary of {{mous_id: count}}, not {type(data)}"
        )
    print(f"📁 Loaded {len(data)} MOUS entries from {json_path}")
    return data


def update_asdm_counts(conn, asdm_counts: dict):
    """Update num_asdms for each MOUS in the database."""
    cursor = conn.cursor()
    updates = []
    missing = []

    for mous_id, count in asdm_counts.items():
        cursor.execute("SELECT mous_id FROM mous WHERE mous_id = ?", (mous_id,))
        result = cursor.fetchone()
        if result:
            cursor.execute(
                """
                UPDATE mous
                SET num_asdms=?
                WHERE mous_id=?
                """,
                (
                    count,
                    mous_id,
                ),
            )
            updates.append((mous_id, count))
        else:
            missing.append(mous_id)

    conn.commit()
    return updates, missing


def print_summary(updates, missing, dry_run=False):
    """Print a summary table."""
    print("\n=== ASDM COUNT UPDATES ===")
    if updates:
        table = [[m, c] for m, c in updates]
        print(tabulate(table, headers=["MOUS ID", "num_asdms"], tablefmt="grid"))
        print(
            f"\n{'🧪 (dry-run)' if dry_run else '✅'} Updated {len(updates)} entries."
        )
    else:
        print("No matching MOUS entries updated.")

    if missing:
        print("\n⚠️  MOUS IDs not found in database:")
        for m in missing:
            print(f"   - {m}")
    else:
        print("\nNo missing MOUS IDs.")


# ---------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update ASDM counts in 'mous' table using a JSON metadata file."
    )
    parser.add_argument(
        "json", help="Path to JSON file with {mous_id: asdm_count} data."
    )
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview changes without committing."
    )
    args = parser.parse_args()

    json_path = Path(args.json)

    with sqlite3.connect(args.db_path) as conn:
        asdm_counts = load_asdm_metadata(json_path)

        if args.dry_run:
            cursor = conn.cursor()
            cursor.execute("SELECT mous_id FROM mous")
            existing = {row[0] for row in cursor.fetchall()}
            updates = [(m, c) for m, c in asdm_counts.items() if m in existing]
            missing = [m for m in asdm_counts if m not in existing]
        else:
            updates, missing = update_asdm_counts(conn, asdm_counts)

        print_summary(updates, missing, dry_run=args.dry_run)
