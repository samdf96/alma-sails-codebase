"""
seed_imaging_state.py
---------------------
Standalone script that populates the imaging_state table with one row for
every (source_name, band, config_type) combination that has at least one
selfcal-complete MOUS.

Run from the ALMA-SAILS root with the UV environment:

    python3 alma-sails-codebase/alma_ops/imaging/seed_imaging_state.py

Or with an explicit database path:

    python3 alma-sails-codebase/alma_ops/imaging/seed_imaging_state.py \\
        --db /path/to/serpens_main.db \\
        --imaging-root /arc/projects/ALMA-SAILS/reduction/imaging

Idempotent — rows that already exist in imaging_state are left unchanged
(INSERT OR IGNORE is used internally).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup: allow running from any working directory
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_CODEBASE_ROOT = _SCRIPT_DIR.parent.parent   # alma-sails-codebase/
_PROJECT_ROOT  = _CODEBASE_ROOT.parent       # ALMA-SAILS/

if str(_CODEBASE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODEBASE_ROOT))

from alma_ops.db import (
    get_db_connection,
    get_selfcal_complete_mous_for_imaging,
    insert_imaging_state_record,
)

# ---------------------------------------------------------------------------
# Default paths (VM-local; platform equivalents are stored in the DB)
# ---------------------------------------------------------------------------
_DEFAULT_DB_PATH = _PROJECT_ROOT / "db" / "serpens_main.db"
_DEFAULT_IMAGING_ROOT_PLATFORM = "/arc/projects/ALMA-SAILS/reduction/imaging"


def seed(db_path: Path, imaging_root_platform: str) -> None:
    """Query selfcal-complete MOUS, group by (source, band, config), seed DB rows."""

    conn = get_db_connection(str(db_path))

    rows = get_selfcal_complete_mous_for_imaging(conn)
    if not rows:
        print("No selfcal-complete MOUS found — nothing to seed.")
        conn.close()
        return

    # Group MOUS by (alma_source_name, band_list, array_type)
    groups: dict[tuple[str, str, str], list] = defaultdict(list)
    for row in rows:
        key = (row["alma_source_name"], row["band_list"], row["array_type"])
        groups[key].append(row)

    print(f"Found {len(rows)} selfcal-complete MOUS across {len(groups)} imaging group(s).\n")

    inserted = 0
    skipped  = 0

    for (source_name, band, config_type), mous_rows in sorted(groups.items()):
        # imaging_dir: platform path — created at imaging time by the driver
        imaging_dir = (
            f"{imaging_root_platform.rstrip('/')}"
            f"/{source_name}/Band{band}_{config_type}"
        )

        was_inserted = insert_imaging_state_record(
            conn,
            source_name=source_name,
            band=band,
            config_type=config_type,
            imaging_dir=imaging_dir,
        )

        status_label = "INSERTED" if was_inserted else "already exists"
        mous_ids = [r["mous_id"] for r in mous_rows]
        print(
            f"  [{status_label}] {source_name}  Band{band}  {config_type}"
            f"  ({len(mous_ids)} MOUS)"
        )
        if was_inserted:
            for mid in mous_ids:
                print(f"    {mid}")
        inserted += int(was_inserted)
        skipped  += int(not was_inserted)

    conn.close()
    print(f"\nDone — {inserted} new row(s) inserted, {skipped} already existed.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Seed imaging_state table from selfcal-complete MOUS."
    )
    p.add_argument(
        "--db",
        type=Path,
        default=_DEFAULT_DB_PATH,
        help=f"Path to the SQLite database (default: {_DEFAULT_DB_PATH})",
    )
    p.add_argument(
        "--imaging-root",
        default=_DEFAULT_IMAGING_ROOT_PLATFORM,
        help=(
            "Platform root under which per-source imaging directories will be "
            f"created (default: {_DEFAULT_IMAGING_ROOT_PLATFORM})"
        ),
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if not args.db.exists():
        print(f"ERROR: database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    seed(args.db, args.imaging_root)
