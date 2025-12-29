"""
add_spw_remap.py
----------------
Add or update the SPW remap for a given MOUS ID in the pipeline_state database.

Example:
    python add_spw_remap.py uid://A002/X628157/X2d 16:0 18:1 20:2 22:3
"""
# ruff: noqa: E402

# ---------------------------------------------------------------------
# Bootstrap path so imports work when running from scripts/
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# Standard imports
# ---------------------------------------------------------------------
import argparse
import json

from alma_ops.config import DB_PATH
from alma_ops.db import (
    db_execute,
    get_db_connection,
    get_pipeline_state_record,
)

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.logging import get_logger
from alma_ops.utils import to_db_mous_id

# ---------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------
log = get_logger("add_spw_remap")


def parse_spw_map(pairs: list[str]) -> dict[int, int]:
    remap = {}
    for p in pairs:
        try:
            src, dst = p.split(":")
            remap[int(src)] = int(dst)
        except ValueError:
            raise ValueError(f"Invalid mapping '{p}'. Expected N:M")
    return remap


def main():
    parser = argparse.ArgumentParser(
        description="Add/update the SPW remap for a given MOUS ID."
    )
    parser.add_argument("mous", help="MOUS ID (uid___A/B/C or uid://A/B/C format)")
    parser.add_argument(
        "map",
        nargs="+",
        help="SPW mappings in form src:dst (e.g. 16:0 18:1)",
    )
    parser.add_argument(
        "--db-path", default=DB_PATH, help=f"Path to database (default: {DB_PATH})"
    )
    args = parser.parse_args()

    # ---------------------------------------------------------------
    # Normalize MOUS ID
    # ---------------------------------------------------------------
    try:
        normalized = to_db_mous_id(args.mous)
        log.info(f"Normalized MOUS ID → {normalized}")
    except Exception as e:
        log.error(f"Invalid MOUS ID format: {e}")
        return

    # ---------------------------------------------------------------
    # Connect to DB
    # ---------------------------------------------------------------
    with get_db_connection(DB_PATH) as conn:
        # ensure MOUS exists
        record = get_pipeline_state_record(conn, normalized)
        if record is None:
            log.error(f"MOUS {normalized} not found in database.")
            return

        # -----------------------------------------------------------
        # Update field
        # -----------------------------------------------------------
        try:
            remap = parse_spw_map(args.map)
        except ValueError:
            raise

        payload = json.dumps(remap, indent=2)

        db_execute(
            conn,
            """
            UPDATE pipeline_state
            SET raw_data_spectral_remap = ?
            WHERE mous_id = ?
            """,
            params=(payload, normalized),
            commit=True,
        )

    log.info(f"SPW remap for MOUS {normalized} updated successfully.")


# =====================================================================
if __name__ == "__main__":
    main()
