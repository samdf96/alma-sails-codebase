"""
mark_listobs_complete.py
----------------
Simply mark the pre_selfcal_listobs_status as complete for a given MOUS ID.

Example:
    python mark_listobs_complete.py uid://A002/X628157/X2d
"""
# ruff: noqa: E402

# ---------------------------------------------------------------------
# Bootstrap path so imports work when running from scripts/
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------
import argparse

from alma_ops.config import DB_PATH
from alma_ops.db import (
    db_execute,
    get_db_connection,
)
from alma_ops.logging import get_logger

# ---------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------
log = get_logger("mark_listobs_complete")


def main():
    parser = argparse.ArgumentParser(
        description="Mark the pre_selfcal_listobs_status as complete for a given MOUS ID."
    )
    parser.add_argument("mous", help="MOUS ID (uid___A/B/C or uid://A/B/C format)")
    parser.add_argument(
        "--db-path", default=DB_PATH, help="Path to the pipeline_state database"
    )
    args = parser.parse_args()

    mous_id = args.mous
    db_path = args.db_path

    # Connect to the database
    conn = get_db_connection(db_path)

    # Update the pre_selfcal_listobs_status to 'complete'
    update_query = """
        UPDATE pipeline_state
        SET pre_selfcal_listobs_status = 'complete'
        WHERE mous_id = ?
    """
    db_execute(conn, update_query, (mous_id,), commit=True)

    log.info(f"Marked pre_selfcal_listobs_status as complete for MOUS ID: {mous_id}")


if __name__ == "__main__":
    main()
