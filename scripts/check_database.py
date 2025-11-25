"""
database_refresh.py
--------------------
For verifying the state of the database. Includes checking downloads, splits, etc.
"""

# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops when running from scripts/)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()

# ---------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------
import argparse

from alma_ops.checks.listobs import check_for_listobs
from alma_ops.checks.raw_asdms import check_for_raw_asdms
from alma_ops.checks.split_products import check_for_split_products
from alma_ops.checks.summary import summarize_results

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.config import DATASETS_DIR, DB_PATH
from alma_ops.db import get_db_connection
from alma_ops.logging import get_logger

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = get_logger("check_database")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Verify that database state matches filesystem data."
    )
    parser.add_argument("--db-path", default=DB_PATH)
    parser.add_argument("--datasets-dir", default=DATASETS_DIR)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--dry-run", action="store_true", help="Will prevent database updates"
    )
    parser.add_argument(
        "--show-table", action="store_true", help="Display per-MOUS status table"
    )
    args = parser.parse_args()

    with get_db_connection(args.db_path) as conn:

        raw = check_for_raw_asdms(
            conn, args.datasets_dir, verbose=args.verbose, dry_run=args.dry_run
        )

        splits = check_for_split_products(
            conn, args.datasets_dir, verbose=args.verbose, dry_run=args.dry_run
        )

        listobs = check_for_listobs(conn, args.datasets_dir, verbose=args.verbose)

        summarize_results(
            raw, splits, listobs, args.db_path, show_table=args.show_table
        )
