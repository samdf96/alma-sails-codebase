"""
report_data_columns.py
--------------------
Uses casatools' table functionality to check which
data columns are present in a given Measurement Set (MS).

*** This should only be run from a session that has access to
casatools, e.g., casa or a casapy environment. ***

Usage:
    python report_data_columns.py <MOUS_ID> [--db-path <DB_PATH>]
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
from pathlib import Path

# casa specific imports
from casatools import table

from alma_ops.config import DB_PATH
from alma_ops.db import (
    get_db_connection,
    get_pipeline_state_calibrated_products,
    get_pipeline_state_record,
)
from alma_ops.logging import get_logger

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
log = get_logger("report_data_columns")


def check_ms_datacolumn(ms_path):
    tb = table()

    try:
        tb.open(ms_path)
    except Exception as e:
        print(f"ERROR: Could not open MS: {ms_path}")
        print(e)
        return None

    cols = tb.colnames()
    tb.close()

    has_data = "DATA" in cols
    has_corrected = "CORRECTED_DATA" in cols

    print(f"MS: {ms_path}")
    print(f"Columns found: {', '.join(cols)}")

    if has_data:
        print("✔ Found DATA column (default)")
        return "data"

    if has_corrected:
        print("✔ DATA not found; using CORRECTED_DATA")
        return "corrected"

    print("⚠ WARNING: Neither DATA nor CORRECTED_DATA column found!")
    return None


# ---------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inspect and report data columns in the MOUS."
    )
    parser.add_argument(
        "mous_id",
        help="MOUS ID to inspect.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help="Path to the database file.",
    )
    args = parser.parse_args()

    # connect to ensure MOUS exists
    with get_db_connection(args.db_path) as conn:
        record = get_pipeline_state_record(conn, args.mous_id)
        if record is None:
            log.error(f"MOUS {args.mous_id} not found in database.")
            exit(1)

        # grab the first value in the string contained in the calibrated products
        calibrated_products = get_pipeline_state_calibrated_products(conn, args.mous_id)

        if calibrated_products is None or len(calibrated_products) == 0:
            log.error(f"No calibrated products found for MOUS {args.mous_id}.")
            exit(1)

        # grab the first value in the string contained in the calibrated products
        ms_to_test = calibrated_products[0]

        log.info(f"Inspecting MS: {ms_to_test}")

    # open table and extract table information
    preferred = check_ms_datacolumn(ms_to_test)

    if preferred is not None:
        print(f"Preferred datacolumn: {preferred}")
    else:
        print("Preferred datacolumn: UNKNOWN")
