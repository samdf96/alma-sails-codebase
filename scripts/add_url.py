"""
add_mous_url.py
----------------
Add or update a MOUS download URL in the database.

Usage:
    python add_mous_url.py uid___A002_X1234_Xabc https://...
    python add_mous_url.py uid://A002/X1234/Xabc https://...

This will:
  • Normalize the MOUS ID (uid___ → uid:// form)
  • Verify the MOUS exists
  • Update the download_url field
  • Reset downloaded state to "no"
"""

# ---------------------------------------------------------------------
# Bootstrap path so imports work when running from scripts/
# ---------------------------------------------------------------------
from bootstrap import setup_path
setup_path()

# ---------------------------------------------------------------------
# Standard imports
# ---------------------------------------------------------------------
import argparse
from datetime import datetime

# ---------------------------------------------------------------------
# Alma Ops imports
# ---------------------------------------------------------------------
from alma_ops.logging import get_logger
from alma_ops.config import DB_PATH
from alma_ops.utils import to_db_mous_id
from alma_ops.db import (
    get_db_connection,
    get_mous_record,
    db_execute,
)

# ---------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------
log = get_logger("add_mous_url")

# =====================================================================
# Script entry point
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Add/update the download URL for a given MOUS."
    )
    parser.add_argument(
        "mous",
        help="MOUS ID (uid___A/B/C or uid://A/B/C format)"
    )
    parser.add_argument(
        "download_url",
        help="Download URL to store"
    )
    parser.add_argument(
        "--db-path", default=DB_PATH,
        help=f"Path to database (default: {DB_PATH})"
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
    with get_db_connection(args.db_path) as conn:

        # Ensure MOUS exists
        record = get_mous_record(conn, normalized)
        if record is None:
            log.error(f"No MOUS found matching ID: {normalized}")
            return

        # -----------------------------------------------------------
        # Update fields
        # -----------------------------------------------------------
        timestamp = datetime.utcnow().isoformat()
        note = f"Download URL added/updated via add_mous_url.py at {timestamp}"

        db_execute(
            conn,
            """
            UPDATE mous
            SET download_url=?,
                downloaded='no',
                download_date=?,
                download_path=NULL,
                download_notes=?
            WHERE mous_id=?
            """,
            params=(
                args.download_url,
                timestamp,
                note,
                normalized,
            ),
            commit=True,
        )

        log.info(f"✅ Updated download URL for {normalized}")
        log.info(f"   → {args.download_url}")


# =====================================================================
if __name__ == "__main__":
    main()
