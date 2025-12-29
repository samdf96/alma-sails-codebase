# alma_ops/downloads/status.py

import json
from datetime import datetime

from alma_ops.db import db_transaction
from alma_ops.logging import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------
# Core download update function
# ---------------------------------------------------------------------

#TODO: Move this to its own status file in splits
def update_mous_split_state(
    conn,
    mous_id: str,
    split_status: str,
    split_products: list[str],
    note: str,
):
    """
    Low-level helper to update the split_* fields for a MOUS.
    Do not call directly from workflows — use mark_split_* convenience
    functions instead.
    """
    with db_transaction(conn):
        conn.execute("""
            UPDATE mous
            SET split_status=?,
                split_products=?,
                split_date=?,
                split_notes=?
            WHERE mous_id=?
        """, (
            split_status,
            json.dumps(split_products or []),
            datetime.utcnow().isoformat(),
            note,
            mous_id
        ))


# ---------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------

#TODO: Move this to its own status file in splits
def mark_split_complete(conn, mous_id, split_products):
    note = f"Found {len(split_products)} complete split products."
    update_mous_split_state(
        conn, mous_id,
        split_status="complete",
        split_products=split_products,
        note=note,
    )
    log.info(f"[{mous_id}] Split COMPLETE — {len(split_products)} products")


#TODO: Move this to its own status file in splits
def mark_split_partial(conn, mous_id, split_products, missing_targets):
    note = f"Missing split datasets for: {', '.join(missing_targets)}"
    update_mous_split_state(
        conn, mous_id,
        split_status="partial",
        split_products=split_products,
        note=note,
    )
    log.warning(f"[{mous_id}] Split PARTIAL — {note}")


#TODO: Move this to its own status file in splits
def mark_split_missing(conn, mous_id):
    note = "No split datasets found"
    update_mous_split_state(
        conn, mous_id,
        split_status="missing",
        split_products=[],
        note=note,
    )
    log.error(f"[{mous_id}] Split MISSING — {note}")

