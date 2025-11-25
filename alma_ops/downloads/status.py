# alma_ops/downloads/status.py

import json
from datetime import datetime
from alma_ops.db import db_transaction
from alma_ops.logging import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------
# Core download update function
# ---------------------------------------------------------------------

def update_mous_download_state(
    conn,
    mous_id: str,
    downloaded: str,
    download_path: str | None,
    asdm_paths: list[str],
    note: str
):
    """Low-level update helper — do not call directly from workflows."""
    with db_transaction(conn):
        conn.execute("""
            UPDATE mous
            SET downloaded=?, 
                download_date=?, 
                download_path=?, 
                asdm_paths=?, 
                download_notes=?
            WHERE mous_id=?
        """, (
            downloaded,
            datetime.utcnow().isoformat(),
            download_path,
            json.dumps(asdm_paths),
            note,
            mous_id
        ))


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

def mark_download_success(conn, mous_id: str, download_path: str, asdm_paths: list[str]):
    """Mark MOUS as fully downloaded and consistent."""
    note = f"Download complete ({len(asdm_paths)} ASDMs) on {datetime.utcnow().isoformat()}"
    update_mous_download_state(
        conn,
        mous_id=mous_id,
        downloaded="yes",
        download_path=download_path,
        asdm_paths=asdm_paths,
        note=note
    )
    log.info(f"[{mous_id}] Download SUCCESS — {len(asdm_paths)} ASDMs")


def mark_download_partial(
    conn,
    mous_id: str,
    download_path: str,
    asdm_paths: list[str],
    expected_count: int
):
    """
    Mark MOUS as partially downloaded.
    expected_count = number expected (from num_asdms column)
    """
    found = len(asdm_paths)
    note = (
        f"Partial download — found {found}/{expected_count} ASDMs "
        f"({datetime.utcnow().isoformat()})"
    )

    update_mous_download_state(
        conn,
        mous_id=mous_id,
        downloaded="partial",
        download_path=download_path,
        asdm_paths=asdm_paths,
        note=note
    )

    log.warning(f"[{mous_id}] Download PARTIAL — {found}/{expected_count} ASDMs")


def mark_download_failure(conn, mous_id: str, error_message: str):
    """Mark MOUS as failed to download and record the error message."""
    note = f"Download failed: {error_message}"
    update_mous_download_state(
        conn,
        mous_id=mous_id,
        downloaded="failed",
        download_path=None,
        asdm_paths=[],
        note=note
    )
    log.error(f"[{mous_id}] Download FAILURE — {error_message}")


def mark_split_complete(conn, mous_id, split_products):
    note = f"Found {len(split_products)} complete split products."
    update_mous_split_state(
        conn, mous_id,
        split_status="complete",
        split_products=split_products,
        note=note,
    )
    log.info(f"[{mous_id}] Split COMPLETE — {len(split_products)} products")


def mark_split_partial(conn, mous_id, split_products, missing_targets):
    note = f"Missing split datasets for: {', '.join(missing_targets)}"
    update_mous_split_state(
        conn, mous_id,
        split_status="partial",
        split_products=split_products,
        note=note,
    )
    log.warning(f"[{mous_id}] Split PARTIAL — {note}")


def mark_split_missing(conn, mous_id):
    note = "No split datasets found"
    update_mous_split_state(
        conn, mous_id,
        split_status="missing",
        split_products=[],
        note=note,
    )
    log.error(f"[{mous_id}] Split MISSING — {note}")

