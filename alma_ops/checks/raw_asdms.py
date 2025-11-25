# alma_ops/checks/raw_asdms.py

from pathlib import Path
from datetime import datetime
from alma_ops.logging import get_logger
from alma_ops.utils import to_dir_mous_id
from alma_ops.db import db_fetch_all, get_mous_expected_asdms
from alma_ops.downloads.status import (
    mark_download_success,
    mark_download_partial,
    mark_download_failure,
)

log = get_logger(__name__)

def check_for_raw_asdms(conn, dataset_dir: str, verbose: bool = False, dry_run: bool = False):
    """
    Check raw ASDM .ms directories in DB_PATH/<mous_dir>.
    Falls back gracefully if no ASDMs exist (splits may still be valid).
    Returns a list of status dictionaries.
    """

    cursor = conn.cursor()
    cursor.execute("SELECT mous_id FROM mous")
    mous_ids = [r[0] for r in cursor.fetchall()]

    results = []

    for mous_id in mous_ids:
        mous_dir = Path(dataset_dir) / to_dir_mous_id(mous_id)

        # If no directory exists at all
        if not mous_dir.exists():
            results.append({
                "mous_id": mous_id,
                "status": "missing",
                "note": "MOUS directory missing",
            })
            continue

        # Raw ASDM directories (*.ms)
        raw_asdms = sorted(str(p) for p in mous_dir.glob("*.ms"))
        found = len(raw_asdms)
        expected = get_mous_expected_asdms(conn, mous_id)

        if found == 0:
            results.append({
                "mous_id": mous_id,
                "status": "none",
                "note": "No raw ASDMs found (may be split-only dataset).",
            })
            continue

        if found == expected:
            note = f"Raw ASDMs OK ({found}/{expected})"
            if verbose:
                log.info(f"✅ [{mous_id}] {note}")

            if not dry_run:
                mark_download_success(
                    conn,
                    mous_id,
                    download_path=str(mous_dir),
                    asdm_paths=raw_asdms,
                )

            results.append({
                "mous_id": mous_id,
                "status": "ok",
                "note": note,
            })

        elif found < expected:
            note = f"Partial: {found}/{expected} raw ASDMs found"
            if verbose:
                log.warning(f"⚠️ [{mous_id}] {note}")

            if not dry_run:
                mark_download_partial(
                    conn,
                    mous_id,
                    download_path=str(mous_dir),
                    asdm_paths=raw_asdms,
                    expected_count=expected,
                )

            results.append({
                "mous_id": mous_id,
                "status": "partial",
                "note": note,
            })

        else:
            # More ASDMs than expected — rare but possible if manually added
            note = f"Found MORE ASDMs than expected ({found}/{expected})"
            if verbose:
                log.error(f"❌ [{mous_id}] {note}")

            if not dry_run:
                mark_download_failure(conn, mous_id, Exception(note))

            results.append({
                "mous_id": mous_id,
                "status": "unexpected",
                "note": note,
            })

    return results