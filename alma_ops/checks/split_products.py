# alma_ops/checks/split_products.py

from pathlib import Path
from datetime import datetime
from alma_ops.logging import get_logger
from alma_ops.utils import to_dir_mous_id
from alma_ops.db import get_unique_target_names
from alma_ops.downloads.status import mark_split_complete, mark_split_partial, mark_split_missing

log = get_logger(__name__)

def check_for_split_products(conn, dataset_dir: str, verbose: bool = False, dry_run: bool = False):
    cursor = conn.cursor()
    cursor.execute("SELECT mous_id FROM mous")
    mous_ids = [r[0] for r in cursor.fetchall()]

    results = []

    for mous_id in mous_ids:
        mous_dir = Path(dataset_dir) / to_dir_mous_id(mous_id)
        splits_dir = mous_dir / "splits"

        if not splits_dir.exists():
            results.append({
                "mous_id": mous_id,
                "status": "none",
                "note": "No split directory",
            })
            continue

        split_products = sorted(str(p) for p in splits_dir.glob("*.ms"))
        found = len(split_products)

        targets = get_unique_target_names(conn, mous_id)
        missing_targets = [
            t for t in targets if not any(t in Path(p).stem for p in split_products)
        ]

        if len(split_products) == 0:

            if not dry_run:
                mark_split_missing(conn, mous_id)

            results.append({
                "mous_id": mous_id,
                "status": "none",
                "note": "Split directory exists but contains no .ms files",
            })
            continue

        if missing_targets:
            note = f"Missing split datasets for: {', '.join(missing_targets)}"
            if verbose:
                log.warning(f"⚠️ [{mous_id}] {note}")

            if not dry_run:
                mark_split_partial(conn, mous_id, split_products, missing_targets)

            results.append({
                "mous_id": mous_id,
                "status": "partial",
                "note": note,
            })

        else:
            note = f"Split products OK ({found} datasets)"
            if verbose:
                log.info(f"✅ [{mous_id}] {note}")

            if not dry_run:
                mark_split_complete(conn, mous_id, split_products)

            results.append({
                "mous_id": mous_id,
                "status": "ok",
                "note": note,
            })

    return results
