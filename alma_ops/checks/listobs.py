# alma_ops/checks/listobs.py

from pathlib import Path
import json
import os
from alma_ops.logging import get_logger
from alma_ops.utils import to_dir_mous_id

log = get_logger(__name__)

def check_for_listobs(conn, base_dir: str, verbose: bool = False):
    """
    Check that listobs products exist for:

      - All raw ASDMs recorded in mous.asdm_paths (even if the .ms itself
        has been deleted after listobs was run)
      - All split MS files currently present under datasets/<mous>/splits/

    Logic:
      - For RAW: use asdm_paths from the database; expected listobs is
        "<asdm_path>.listobs.txt"
      - For SPLITS: for each *.ms in splits/, expected listobs is
        "<split_ms>.listobs.txt"
      - Status 'ok' only if ALL expected listobs files exist.
      - Status 'missing' otherwise, with a note describing what is missing.

    Special case:
      If split listobs exist but some raw listobs are missing, we append
      a warning note (this likely indicates a workflow hiccup).
    """
    cursor = conn.cursor()
    cursor.execute("SELECT mous_id, asdm_paths FROM mous")
    rows = cursor.fetchall()

    results = []

    for row in rows:
        mous_id = row["mous_id"]
        asdm_paths_json = row["asdm_paths"]

        # -----------------------------
        # RAW ASDM listobs expectations
        # -----------------------------
        raw_asdm_paths = []
        if asdm_paths_json:
            try:
                raw_asdm_paths = json.loads(asdm_paths_json)
            except Exception:
                if verbose:
                    log.warning(f"[{mous_id}] Could not parse asdm_paths JSON; skipping raw listobs checks.")

        expected_raw_listobs = [p + ".listobs.txt" for p in raw_asdm_paths]
        present_raw_listobs = [p for p in expected_raw_listobs if os.path.exists(p)]
        missing_raw_listobs = [p for p in expected_raw_listobs if not os.path.exists(p)]
        
        # -----------------------------
        # SPLIT MS listobs expectations
        # -----------------------------
        mous_dir = Path(base_dir) / to_dir_mous_id(mous_id)
        splits_dir = mous_dir / "splits"

        split_ms_files = list(splits_dir.glob("*.ms")) if splits_dir.exists() else []
        expected_split_listobs = [
            ms.with_suffix(ms.suffix + ".listobs.txt") for ms in split_ms_files
        ]
        present_split_listobs = [str(p) for p in expected_split_listobs if p.exists()]
        missing_split_listobs = [str(p) for p in expected_split_listobs if not p.exists()]

        # -----------------------------
        # Determine overall status
        # -----------------------------
        any_expected = bool(expected_raw_listobs or expected_split_listobs)

        if not any_expected:
            # We don't actually know what to expect here (no asdm_paths, no splits)
            status = "no-data"
            note = "No ASDM paths in DB and no split MS on disk; no expected listobs."
            if verbose:
                log.info(f"[{mous_id}] {note}")

        else:
            if not missing_raw_listobs and not missing_split_listobs:
                # Everything we expect is present
                status = "ok"
                note = (
                    f"All listobs present "
                    f"({len(present_raw_listobs)} raw, {len(present_split_listobs)} split)."
                )
                if verbose:
                    log.info(f"✅ [{mous_id}] {note}")
            else:
                # Something is missing
                pieces = []
                if missing_raw_listobs:
                    pieces.append(f"{len(missing_raw_listobs)} raw listobs missing")
                if missing_split_listobs:
                    pieces.append(f"{len(missing_split_listobs)} split listobs missing")

                note = "; ".join(pieces)
                status = "missing"

                # Special diagnostic: we DO have split listobs, but some raw listobs missing
                if present_split_listobs and missing_raw_listobs:
                    note += "; split listobs present but raw listobs missing"

                if verbose:
                    log.warning(f"⚠️ [{mous_id}] {note}")

        results.append({
            "mous_id": mous_id,
            "status": status,
            "note": note,
        })

    return results

