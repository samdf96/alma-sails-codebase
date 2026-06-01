"""
migrate_v1_to_v2.py
-------------------
One-time migration script that reads pipeline state from the v1 database
and applies it to the already-seeded v2 database.

For each MOUS:
  1. Map v1's scattered status columns to a single v2 FSM state
  2. Validate that the paths required by that state actually exist on disk
  3. Downgrade the state if required paths are missing
  4. Write the verified state and paths into v2 pipeline_state
  5. Insert a state_transitions record recording the migration

State mapping (v1 → v2):
  selfcal_status = 'complete'                          → selfcal_complete
  selfcal_status in ('prepped', 'selfcaled', 'in_progress') → split_complete
  pre_selfcal_split_status = 'complete'                → split_complete
  download_status in ('complete', 'downloaded')        → downloaded
  everything else                                      → pending

Downgrade chain (if required paths are missing on disk):
  selfcal_complete  → split_complete → downloaded → pending

Usage:
    python alma-sails-codebase/scripts/migrate_v1_to_v2.py [--dry-run] [--v1-db PATH] [--v2-db PATH]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from alma_ops.db import to_local


# =============================================================================
# Defaults
# =============================================================================

DEFAULT_V1_DB = Path(__file__).resolve().parents[2] / "db" / "serpens_main.db"
DEFAULT_V2_DB = Path(__file__).resolve().parents[2] / "db" / "serpens_v2.db"


# =============================================================================
# Helpers
# =============================================================================

def _parse_paths(raw) -> list[str]:
    """Parse a JSON path field from v1, returning a list of canonical path strings."""
    if not raw or raw in ("[]", ""):
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [p for p in parsed if p]
        return [parsed] if parsed else []
    except (json.JSONDecodeError, TypeError):
        return [raw] if raw else []


def _paths_exist(canonical_paths: list[str]) -> bool:
    """Return True if all paths in the list exist on disk (translated to local form)."""
    if not canonical_paths:
        return False
    return all(Path(to_local(p)).exists() for p in canonical_paths)


def _dir_exists(canonical_path: str | None) -> bool:
    """Return True if the single directory path exists on disk."""
    if not canonical_path or canonical_path in ("[]", ""):
        return False
    # mous_directory was sometimes stored as a JSON list in v1
    paths = _parse_paths(canonical_path)
    if paths:
        return Path(to_local(paths[0])).exists()
    return Path(to_local(canonical_path)).exists()


@dataclass
class MigrationRecord:
    mous_id: str
    candidate_state: str        # state inferred from v1 columns
    verified_state: str         # state after path validation
    downgrade_reason: str | None
    mous_directory: str | None
    split_products: list[str]
    selfcal_products: list[str]
    selfcal_products_sub: list[str]
    download_url: str | None


# =============================================================================
# State mapping
# =============================================================================

def _map_v1_state(row: sqlite3.Row) -> str:
    """Infer the candidate v2 state from v1's status columns."""
    selfcal   = row["selfcal_status"] or "pending"
    split     = row["pre_selfcal_split_status"] or "pending"
    download  = row["download_status"] or "pending"

    if selfcal == "complete":
        return "selfcal_complete"
    if selfcal in ("prepped", "selfcaled", "in_progress"):
        return "split_complete"
    if split == "complete":
        return "split_complete"
    if download in ("complete", "downloaded"):
        return "downloaded"
    return "pending"


def _validate_and_downgrade(candidate: str, row: sqlite3.Row) -> tuple[str, str | None]:
    """Validate paths exist for the candidate state; downgrade if not.

    Returns (verified_state, downgrade_reason).
    """
    selfcal_paths     = _parse_paths(row["selfcal_products_nonsub_path"])
    split_paths       = _parse_paths(row["split_products_path"])
    mous_dir          = row["mous_directory"]

    if candidate == "selfcal_complete":
        if _paths_exist(selfcal_paths):
            return "selfcal_complete", None
        reason = "selfcal_products missing on disk"
        candidate = "split_complete"

    if candidate == "split_complete":
        if _paths_exist(split_paths):
            return "split_complete", reason if 'reason' in dir() else None
        r = "split_products missing on disk"
        reason = f"{reason}; {r}" if 'reason' in dir() else r
        candidate = "downloaded"

    if candidate == "downloaded":
        if _dir_exists(mous_dir):
            return "downloaded", reason if 'reason' in dir() else None
        r = "mous_directory missing on disk"
        reason = f"{reason}; {r}" if 'reason' in dir() else r
        return "pending", reason

    return "pending", None


# =============================================================================
# Build migration records
# =============================================================================

def build_records(v1_conn: sqlite3.Connection) -> list[MigrationRecord]:
    v1_conn.row_factory = sqlite3.Row
    rows = v1_conn.execute("SELECT * FROM pipeline_state").fetchall()
    records = []

    for row in rows:
        candidate = _map_v1_state(row)
        verified, downgrade_reason = _validate_and_downgrade(candidate, row)

        selfcal_paths     = _parse_paths(row["selfcal_products_nonsub_path"])
        selfcal_sub_paths = _parse_paths(row["selfcal_products_sub_path"])
        split_paths       = _parse_paths(row["split_products_path"])

        # Only carry paths that are valid for the verified state
        records.append(MigrationRecord(
            mous_id           = row["mous_id"],
            candidate_state   = candidate,
            verified_state    = verified,
            downgrade_reason  = downgrade_reason,
            mous_directory    = row["mous_directory"] if _dir_exists(row["mous_directory"]) else None,
            split_products    = split_paths if _paths_exist(split_paths) else [],
            selfcal_products  = selfcal_paths if _paths_exist(selfcal_paths) else [],
            selfcal_products_sub = selfcal_sub_paths if _paths_exist(selfcal_sub_paths) else [],
            download_url      = row["download_url"],
        ))

    return records


# =============================================================================
# Apply to v2
# =============================================================================

def apply_records(v2_conn: sqlite3.Connection, records: list[MigrationRecord]) -> None:
    now = datetime.now(timezone.utc).isoformat()

    for rec in records:
        if rec.verified_state == "pending":
            # Seeding already created a pending row and initial transition — nothing to do
            continue

        note = "migrated from v1"
        if rec.downgrade_reason:
            note += f" (downgraded: {rec.downgrade_reason})"

        # Update pipeline_state
        v2_conn.execute(
            """
            UPDATE pipeline_state
            SET current_state=?, updated_at=?,
                download_url=?, mous_directory=?,
                split_products=?, selfcal_products=?, selfcal_products_sub=?
            WHERE mous_id=?
            """,
            (
                rec.verified_state,
                now,
                rec.download_url,
                rec.mous_directory,
                json.dumps(rec.split_products)     if rec.split_products     else None,
                json.dumps(rec.selfcal_products)   if rec.selfcal_products   else None,
                json.dumps(rec.selfcal_products_sub) if rec.selfcal_products_sub else None,
                rec.mous_id,
            ),
        )

        # Insert migration transition record (NULL → verified_state)
        cur = v2_conn.execute(
            """
            INSERT INTO state_transitions (mous_id, from_state, to_state, at, note)
            VALUES (?, NULL, ?, ?, ?)
            """,
            (rec.mous_id, rec.verified_state, now, note),
        )
        transition_id = cur.lastrowid

        # Update last_transition_id
        v2_conn.execute(
            "UPDATE pipeline_state SET last_transition_id=? WHERE mous_id=?",
            (transition_id, rec.mous_id),
        )

    v2_conn.commit()


# =============================================================================
# Summary
# =============================================================================

def print_summary(records: list[MigrationRecord], dry_run: bool) -> None:
    from collections import Counter
    prefix = "[DRY RUN] " if dry_run else ""

    state_counts = Counter(r.verified_state for r in records)
    downgrades   = [r for r in records if r.downgrade_reason]

    print(f"\n{prefix}Migration summary ({len(records)} MOUSes)")
    print("=" * 60)
    for state, count in sorted(state_counts.items()):
        print(f"  {state:<25} {count}")

    if downgrades:
        print(f"\n{prefix}Downgraded MOUSes ({len(downgrades)}):")
        for r in downgrades:
            print(f"  {r.mous_id}")
            print(f"    {r.candidate_state} → {r.verified_state}")
            print(f"    reason: {r.downgrade_reason}")

    print()


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Migrate pipeline state from v1 to v2 database.")
    parser.add_argument("--v1-db",    default=DEFAULT_V1_DB, help="Path to v1 database")
    parser.add_argument("--v2-db",    default=DEFAULT_V2_DB, help="Path to v2 database")
    parser.add_argument("--dry-run",  action="store_true",   help="Preview without writing to v2 DB")
    args = parser.parse_args()

    v1_conn = sqlite3.connect(str(args.v1_db))
    v1_conn.row_factory = sqlite3.Row

    print(f"Reading v1 database: {args.v1_db}")
    records = build_records(v1_conn)
    v1_conn.close()

    print_summary(records, dry_run=args.dry_run)

    if args.dry_run:
        print("[DRY RUN] No changes written.")
        return

    v2_conn = sqlite3.connect(str(args.v2_db))
    v2_conn.execute("PRAGMA foreign_keys = ON;")
    print(f"Applying to v2 database: {args.v2_db}")
    apply_records(v2_conn, records)
    v2_conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    main()
