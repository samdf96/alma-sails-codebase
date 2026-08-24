"""
reset_scouse.py
---------------
Reset a SCOUSE unit back to a prior state so a stage can be re-run.

Run from the VM shell (no CASA or venv required):

    python alma-sails-codebase/scripts/reset_scouse.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)" \\
        --to-state prep_complete

Valid reset targets (--to-state):
    prep_complete    re-run from stage 1
    stage1_complete  re-run from stage 2
    stage2_complete  re-run from stage 3
    stage3_complete  re-run from stage 4
    stage4_complete  re-run ACORNS

The unit's output directory is NOT deleted — only the DB state is rolled back.
If you want a clean re-run from stage 1, manually delete
<output_dir>/<cube_stem>/ first.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import SCOUSE_FSM_ORDER

VALID_TARGETS = {
    "prep_complete",
    "stage1_complete",
    "stage2_complete",
    "stage3_complete",
    "stage4_complete",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset a SCOUSE unit back to a prior FSM state."
    )
    parser.add_argument("--source",     required=True)
    parser.add_argument("--line-label", required=True)
    parser.add_argument(
        "--to-state", required=True,
        choices=sorted(VALID_TARGETS),
        help="Target state to reset to",
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    if args.to_state not in VALID_TARGETS:
        print(f"[ERROR] Invalid reset target '{args.to_state}'. "
              f"Choose from: {sorted(VALID_TARGETS)}")
        sys.exit(1)

    conn = db.get_connection(args.db_path)
    state_row = db.fetch_one(
        conn,
        "SELECT * FROM scouse_pipeline_state WHERE source_name=? AND line_label=?",
        (args.source, args.line_label),
    )
    if state_row is None:
        print(f"[ERROR] No SCOUSE unit for source='{args.source}' line='{args.line_label}'.")
        sys.exit(1)

    current = state_row["current_state"]
    target  = args.to_state

    target_idx  = SCOUSE_FSM_ORDER.index(target)
    current_idx = SCOUSE_FSM_ORDER.index(current) if current in SCOUSE_FSM_ORDER else 999

    if current_idx <= target_idx:
        print(
            f"[ERROR] Current state '{current}' is not ahead of target '{target}'. "
            f"Nothing to reset."
        )
        sys.exit(1)

    unit_id = state_row["scouse_unit_id"]
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        "UPDATE scouse_pipeline_state SET current_state=?, updated_at=? WHERE scouse_unit_id=?",
        (target, now, unit_id),
    )
    conn.execute(
        """
        INSERT INTO scouse_state_transitions
            (scouse_unit_id, source_name, line_label, from_state, to_state, at, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (unit_id, args.source, args.line_label, current, target, now, "manual reset"),
    )
    conn.commit()
    conn.close()

    print(f"[OK] {args.source} / {args.line_label}: '{current}' → '{target}'")


if __name__ == "__main__":
    main()
