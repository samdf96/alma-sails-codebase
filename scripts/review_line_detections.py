#!/usr/bin/env python3
"""
review_line_detections.py
----------------------------
List and review ADMIT line detections (line_admit_detection). No CASA
needed -- pure DB read/write, run from the VM shell.

By default, lists all 'pending' detections across the whole survey, one
line per row: which (mous, source, spw) it came from, species/frequency,
peakrms, and blend_id (a nonzero blend_id means ADMIT's own clustering
flagged this as part of a blended/overlapping group -- worth a second look,
same pattern seen repeatedly during initial testing: a low or negative
peakrms right next to a much stronger line is usually that stronger line's
neighbor losing the channel-range split, not an independent detection).

Detections are listed sorted by peakrms_sigma descending so the strongest
lines appear first.

Usage:
    python3 review_line_detections.py                          # list all pending
    python3 review_line_detections.py --mous-id uid://... --source EC_53 --spw 3
    python3 review_line_detections.py --confirm 42 [--note "..."]
    python3 review_line_detections.py --reject 43 --note "riding on stronger neighbor, blend_id=1"
    python3 review_line_detections.py --reject-below 3.0 [--dry-run]
        # bulk-reject all pending detections below 3.0 sigma; combine with
        # --mous-id/--source/--spw to scope to a specific unit. --dry-run
        # previews what would be rejected without writing anything.
    python3 review_line_detections.py --purge-triage-cube --mous-id uid://... --source EC_53 --spw 3
        # purges the triage cube for that SPW only if every detection was rejected
        # (none pending, none confirmed). Safe default -- other SPWs for the same
        # source may still be pending. Pair with --reject-below to clean up noise SPWs.
    python3 review_line_detections.py --purge-triage-cube --include-confirmed --mous-id uid://... --source EC_53 --spw 3
        # same, but also purges SPWs where some detections were confirmed. Use when
        # you want to clear a fully-reviewed SPW regardless of outcome.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.species_shorthand import shorthand_for

SEP = "-" * 120


def _print_detections(rows) -> None:
    if not rows:
        print("  (none)")
        return
    print(f"  {'id':>5} {'mous_id':22} {'source':13} {'spw':>3} {'species':20} {'shorthand':11} "
          f"{'freq(GHz)':>10} {'peakrms':>9} {'blend':>6} {'status':10}")
    print("  " + SEP)
    for r in rows:
        species = r['species_name'] or r['formula'] or ''
        shorthand = shorthand_for(r['species_name'], r['frequency_ghz']) or ''
        print(f"  {r['line_admit_detection_id']:>5} {r['mous_id']:22.22} {r['target_name']:13.13} {r['spw_idx']:>3} "
              f"{species:20.20} {shorthand:11.11} "
              f"{r['frequency_ghz']:>10.4f} {r['peakrms_sigma']:>9.2f} {r['blend_id']:>6} {r['review_status']:10}")


def _rows_with_unit_info(conn, detections):
    """Join detections back to line_admit_pipeline_state for mous/source/spw."""
    out = []
    for d in detections:
        unit = db.get_line_admit_pipeline_state(conn, d["line_admit_unit_id"])
        merged = dict(d)
        merged["mous_id"] = unit["mous_id"]
        merged["target_name"] = unit["target_name"]
        merged["spw_idx"] = unit["spw_idx"]
        out.append(merged)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="List and review ADMIT line detections.")
    parser.add_argument("--mous-id", default=None)
    parser.add_argument("--source", default=None, help="Target/field name")
    parser.add_argument("--spw", type=int, default=None)
    parser.add_argument("--status", default="pending", choices=["pending", "confirmed", "rejected", "all"],
                         help="Filter by review_status when listing (default: pending)")
    parser.add_argument("--confirm", type=int, default=None, metavar="DETECTION_ID")
    parser.add_argument("--reject", type=int, default=None, metavar="DETECTION_ID")
    parser.add_argument("--note", default=None, help="Optional note for --confirm/--reject")
    parser.add_argument("--reject-below", type=float, default=None, metavar="SIGMA",
                         help="Bulk-reject all pending detections with peakrms_sigma below SIGMA")
    parser.add_argument("--dry-run", action="store_true",
                         help="With --reject-below: show what would be rejected without writing")
    parser.add_argument("--purge-triage-cube", action="store_true",
                         help="Delete the triage cube for --mous-id/--source/--spw if every detection is rejected")
    parser.add_argument("--include-confirmed", action="store_true",
                         help="With --purge-triage-cube: also purge SPWs where some detections were confirmed")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    if args.confirm is not None:
        db.update_detection_review(conn, args.confirm, "confirmed", args.note)
        print(f"[OK] detection {args.confirm} → confirmed")
        conn.close()
        return

    if args.reject is not None:
        db.update_detection_review(conn, args.reject, "rejected", args.note)
        print(f"[OK] detection {args.reject} → rejected")
        conn.close()
        return

    if args.reject_below is not None:
        candidates = db.get_line_admit_detections(conn, review_status="pending")
        rows = _rows_with_unit_info(conn, candidates)
        if args.mous_id:
            rows = [r for r in rows if r["mous_id"] == args.mous_id]
        if args.source:
            rows = [r for r in rows if r["target_name"] == args.source]
        if args.spw is not None:
            rows = [r for r in rows if r["spw_idx"] == args.spw]
        to_reject = sorted(
            [r for r in rows if r["peakrms_sigma"] < args.reject_below],
            key=lambda r: r["peakrms_sigma"],
        )
        tag = "[DRY RUN] " if args.dry_run else ""
        print(f"{tag}Rejecting {len(to_reject)} pending detection(s) below {args.reject_below}σ:")
        _print_detections(to_reject)
        if not args.dry_run:
            for r in to_reject:
                db.update_detection_review(
                    conn, r["line_admit_detection_id"], "rejected",
                    f"auto-rejected: peakrms_sigma={r['peakrms_sigma']:.2f} < threshold {args.reject_below}",
                )
            print(f"[OK] {len(to_reject)} detection(s) rejected.")
        conn.close()
        return

    if args.purge_triage_cube:
        if not (args.mous_id and args.source and args.spw is not None):
            print("[ERROR] --purge-triage-cube requires --mous-id, --source, and --spw")
            conn.close()
            sys.exit(1)
        triage_row = db.fetch_one(
            conn,
            "SELECT * FROM line_triage_pipeline_state WHERE mous_id=? AND target_name=? AND spw_idx=?",
            (args.mous_id, args.source, args.spw),
        )
        admit_row = triage_row and db.get_line_admit_unit_for_triage_unit(conn, triage_row["line_triage_unit_id"])
        if admit_row is None:
            print("[ERROR] No line_admit_unit found for that (mous_id, source, spw)")
            conn.close()
            sys.exit(1)
        from alma_ops.imaging.line_admit_utils import purge_triage_cube_if_reviewed
        purged = purge_triage_cube_if_reviewed(
            conn, admit_row["line_admit_unit_id"],
            include_confirmed=args.include_confirmed,
        )
        if purged:
            reason = "all reviewed" if args.include_confirmed else "all rejected"
            print(f"[OK] triage cube purged ({reason})")
        else:
            if args.include_confirmed:
                print("[SKIP] triage cube kept — some detections still pending")
            else:
                print("[SKIP] triage cube kept — some detections still pending or confirmed")
        conn.close()
        return

    status = None if args.status == "all" else args.status
    detections = db.get_line_admit_detections(conn, review_status=status)

    # mous_id/source/spw aren't columns on line_admit_detection itself, so
    # filtering on them happens after joining to the parent unit below.
    rows = _rows_with_unit_info(conn, detections)
    if args.mous_id:
        rows = [r for r in rows if r["mous_id"] == args.mous_id]
    if args.source:
        rows = [r for r in rows if r["target_name"] == args.source]
    if args.spw is not None:
        rows = [r for r in rows if r["spw_idx"] == args.spw]

    rows.sort(key=lambda r: r["peakrms_sigma"], reverse=True)

    conn.close()

    print(f"Detections (status={args.status}, n={len(rows)}):")
    _print_detections(rows)


if __name__ == "__main__":
    main()
