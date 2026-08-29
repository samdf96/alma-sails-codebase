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
    python3 review_line_detections.py --limit 20               # show only the top 20 by peakrms
    python3 review_line_detections.py --peakrms-min 5.0        # only detections >= 5 sigma
    python3 review_line_detections.py --peakrms-min 3.0 --peakrms-max 10.0  # 3–10 sigma window
    python3 review_line_detections.py --mous-id uid://... --source EC_53 --spw 3
    python3 review_line_detections.py --det 2651 2682 2653          # list just these ids
    python3 review_line_detections.py --det 2651,2682 --peakrms-min 6
        # --det takes any number of ids, space- and/or comma-separated, and stacks
        # with every other filter. It implies --status all (so a named id shows up
        # whatever its review state) unless --status is passed explicitly. Ids that
        # cannot be shown are reported with the reason.
    python3 review_line_detections.py --confirm 42 [--note "..."]
    python3 review_line_detections.py --reject 43 --note "riding on stronger neighbor, blend_id=1"
    python3 review_line_detections.py --confirm 42 43 44          # several at once
    python3 review_line_detections.py --reject 2680,2683,2691 --note "hyperfine of 2633"
        # --confirm/--reject take any number of ids, space- and/or comma-separated.
        # A --note applies to every id in the batch. Ids are all validated first,
        # so a typo fails the whole call rather than half-applying it.
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

SEP = "-" * 165


def _print_detections(rows) -> None:
    if not rows:
        print("  (none)")
        return
    print(f"  {'id':>5} {'mous_id':22} {'source':20} {'spw':>3} {'species':20} {'shorthand':11} "
          f"{'transition':24} {'freq(GHz)':>10} {'vel(km/s)':>9} {'peakrms':>9} {'chan range':>14} "
          f"{'blend':>6} {'status':10}")
    print("  " + SEP)
    for r in rows:
        species = r['species_name'] or r['formula'] or ''
        shorthand = shorthand_for(r['species_name'], r['frequency_ghz']) or ''
        transition = str(r['transition'] or '')
        vel = r['velocity_kms']
        vel_str = f"{vel:9.2f}" if vel is not None else f"{'—':>9}"
        sc, ec = r['startchan'], r['endchan']
        chan_str = f"{sc}-{ec}" if sc is not None and ec is not None else "—"
        print(f"  {r['line_admit_detection_id']:>5} {r['mous_id']:22.22} {r['target_name']:20.20} {r['spw_idx']:>3} "
              f"{species:20.20} {shorthand:11.11} "
              f"{transition:24.24} {r['frequency_ghz']:>10.4f} {vel_str} {r['peakrms_sigma']:>9.2f} "
              f"{chan_str:>14} {r['blend_id']:>6} {r['review_status']:10}")


def _sort_by_chanrange(rows):
    """Within each (mous, source, spw), cluster detections by overlapping channel ranges.

    Units are ordered by their strongest detection. Within each unit, clusters appear
    in startchan order, strongest-first within each cluster. Detections with no channel
    info are treated as their own cluster.
    """
    units: dict = {}
    for r in rows:
        key = (r["mous_id"], r["target_name"], r["spw_idx"])
        units.setdefault(key, []).append(r)

    sorted_rows = []
    unit_keys = sorted(units, key=lambda k: max(r["peakrms_sigma"] for r in units[k]), reverse=True)

    for key in unit_keys:
        unit_rows = sorted(units[key], key=lambda r: (r["startchan"] if r["startchan"] is not None else 0))

        clusters: list = []
        cur_cluster: list = []
        cur_end: int | None = None

        for r in unit_rows:
            sc, ec = r["startchan"], r["endchan"]
            if sc is None or ec is None:
                if cur_cluster:
                    clusters.append(cur_cluster)
                    cur_cluster = []
                    cur_end = None
                clusters.append([r])
                continue
            if cur_end is None or sc <= cur_end + 1:
                cur_cluster.append(r)
                cur_end = max(cur_end, ec) if cur_end is not None else ec
            else:
                clusters.append(cur_cluster)
                cur_cluster = [r]
                cur_end = ec
        if cur_cluster:
            clusters.append(cur_cluster)

        for cluster in clusters:
            cluster.sort(key=lambda r: r["peakrms_sigma"], reverse=True)
            sorted_rows.extend(cluster)

    return sorted_rows


def _rows_with_unit_info(conn, detections):
    """Join detections back to line_admit_pipeline_state for mous/source/spw.

    The unit table is read once into a dict rather than queried per detection:
    the per-row version issued thousands of round-trips against a DB on network
    storage, which dominated the runtime of every listing.
    """
    units = {
        u["line_admit_unit_id"]: u
        for u in conn.execute(
            "SELECT line_admit_unit_id, mous_id, target_name, spw_idx "
            "FROM line_admit_pipeline_state"
        ).fetchall()
    }
    out = []
    for d in detections:
        unit = units[d["line_admit_unit_id"]]
        merged = dict(d)
        merged["mous_id"] = unit["mous_id"]
        merged["target_name"] = unit["target_name"]
        merged["spw_idx"] = unit["spw_idx"]
        out.append(merged)
    return out


def _parse_detection_ids(values) -> list[int]:
    """Flatten space- and/or comma-separated detection ids into ints, order preserved.

    Accepts "--confirm 1 2 3", "--confirm 1,2,3", and mixtures of the two.
    Duplicates are collapsed so an id repeated on the command line is written once.
    """
    ids: list[int] = []
    for value in values:
        for piece in str(value).replace(",", " ").split():
            try:
                parsed = int(piece)
            except ValueError:
                print(f"[ERROR] Not a valid detection id: {piece!r}")
                sys.exit(1)
            if parsed not in ids:
                ids.append(parsed)
    if not ids:
        print("[ERROR] No detection ids given.")
        sys.exit(1)
    return ids


def _warn_missing_dets(conn, wanted: list[int], rows, status: str) -> None:
    """Explain any --det id that will not appear in the listing.

    Silence would be ambiguous here: a missing id could be a typo, a detection
    filtered out by --status, or one excluded by --species/--peakrms/etc. Each
    needs a different fix, so say which it is.
    """
    shown = {r["line_admit_detection_id"] for r in rows}
    missing = [d for d in wanted if d not in shown]
    if not missing:
        return

    absent, wrong_status, filtered = [], [], []
    for det_id in missing:
        row = db.get_line_admit_detection(conn, det_id)
        if row is None:
            absent.append(det_id)
        elif status != "all" and row["review_status"] != status:
            wrong_status.append((det_id, row["review_status"]))
        else:
            filtered.append(det_id)

    if absent:
        print(f"[WARN] no detection with id: {', '.join(str(d) for d in absent)}")
    for det_id, actual in wrong_status:
        print(f"[WARN] detection {det_id} is '{actual}', excluded by --status {status} "
              f"(use --status all to see it)")
    if filtered:
        print(f"[WARN] excluded by the other filters: "
              f"{', '.join(str(d) for d in filtered)}")


def _apply_review(conn, values, review_status: str, note: str | None) -> None:
    """Set review_status on one or more detections, sharing a single note.

    Every id is validated before anything is written: a typo in one id would
    otherwise leave the batch half-applied, which is worse to unpick than an
    outright failure.
    """
    ids = _parse_detection_ids(values)

    rows, missing = {}, []
    for det_id in ids:
        row = db.get_line_admit_detection(conn, det_id)
        if row is None:
            missing.append(det_id)
        else:
            rows[det_id] = row
    if missing:
        print(f"[ERROR] No detection with id: {', '.join(str(m) for m in missing)}")
        print("        Nothing was written -- fix the id(s) and re-run.")
        sys.exit(1)

    changed = unchanged = 0
    for det_id in ids:
        row = rows[det_id]
        was = row["review_status"]
        shorthand = shorthand_for(row["species_name"], row["frequency_ghz"]) or row["formula"] or "?"
        if was == review_status:
            print(f"  [SKIP] {det_id:>6}  {shorthand:<16} already {review_status}")
            unchanged += 1
            continue
        db.update_detection_review(conn, det_id, review_status, note)
        print(f"  [OK]   {det_id:>6}  {shorthand:<16} {was} → {review_status}")
        changed += 1

    summary = f"[OK] {changed} detection(s) → {review_status}"
    if unchanged:
        summary += f"; {unchanged} already {review_status}"
    print(summary)


def main() -> None:
    parser = argparse.ArgumentParser(description="List and review ADMIT line detections.")
    parser.add_argument("--mous-id", default=None)
    parser.add_argument("--source", default=None, help="Target/field name")
    parser.add_argument("--spw", type=int, default=None)
    parser.add_argument("--status", default=None, choices=["pending", "confirmed", "rejected", "all"],
                         help="Filter by review_status when listing (default: pending, "
                              "or 'all' when --det names specific ids)")
    parser.add_argument("--det", nargs="+", default=None, metavar="DETECTION_ID",
                         help="List only these detection ids, space- and/or comma-separated. "
                              "Combines with every other filter. Implies --status all unless "
                              "--status is given explicitly, so named ids show up whatever "
                              "their review state.")
    parser.add_argument("--confirm", nargs="+", default=None, metavar="DETECTION_ID",
                         help="One or more detection ids, space- and/or comma-separated")
    parser.add_argument("--reject", nargs="+", default=None, metavar="DETECTION_ID",
                         help="One or more detection ids, space- and/or comma-separated")
    parser.add_argument("--note", default=None,
                         help="Optional note for --confirm/--reject; applied to every id given")
    parser.add_argument("--reject-below", type=float, default=None, metavar="SIGMA",
                         help="Bulk-reject all pending detections with peakrms_sigma below SIGMA")
    parser.add_argument("--dry-run", action="store_true",
                         help="With --reject-below: show what would be rejected without writing")
    parser.add_argument("--purge-triage-cube", action="store_true",
                         help="Delete the triage cube for --mous-id/--source/--spw if every detection is rejected")
    parser.add_argument("--include-confirmed", action="store_true",
                         help="With --purge-triage-cube: also purge SPWs where some detections were confirmed")
    parser.add_argument("--species", default=None,
                         help="Filter by shorthand species name, case-insensitive, transition stripped "
                              "(e.g. HC3N matches HC3N(12-11); HCO+ matches HCO+(3-2) and DCO+(3-2) "
                              "won't match HCO+ — use DCO+ for that)")
    parser.add_argument("--peakrms-min", type=float, default=None, metavar="SIGMA",
                         help="Only show detections with peakrms_sigma >= SIGMA")
    parser.add_argument("--peakrms-max", type=float, default=None, metavar="SIGMA",
                         help="Only show detections with peakrms_sigma <= SIGMA")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                         help="Show only the top N detections by peakrms_sigma (strongest first)")
    parser.add_argument("--sort", default="sigma", choices=["sigma", "chanrange"],
                         help="Sort order: 'sigma' (default, strongest first) or 'chanrange' "
                              "(grouped by overlapping channel ranges within each SPW unit, "
                              "units ordered by strongest detection)")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    if args.confirm is not None:
        _apply_review(conn, args.confirm, "confirmed", args.note)
        conn.close()
        return

    if args.reject is not None:
        _apply_review(conn, args.reject, "rejected", args.note)
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
                note = f"auto-rejected: peakrms_sigma={r['peakrms_sigma']:.2f} < threshold {args.reject_below}"
                if args.note:
                    note += f"; {args.note}"
                db.update_detection_review(conn, r["line_admit_detection_id"], "rejected", note)
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

    # A named id should be visible whatever its review state, so --det flips the
    # default to 'all'. An explicit --status still wins.
    if args.status is None:
        args.status = "all" if args.det else "pending"

    status = None if args.status == "all" else args.status
    detections = db.get_line_admit_detections(conn, review_status=status)

    # mous_id/source/spw aren't columns on line_admit_detection itself, so
    # filtering on them happens after joining to the parent unit below.
    wanted = None
    if args.det is not None:
        wanted = _parse_detection_ids(args.det)
        detections = [d for d in detections
                      if d["line_admit_detection_id"] in wanted]

    rows = _rows_with_unit_info(conn, detections)

    if args.mous_id:
        rows = [r for r in rows if r["mous_id"] == args.mous_id]
    if args.source:
        rows = [r for r in rows if r["target_name"] == args.source]
    if args.spw is not None:
        rows = [r for r in rows if r["spw_idx"] == args.spw]
    if args.species is not None:
        target = args.species.split("(")[0].upper()
        rows = [r for r in rows if (shorthand_for(r["species_name"], r["frequency_ghz"]) or "").split("(")[0].upper() == target]

    if args.peakrms_min is not None:
        rows = [r for r in rows if r["peakrms_sigma"] >= args.peakrms_min]
    if args.peakrms_max is not None:
        rows = [r for r in rows if r["peakrms_sigma"] <= args.peakrms_max]

    if wanted is not None:
        _warn_missing_dets(conn, wanted, rows, args.status)

    if args.sort == "chanrange":
        rows = _sort_by_chanrange(rows)
    else:
        rows.sort(key=lambda r: r["peakrms_sigma"], reverse=True)

    conn.close()

    total = len(rows)
    if args.limit is not None:
        rows = rows[:args.limit]
    label = f"n={total}" if args.limit is None else f"showing {len(rows)} of {total}"
    range_tag = ""
    if args.peakrms_min is not None and args.peakrms_max is not None:
        range_tag = f", peakrms={args.peakrms_min}–{args.peakrms_max}σ"
    elif args.peakrms_min is not None:
        range_tag = f", peakrms>={args.peakrms_min}σ"
    elif args.peakrms_max is not None:
        range_tag = f", peakrms<={args.peakrms_max}σ"
    print(f"Detections (status={args.status}, {label}{range_tag}, sort={args.sort}):")
    _print_detections(rows)


if __name__ == "__main__":
    main()
