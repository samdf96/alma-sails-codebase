"""
manual_split.py
---------------
Manually run the split step for a MOUS from inside a CASA container.

Splits calibrated measurement sets to target-only science SPWs, runs listobs
on both originals and outputs, deletes the originals, and advances the MOUS
to split_complete.

The data column is auto-detected per MS: CORRECTED_DATA is used if present
(standard for SRDP pipeline products), otherwise DATA. Override with
--datacolumn if needed.

Usage (inside a CASA interactive container):
    casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_split.py --mous-id uid://A001/X.../X...

Optional:
    --datacolumn corrected        (override auto-detection)
    --keep-originals              (do not delete the calibrated MSes after splitting)
    --logfile /path/to/casa.log  (override auto-generated log path in MOUS directory)
    --db-path /path/to/db
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.split import (_find_calibrated_ms, check_spw_consistency,
                            format_spw_report, run_split)


def main(
    mous_id: str,
    datacolumn: str,
    db_path: Path,
    logfile: str | None = None,
    spw_override: str | None = None,
    keep_originals: bool = False,
) -> None:
    conn = db.get_connection(db_path)

    # ------------------------------------------------------------------
    # Pre-flight
    # ------------------------------------------------------------------
    state_row = db.get_pipeline_state(conn, mous_id)
    if state_row is None:
        print(f"[ERROR] MOUS {mous_id} not found in database")
        sys.exit(1)

    current_state = state_row["current_state"]
    if current_state != "downloaded":
        print(f"[ERROR] Expected state 'downloaded', got '{current_state}' — aborting")
        sys.exit(1)

    mous_directory = db.get_mous_directory(conn, mous_id)
    if mous_directory is None:
        print(f"[ERROR] mous_directory not set for {mous_id}")
        sys.exit(1)

    if not mous_directory.exists():
        print(f"[ERROR] mous_directory does not exist on disk: {mous_directory}")
        db.record_failure(conn, mous_id=mous_id, from_state="downloaded",
                          error=f"mous_directory missing: {mous_directory}", step="split")
        sys.exit(1)

    if spw_override:
        try:
            spws = [int(x.strip()) for x in spw_override.split(",")]
        except ValueError:
            print(f"[ERROR] --spw value could not be parsed as integers: {spw_override!r}")
            sys.exit(1)
        print(f"[manual-split] SPW override active — using {spws} (ignoring alminer_metadata)")
    else:
        spws = db.get_mous_science_spws(conn, mous_id)
        if not spws:
            print(f"[ERROR] No science SPWs found in alminer_metadata for {mous_id}")
            db.record_failure(conn, mous_id=mous_id, from_state="downloaded",
                              error="no science SPWs in alminer_metadata", step="split")
            sys.exit(1)

    field_names = db.get_unique_target_names(conn, mous_id)
    if not field_names:
        print(f"[ERROR] No target field names found in targets table for {mous_id}")
        db.record_failure(conn, mous_id=mous_id, from_state="downloaded",
                          error="no target field names in targets table", step="split")
        sys.exit(1)

    # Redirect CASA log to MOUS directory (or caller-supplied path)
    from datetime import datetime
    log_path = Path(logfile) if logfile else mous_directory / f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-split.log"
    casalog.setlogfile(str(log_path))  # noqa: F821 — casalog is a CASA global

    # ------------------------------------------------------------------
    # SPW pre-flight. Execution blocks within one MOUS may number their
    # spectral windows differently, and the neighbouring numbers are valid
    # 1-channel CH_AVG windows, so splitting the wrong ones does not raise --
    # it quietly writes almost no data. Check before anything is created, and
    # stop here rather than recording a failure, since nothing was attempted.
    # ------------------------------------------------------------------
    if spw_override:
        print("[manual-split] SPW override active — skipping per-MS SPW resolution")
        plan = None
    else:
        calibrated = _find_calibrated_ms(mous_directory)
        if not calibrated:
            print(f"[ERROR] no calibrated .ms files found in {mous_directory}")
            sys.exit(1)
        plan = check_spw_consistency(calibrated, expected_spws=spws)
        print(format_spw_report(plan))
        if plan["problems"]:
            print()
            print("[ABORT] The measurement sets for this MOUS do not agree on their")
            print("        science spectral windows, so a single SPW selection cannot")
            print("        be correct for all of them. Nothing was split and the state")
            print("        is unchanged — this needs a look before proceeding.")
            print("        To force a specific selection anyway, re-run with --spw.")
            sys.exit(1)

    print(f"[manual-split] MOUS {mous_id} confirmed downloaded")
    print(f"[manual-split] MOUS directory: {mous_directory}")
    print(f"[manual-split] Science SPWs:   {spws}{' (override)' if spw_override else ''}")
    print(f"[manual-split] Target fields:  {field_names}")
    print(f"[manual-split] Data column:    {datacolumn}")
    print(f"[manual-split] Originals:      {'kept' if keep_originals else 'deleted after split'}")
    print(f"[manual-split] CASA log:       {log_path}")

    # ------------------------------------------------------------------
    # Run split
    # ------------------------------------------------------------------
    print(f"[manual-split] Starting split...")
    result = run_split(
        mous_id=mous_id,
        mous_directory=mous_directory,
        spws=spws,
        field_names=field_names,
        datacolumn=datacolumn,
        keep_originals=keep_originals,
        resolve_spws=not spw_override,
    )

    # ------------------------------------------------------------------
    # Record outcome
    # ------------------------------------------------------------------
    if not result.success:
        print(f"[ERROR] Split failed: {result.error}")
        db.record_failure(conn, mous_id=mous_id, from_state="downloaded",
                          error=result.error or "unknown", step="split")
        sys.exit(1)

    split_paths = [str(p) for p in result.split_products]
    db.update_pipeline_paths(conn, mous_id, split_products=split_paths)
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state="downloaded",
        to_state="split_complete",
        payload={
            "split_products":   split_paths,
            "field_names":      field_names,
            "spws_original":    spws,
            "spw_map":          result.spw_map,
            "spw_map_by_ms":    result.spw_map_by_ms,
            "spws_resolved_per_ms": (
                {k: v for k, v in
                 ((ms.name, sp) for ms, sp in plan["per_ms"].items())} if plan else None),
            "spw_numbering_differs": bool(plan and plan["renumbered"]),
            "datacolumn":       datacolumn,
            "keep_originals":   keep_originals,
            "spw_override":     spw_override,
            "n_input_ms":       len(result.split_products),
            "duration_s":       result.duration_s,
        },
        step="split",
    )

    print(f"[manual-split] Split complete in {result.duration_s:.1f}s")
    if result.spw_map_by_ms and len(set(map(str, result.spw_map_by_ms.values()))) > 1:
        print("[manual-split] SPW maps (original → new index), per MS:")
        for name, m in result.spw_map_by_ms.items():
            print(f"         {name}: {m}")
    else:
        print(f"[manual-split] SPW map (original → new index): {result.spw_map}")
    print(f"[manual-split] Products ({len(result.split_products)}):")
    for p in result.split_products:
        print(f"         {p}")
    print(f"[OK]   {mous_id} → split_complete")

    # TP datasets skip selfcal entirely — advance immediately so imaging can proceed
    mous_row = db.get_mous(conn, mous_id)
    if mous_row and mous_row["array_config"] == "tp":
        db.record_transition(
            conn,
            mous_id=mous_id,
            from_state="split_complete",
            to_state="tp_ready",
            note="TP dataset: selfcal not applicable to single-dish data",
            step="split",
        )
        print(f"[OK]   {mous_id} → tp_ready (TP, selfcal not applicable)")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manually run the CASA split step for a MOUS."
    )
    parser.add_argument("--mous-id",     required=True,     help="MOUS ID (uid://A001/...)")
    parser.add_argument("--datacolumn",  default=None,      help="Data column to split (default: auto-detect per MS)")
    parser.add_argument("--logfile",     default=None,      help="CASA log file path (default: auto-generated in MOUS directory)")
    parser.add_argument("--db-path",     default=DB_PATH,   help="Path to the SQLite database")
    parser.add_argument("--spw",         default=None,      help="Comma-separated SPW IDs to override alminer metadata (e.g. '21,23,25,27,29')")
    parser.add_argument("--keep-originals", action="store_true",
                        help="Leave the calibrated MSes in place instead of deleting them after a "
                             "successful split. Use when they are the only copy, or until the split "
                             "output has been verified")
    args = parser.parse_args()

    main(
        mous_id=args.mous_id,
        datacolumn=args.datacolumn,
        logfile=args.logfile,
        db_path=Path(args.db_path),
        spw_override=args.spw,
        keep_originals=args.keep_originals,
    )
