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
from alma_ops.split import run_split


def main(
    mous_id: str,
    datacolumn: str,
    db_path: Path,
    logfile: str | None = None,
    spw_override: str | None = None,
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
        print(f"[INFO] SPW override active — using {spws} (ignoring alminer_metadata)")
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

    print(f"[INFO] MOUS {mous_id} confirmed downloaded")
    print(f"[INFO] MOUS directory: {mous_directory}")
    print(f"[INFO] Science SPWs:   {spws}{' (override)' if spw_override else ''}")
    print(f"[INFO] Target fields:  {field_names}")
    print(f"[INFO] Data column:    {datacolumn}")
    print(f"[INFO] CASA log:       {log_path}")

    # ------------------------------------------------------------------
    # Run split
    # ------------------------------------------------------------------
    print(f"[INFO] Starting split...")
    result = run_split(
        mous_id=mous_id,
        mous_directory=mous_directory,
        spws=spws,
        field_names=field_names,
        datacolumn=datacolumn,
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
            "datacolumn":       datacolumn,
            "spw_override":     spw_override,
            "n_input_ms":       len(result.split_products),
            "duration_s":       result.duration_s,
        },
        step="split",
    )

    print(f"[INFO] Split complete in {result.duration_s:.1f}s")
    print(f"[INFO] SPW map (original → new index): {result.spw_map}")
    print(f"[INFO] Products ({len(result.split_products)}):")
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
    args = parser.parse_args()

    main(
        mous_id=args.mous_id,
        datacolumn=args.datacolumn,
        logfile=args.logfile,
        db_path=Path(args.db_path),
        spw_override=args.spw,
    )
