"""
manual_line_triage_imaging.py
------------------------------
User-facing CASA script for ADMIT-bound line-triage cube imaging (v2).

Run from a CANFAR c-casa container:

    casa --nologger --nogui -c alma-sails-codebase/scripts/manual_line_triage_imaging.py \\
        --mous-id uid://A002/X628157/X2d \\
        --source Serpens_Main \\
        [--spw 3] \\
        [--linewidth-floor-kms 0.3] \\
        [--force] \\
        [--dry-run] \\
        [--keep-casa-products] \\
        [--db-path /arc/.../db/serpens_v2.db]

For the given MOUS, classifies every spw via its auto_selfcal/cont.dat
(continuum-only spws are skipped) and produces one dirty, channel-binned
cube per line-candidate spw for ADMIT to scan. Runs entirely per-MOUS — spw
coverage is not assumed to match across different MOUSes of the same source.

Images the continuum-subtracted selfcal products (selfcal_products_sub /
*_targets.contsub.ms), not the plain selfcal-corrected MS — matching the
precedent in talk_prep/line_imaging/line_imaging_driver.py. Without
continuum subtraction, a strong continuum source's dirty-beam sidelobes
appear identically in every channel, and the leftover continuum pedestal
makes per-channel S/N harder to judge when scanning for lines.

The MOUS must be in pipeline state 'selfcal_complete'. Output is written to
imaging/lines/triage/<source>/B<band>_<config>/.
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH, LINE_TRIAGE_DIR
from alma_ops.imaging.line_triage_config import THERMAL_LINEWIDTH_FLOOR_KMS
from alma_ops.utils import to_dir_mous_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually run ADMIT-bound line-triage cube imaging for one MOUS."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS id (e.g. 'uid://A002/X628157/X2d')")
    parser.add_argument("--source", required=True, help="Target/field name within this MOUS")
    parser.add_argument("--spw", type=int, default=None, help="Restrict to a single spw index")
    parser.add_argument(
        "--linewidth-floor-kms", type=float, default=THERMAL_LINEWIDTH_FLOOR_KMS,
        help=f"Narrowest protected line FWHM in km/s (default {THERMAL_LINEWIDTH_FLOOR_KMS})",
    )
    parser.add_argument("--force", action="store_true", help="Re-image spws already marked 'complete'")
    parser.add_argument("--dry-run", action="store_true", help="Print the imaging plan without running tclean")
    parser.add_argument("--keep-casa-products", action="store_true",
                        help="Keep CASA-format intermediates after FITS export (default: delete them)")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    # --- Validate MOUS state ---
    ps = db.get_pipeline_state(conn, args.mous_id)
    if ps is None:
        print(f"[ERROR] No pipeline_state row for mous_id={args.mous_id}")
        conn.close()
        sys.exit(1)
    if ps["current_state"] != "selfcal_complete":
        print(f"[ERROR] MOUS is in state '{ps['current_state']}'; expected 'selfcal_complete'.")
        conn.close()
        sys.exit(1)

    target_row = next(
        (r for r in db.get_mous_targets(conn, args.mous_id) if r["alma_source_name"] == args.source),
        None,
    )
    if target_row is None:
        print(f"[ERROR] Source '{args.source}' not found among targets for {args.mous_id}.")
        conn.close()
        sys.exit(1)
    is_mosaic = target_row["is_mosaic"] == "T"

    ms_paths = [str(p) for p in db.get_selfcal_products(conn, args.mous_id, continuum_subtracted=True)]
    if not ms_paths:
        print(f"[ERROR] No continuum-subtracted selfcal products (selfcal_products_sub) for {args.mous_id}.")
        conn.close()
        sys.exit(1)

    mous_dir = db.get_mous_directory(conn, args.mous_id)
    cont_dat = mous_dir / "auto_selfcal" / "cont.dat" if mous_dir else None
    if cont_dat is None or not cont_dat.exists():
        print(f"[ERROR] cont.dat not found for {args.mous_id} — has selfcal completed?")
        conn.close()
        sys.exit(1)

    mous_row = db.get_mous(conn, args.mous_id)
    band, config_type = mous_row["band"], mous_row["array_config"]
    mous_dir_id = to_dir_mous_id(args.mous_id)

    if config_type == "tp":
        print("[ERROR] Total-power (tp) MOUSes require sdimaging, not tclean — "
              "not supported by this script.")
        conn.close()
        sys.exit(1)

    # --- Classify spws ---
    from alma_ops.imaging.line_triage_utils import classify_spws, compute_bin_factor

    classification = classify_spws(cont_dat, args.source, ms_paths[0])
    line_spws = sorted(idx for idx, cls in classification.items() if cls == "line")
    if args.spw is not None:
        if args.spw not in line_spws:
            print(f"[ERROR] spw {args.spw} is not classified as a line candidate "
                  f"(classification: {classification.get(args.spw, 'not present')}).")
            conn.close()
            sys.exit(1)
        line_spws = [args.spw]

    img_dir = LINE_TRIAGE_DIR / args.source / f"B{band}_{config_type}"

    print(f"[line-triage] {args.mous_id}  source={args.source}  B{band}  {config_type}")
    print(f"  imaging_dir : {img_dir}")
    print(f"  cont.dat    : {cont_dat}")
    print(f"  is_mosaic   : {is_mosaic}")
    print(f"  spws        : {sorted(classification)}  "
          f"(continuum: {sorted(i for i, c in classification.items() if c == 'continuum')}, "
          f"line candidates: {line_spws})")

    if not line_spws:
        unit_id = db.get_or_create_line_triage_unit(conn, args.mous_id, args.source, -1)
        sentinel_state = db.get_line_triage_pipeline_state(conn, unit_id)["current_state"]
        if sentinel_state == "complete" and not args.force:
            print("[OK] No line-candidate spws — already recorded as continuum-only.")
        else:
            print("[OK] No line-candidate spws (all continuum) — recording sentinel.")
            db.record_line_triage_transition(
                conn, unit_id,
                from_state=sentinel_state, to_state="complete",
                run_id=str(uuid.uuid4()),
                note="no line-candidate spws — all continuum",
                step="complete",
                triage_products={"continuum_only": True,
                                 "completed_at": datetime.now(timezone.utc).isoformat()},
                payload={"note": "no line-candidate spws", "classification": classification},
            )
        conn.close()
        return

    img_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for spw_idx in line_spws:
        bin_info = compute_bin_factor(ms_paths[0], spw_idx, linewidth_floor_kms=args.linewidth_floor_kms)
        native_width_kms = bin_info["native_chanwidth_hz"] / bin_info["center_freq_hz"] * 299_792.458
        print(f"  [spw {spw_idx}] native_chanwidth={native_width_kms:.4f} km/s  "
              f"bin_factor={bin_info['bin_factor']}  nchan={bin_info['nchan']}  "
              f"center_freq={bin_info['center_freq_hz'] / 1e9:.3f} GHz")

        if args.dry_run:
            results.append((spw_idx, "dry-run"))
            continue

        # --- DB writes start here; nothing above this point touches the DB ---
        unit_id = db.get_or_create_line_triage_unit(conn, args.mous_id, args.source, spw_idx)
        ips = db.get_line_triage_pipeline_state(conn, unit_id)
        current_state = ips["current_state"]

        if current_state == "running":
            print(f"  [spw {spw_idx}] SKIP — already running.")
            results.append((spw_idx, "skipped"))
            continue
        if current_state == "complete" and not args.force:
            print(f"  [spw {spw_idx}] SKIP — already complete (use --force to re-run).")
            results.append((spw_idx, "skipped"))
            continue

        run_id = str(uuid.uuid4())
        db.record_line_triage_transition(
            conn, unit_id, from_state=current_state, to_state="running",
            run_id=run_id, note="manual run", step="start",
        )

        try:
            from alma_ops.imaging.line_triage_utils import run_line_triage_imaging
            products = run_line_triage_imaging(
                mous_dir_id=mous_dir_id,
                target_name=args.source,
                spw_idx=spw_idx,
                ms_paths=ms_paths,
                imaging_dir=str(img_dir),
                bin_factor=bin_info["bin_factor"],
                nchan=bin_info["nchan"],
                is_mosaic=is_mosaic,
                keep_casa_products=args.keep_casa_products,
            )
        except Exception:
            err = traceback.format_exc()
            print(f"  [spw {spw_idx}] [ERROR]\n{err}", flush=True)
            db.record_line_triage_failure(
                conn, unit_id, from_state="running", error=err[-4000:],
                run_id=run_id, step="imaging",
            )
            results.append((spw_idx, "failed"))
            continue

        products_entry = dict(products)
        products_entry["bin_factor"] = bin_info["bin_factor"]
        products_entry["native_chanwidth_hz"] = bin_info["native_chanwidth_hz"]
        products_entry["center_freq_hz"] = bin_info["center_freq_hz"]
        products_entry["linewidth_floor_kms"] = args.linewidth_floor_kms
        products_entry["gridder"] = "mosaic" if is_mosaic else "standard"
        products_entry["completed_at"] = datetime.now(timezone.utc).isoformat()

        db.record_line_triage_transition(
            conn, unit_id, from_state="running", to_state="complete",
            run_id=run_id, note="triage imaging complete", step="complete",
            triage_products=products_entry,
            payload={"triage_products": products_entry, "imaging_dir": str(img_dir)},
        )
        results.append((spw_idx, "complete"))

    conn.close()

    print("\n[summary]")
    for spw_idx, status in results:
        print(f"  spw {spw_idx}: {status}")


if __name__ == "__main__":
    main()
