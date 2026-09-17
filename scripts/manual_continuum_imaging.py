"""
manual_continuum_imaging.py
---------------------------
User-facing CASA script for ALMA-SAILS continuum imaging (v2).

Run from a CANFAR c-casa container:

    casa --nologger --nogui -c alma-sails-codebase/scripts/manual_continuum_imaging.py \\
        --source EC_53 \\
        --band 6 \\
        --config 12m \\
        [--robust 0.5] \\
        [--dry-run] \\
        [--db-path /arc/.../db/serpens_v2.db]

Looks up the imaging unit by (source, band, config_type), assembles ms_paths
from selfcal_products and cont_dat_path from auto_selfcal/cont.dat for all
contributing MOUSes, then runs tclean + impbcor + exportfits.

The imaging unit must be in state 'ready' or 'failed' (retry) before running.
Output is written to imaging/continuum/<source>/B<band>_<config>/.

Required args
-------------
--source    Target/source name (must match target_name in the imaging_unit table).
--band      Band number as a string: '3', '6', '7', etc.
--config    Config type: '12m', '7m', 'tp', or 'combined'.

Optional args
-------------
--robust              Briggs robust weighting (default 0.5).
--dry-run             Print the imaging plan without modifying the DB or running tclean.
--keep-casa-products  Keep CASA-format intermediates (.image, .pb, .psf, etc.) after FITS export.
--db-path             Path to serpens_v2.db (default: alma_ops.config.DB_PATH).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.utils import code_version
from alma_ops.config import CONTINUUM_IMAGING_DIR, DB_PATH


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually run ALMA-SAILS continuum imaging for one imaging unit."
    )
    parser.add_argument("--source", required=True, help="Source/target name (e.g. 'EC_53')")
    parser.add_argument("--band",   required=True, help="Band number (e.g. '6', '7')")
    parser.add_argument("--config", required=True, help="Config type: 12m, 7m, tp, combined")
    parser.add_argument("--robust", type=float, default=0.5, help="Briggs robust (default 0.5)")
    parser.add_argument("--force", action="store_true",
                        help="Delete existing products for this robust value and re-image")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without running tclean")
    parser.add_argument("--keep-casa-products", action="store_true",
                        help="Keep CASA-format intermediates after FITS export (default: delete them)")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    # --- Look up the imaging unit directly by (source, band, config) ---
    imaging_unit = db.get_imaging_unit(conn, args.source, args.band, args.config)
    if imaging_unit is None:
        print(f"[ERROR] No imaging unit found for source='{args.source}'  band={args.band}  config={args.config}")
        print("  Check that the source name matches exactly what is in the imaging_unit table.")
        conn.close()
        sys.exit(1)

    imaging_unit_id = imaging_unit["imaging_unit_id"]
    field_name      = imaging_unit["target_name"]

    # --- Check imaging pipeline state ---
    ips = db.get_imaging_pipeline_state(conn, imaging_unit_id)
    if ips is None:
        print(f"[ERROR] No imaging_pipeline_state row for imaging_unit_id={imaging_unit_id}")
        conn.close()
        sys.exit(1)

    current_state = ips["current_state"]
    if current_state not in ("ready", "failed", "complete"):
        print(f"[ERROR] Imaging unit is in state '{current_state}'; expected 'ready', 'failed', or 'complete'.")
        if current_state == "running":
            print("  Another run is in progress. Wait for it to finish or reset state manually.")
        conn.close()
        sys.exit(1)

    existing_products = json.loads(ips["imaging_products"] or "[]")
    if isinstance(existing_products, dict):
        existing_products = [existing_products]
    already_done = [e for e in existing_products if e.get("robust") == args.robust]
    if already_done:
        if not args.force:
            print(f"[ERROR] Products for robust={args.robust} already exist:")
            for key in ("image_pbcor_fits", "image_pbcor", "image_fits", "image"):
                print(f"  {key}: {already_done[0].get(key, 'n/a')}")
            print("  Pass --force to delete them and re-image.")
            conn.close()
            sys.exit(1)
        for entry in already_done:
            for key in ("image_pbcor_fits", "image_fits", "image_pbcor", "image"):
                path = entry.get(key)
                if path:
                    p = Path(db.to_local(path))
                    if p.exists():
                        shutil.rmtree(str(p)) if p.is_dir() else p.unlink()
                        print(f"[imaging] --force: removed {p.name}")
        db.remove_imaging_products_entry(conn, imaging_unit_id, args.robust)

    # --- Collect contributing MOUSes and build mous_list ---
    mous_rows = db.get_mous_for_imaging_unit(conn, imaging_unit_id)
    mous_list = []

    for row in mous_rows:
        contributing_mous_id = row["mous_id"]
        ms_paths = db.get_selfcal_products(conn, contributing_mous_id, continuum_subtracted=False)
        mous_dir = db.get_mous_directory(conn, contributing_mous_id)

        if not ms_paths:
            print(f"  [WARNING] No selfcal products for {contributing_mous_id} — skipping.")
            continue

        cont_dat = mous_dir / "auto_selfcal" / "cont.dat" if mous_dir else None
        if cont_dat is None or not cont_dat.exists():
            print(f"  [WARNING] cont.dat not found for {contributing_mous_id} — skipping.")
            continue

        # source_field_name/field_ids are usually NULL -- only set for imaging
        # units that select a subset of fields out of a shared-name mosaic
        # MOUS (see migrate_add_field_selection_columns.py).
        source_field_name = row["source_field_name"]
        field_ids = json.loads(row["field_ids"]) if row["field_ids"] else None

        mous_list.append({
            "mous_id":           contributing_mous_id,
            "ms_paths":          [str(p) for p in ms_paths],
            "cont_dat_path":     str(cont_dat),
            "source_field_name": source_field_name,
            "field_ids":         field_ids,
        })

    if not mous_list:
        print("[ERROR] No usable MOUS entries — check selfcal_products and cont.dat in DB.")
        conn.close()
        sys.exit(1)

    # Always look up is_mosaic via the imaging unit's own target_name (field_name).
    # source_field_name is the parent MOUS's MS field name (e.g.
    # "Serpens_Main_and_Serpens_South") — inheriting its mosaic flag would
    # incorrectly mark a single extracted field as a mosaic and cause
    # compute_mosaic_imaging_params to span the full parent footprint.
    is_mosaic = any(
        target_row["is_mosaic"] == "T"
        for entry in mous_list
        for target_row in db.get_mous_targets(conn, entry["mous_id"])
        if target_row["alma_source_name"] == field_name
    )

    img_dir = str(CONTINUUM_IMAGING_DIR / field_name / f"B{args.band}_{args.config}")

    # --- Print plan ---
    print(f"[imaging] {field_name}  B{args.band}  {args.config}  robust={args.robust}")
    print(f"  imaging_unit_id : {imaging_unit_id}  (state: {current_state})")
    print(f"  imaging_dir     : {img_dir}")
    print(f"  is_mosaic       : {is_mosaic}")
    print(f"  contributing MOUSes ({len(mous_list)}):")
    for entry in mous_list:
        print(f"    {entry['mous_id']}")
        for p in entry["ms_paths"]:
            print(f"      {Path(p).name}")
        print(f"      cont.dat: {entry['cont_dat_path']}")
        if entry["field_ids"]:
            print(f"      field_ids: {entry['field_ids']}  (source_field_name: {entry['source_field_name']})")

    if args.dry_run:
        print("\n[dry-run] No changes made.")
        conn.close()
        return

    # --- DB: advance to running ---
    run_id = str(uuid.uuid4())
    db.record_imaging_transition(
        conn,
        imaging_unit_id=imaging_unit_id,
        from_state=current_state,
        to_state="running",
        run_id=run_id,
        note="manual run",
        step="start",
    )
    conn.close()

    os.makedirs(img_dir, exist_ok=True)

    # --- Run imaging ---
    try:
        from alma_ops.imaging.imaging_utils import run_continuum_imaging
        products = run_continuum_imaging(
            field_name=field_name,
            band=args.band,
            config_type=args.config,
            imaging_dir=img_dir,
            robust=args.robust,
            mous_list=mous_list,
            keep_casa_products=args.keep_casa_products,
            is_mosaic=is_mosaic,
        )
    except Exception:
        err = traceback.format_exc()
        print(f"[ERROR]\n{err}", flush=True)
        conn = db.get_connection(args.db_path)
        db.record_imaging_failure(
            conn,
            imaging_unit_id=imaging_unit_id,
            from_state="running",
            error=err[-4000:],
            run_id=run_id,
            step="imaging",
        )
        conn.close()
        sys.exit(1)

    # --- DB: advance to complete ---
    products_entry = dict(products)
    products_entry["robust"] = args.robust
    products_entry["completed_at"] = datetime.now(timezone.utc).isoformat()
    # Which code produced this product. Lets a later query tell pre- from
    # post-fix products directly instead of inferring it from timestamps.
    products_entry["code_commit"] = code_version()

    conn = db.get_connection(args.db_path)
    db.record_imaging_transition(
        conn,
        imaging_unit_id=imaging_unit_id,
        from_state="running",
        to_state="complete",
        run_id=run_id,
        note="imaging complete",
        step="complete",
        imaging_products=products_entry,
        payload={"imaging_products": products_entry, "imaging_dir": img_dir, "robust": args.robust},
    )
    conn.close()
    print(f"[OK] {field_name} B{args.band} {args.config} → complete")


if __name__ == "__main__":
    main()