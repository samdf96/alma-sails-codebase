"""
batch_continuum_imaging.py
--------------------------
Randomly selects N imaging-ready units and images them sequentially.

Run from a CANFAR c-casa container:

    casa --nologger --nogui -c alma-sails-codebase/scripts/batch_continuum_imaging.py \\
        --count 5 \\
        [--robust 0.5] \\
        [--dry-run] \\
        [--db-path /arc/.../db/serpens_v2.db]

Queries all imaging units in state 'ready', draws --n at random, and runs
tclean + impbcor + exportfits for each in turn.  A failure on one unit is
logged and the batch continues with the next.

If fewer than --n units are ready, all available units are imaged.

Output for each unit goes to imaging/continuum/<source>/B<band>_<config>/.

Args
----
--count               Number of units to image (required).
--robust              Briggs robust weighting (default 0.5).
--dry-run             Print the selected units and plan without running tclean or changing DB.
--keep-casa-products  Keep CASA-format intermediates (.image, .pb, .psf, etc.) after FITS export.
--db-path             Path to serpens_v2.db (default: alma_ops.config.DB_PATH).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import CONTINUUM_IMAGING_DIR, DB_PATH


def _build_mous_list(conn, imaging_unit_id: int) -> tuple:
    """Return (field_name, mous_list, is_mosaic) for an imaging unit, or (field_name, None, False) if unusable."""
    mous_rows = db.get_mous_for_imaging_unit(conn, imaging_unit_id)
    mous_list = []
    is_mosaic = False

    ips = db.get_imaging_pipeline_state(conn, imaging_unit_id)
    field_name = ips["target_name"]

    for row in mous_rows:
        mous_id  = row["mous_id"]
        ms_paths = db.get_selfcal_products(conn, mous_id, continuum_subtracted=False)
        mous_dir = db.get_mous_directory(conn, mous_id)

        if not ms_paths:
            print(f"    [WARNING] No selfcal products for {mous_id} — skipping this MOUS.")
            continue

        cont_dat = mous_dir / "auto_selfcal" / "cont.dat" if mous_dir else None
        if cont_dat is None or not cont_dat.exists():
            print(f"    [WARNING] cont.dat not found for {mous_id} — skipping this MOUS.")
            continue

        # source_field_name/field_ids are usually NULL -- only set for imaging
        # units that select a subset of fields out of a shared-name mosaic
        # MOUS (see migrate_add_field_selection_columns.py).
        source_field_name = row["source_field_name"]
        field_ids = json.loads(row["field_ids"]) if row["field_ids"] else None

        mous_list.append({
            "mous_id":           mous_id,
            "ms_paths":          [str(p) for p in ms_paths],
            "cont_dat_path":     str(cont_dat),
            "source_field_name": source_field_name,
            "field_ids":         field_ids,
        })

        # Match on source_field_name (the actual MS field name) when set,
        # since for a field-id-subset imaging unit alma_source_name is the
        # shared mosaic name, not the canonical target_name (field_name).
        target_row = next(
            (
                r for r in db.get_mous_targets(conn, mous_id)
                if r["alma_source_name"] == (source_field_name or field_name)
            ),
            None,
        )
        if target_row is not None and target_row["is_mosaic"] == "T":
            is_mosaic = True

    return field_name, (mous_list if mous_list else None), is_mosaic


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch continuum imaging: randomly select and image N ready units."
    )
    parser.add_argument("--count",   required=True, type=int, help="Number of units to image")
    parser.add_argument("--robust",  type=float, default=0.5, help="Briggs robust (default 0.5)")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without running tclean")
    parser.add_argument("--keep-casa-products", action="store_true",
                        help="Keep CASA-format intermediates after FITS export (default: delete them)")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)
    all_ready    = db.get_imaging_units_by_state(conn, "ready")
    all_complete = db.get_imaging_units_by_state(conn, "complete")
    conn.close()

    # Filter complete units to those that haven't been run at this robust value yet
    eligible_complete = []
    for u in all_complete:
        existing = json.loads(u["imaging_products"] or "[]")
        if isinstance(existing, dict):
            existing = [existing]
        if not any(e.get("robust") == args.robust for e in existing):
            eligible_complete.append(u)

    candidates = [u for u in list(all_ready) + eligible_complete if u["config_type"] != "tp"]

    if not candidates:
        print(f"No imaging units available (ready or complete without robust={args.robust}).")
        return

    # Random selection
    n = min(args.count, len(candidates))
    selected = random.sample(candidates, n)

    print(f"Found {len(candidates)} eligible unit(s) (tp excluded). Selected {n} at random:\n")
    for u in selected:
        print(f"  {u['target_name']}  B{u['band']}  {u['config_type']}")
    print()

    if args.dry_run:
        print("[dry-run] No changes made.")
        return

    # --- Sequential imaging ---
    results = {"complete": [], "failed": [], "skipped": []}

    for unit in selected:
        imaging_unit_id = unit["imaging_unit_id"]
        band        = unit["band"]
        config_type = unit["config_type"]

        conn = db.get_connection(args.db_path)
        field_name, mous_list, is_mosaic = _build_mous_list(conn, imaging_unit_id)
        conn.close()

        print(f"{'='*60}")
        print(f"[imaging] {field_name}  B{band}  {config_type}  robust={args.robust}")
        if is_mosaic:
            print(f"  is_mosaic: True")

        if not mous_list:
            print(f"  [SKIP] No usable MOUS entries — check selfcal_products and cont.dat.")
            results["skipped"].append(f"{field_name} B{band} {config_type}")
            continue

        img_dir = str(CONTINUUM_IMAGING_DIR / field_name / f"B{band}_{config_type}")
        run_id  = str(uuid.uuid4())

        from_state = unit["current_state"]

        conn = db.get_connection(args.db_path)
        db.record_imaging_transition(
            conn,
            imaging_unit_id=imaging_unit_id,
            from_state=from_state,
            to_state="running",
            run_id=run_id,
            note="batch run",
            step="start",
        )
        conn.close()

        os.makedirs(img_dir, exist_ok=True)

        try:
            from alma_ops.imaging.imaging_utils import run_continuum_imaging
            products = run_continuum_imaging(
                field_name=field_name,
                band=band,
                config_type=config_type,
                imaging_dir=img_dir,
                robust=args.robust,
                mous_list=mous_list,
                keep_casa_products=args.keep_casa_products,
                is_mosaic=is_mosaic,
            )
            products_entry = dict(products)
            products_entry["robust"] = args.robust
            products_entry["gridder"] = "mosaic" if is_mosaic else "standard"
            products_entry["completed_at"] = datetime.now(timezone.utc).isoformat()

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
            results["complete"].append(f"{field_name} B{band} {config_type}")
            print(f"  [OK] → complete")

        except Exception:
            err = traceback.format_exc()
            print(f"  [ERROR]\n{err}", flush=True)
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
            results["failed"].append(f"{field_name} B{band} {config_type}")

    # --- Summary ---
    print(f"\n{'='*60}")
    print(f"Batch complete.  {len(results['complete'])} done, "
          f"{len(results['failed'])} failed, {len(results['skipped'])} skipped.\n")

    if results["complete"]:
        print("Complete:")
        for s in results["complete"]:
            print(f"  [OK]   {s}")

    if results["failed"]:
        print("Failed (state=failed in DB):")
        for s in results["failed"]:
            print(f"  [FAIL] {s}")

    if results["skipped"]:
        print("Skipped (missing data, state unchanged):")
        for s in results["skipped"]:
            print(f"  [SKIP] {s}")


if __name__ == "__main__":
    main()
