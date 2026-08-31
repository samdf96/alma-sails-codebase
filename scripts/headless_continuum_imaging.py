"""
headless_continuum_imaging.py
------------------------------
CASA driver script for ALMA-SAILS continuum imaging (v2).

Launched by imaging_flow.py via a CANFAR headless CASA session:

    casa --nologger --nogui -c headless_continuum_imaging.py <payload.json>

Assumes the imaging unit is already in state 'running' — imaging_flow.py sets
this before launching the session.  This script runs tclean + impbcor +
exportfits and advances the state to 'complete' (or 'failed').

JSON payload schema
-------------------
{
    "imaging_unit_id": 42,
    "field_name":      "EC_53",
    "imaging_dir":     "/arc/.../imaging/continuum/EC_53/B6_12m/",
    "db_path":         "/arc/.../db/serpens_v2.db",
    "robust":          0.5,
    "run_id":          "uuid-from-flow",
    "mous_list": [
        {
            "mous_id":       "uid://A001/...",
            "ms_paths":      ["/arc/.../_targets.corrected.ms"],
            "cont_dat_path": "/arc/.../auto_selfcal/cont.dat"
        },
        ...
    ]
}

Output (all written inside imaging_dir)
-------
<field_name>_B<band>_<config_type>_cont.image
<field_name>_B<band>_<config_type>_cont.pb
<field_name>_B<band>_<config_type>_cont.image.pbcor
<field_name>_B<band>_<config_type>_cont.fits
"""

from __future__ import annotations

import json
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: casa -c headless_continuum_imaging.py <payload.json>", flush=True)
        sys.exit(1)

    with open(sys.argv[1]) as fh:
        payload = json.load(fh)

    imaging_unit_id = int(payload["imaging_unit_id"])
    field_name      = payload["field_name"]
    imaging_dir     = payload["imaging_dir"]
    db_path         = payload["db_path"]
    robust          = float(payload.get("robust", 0.5))
    mous_list       = payload["mous_list"]
    run_id          = payload.get("run_id") or str(uuid.uuid4())
    parallel        = bool(payload.get("parallel", False))
    is_mosaic       = bool(payload.get("is_mosaic", False))

    from alma_ops import db
    from alma_ops.utils import code_version

    conn = db.get_connection(db_path)
    unit_row    = db.get_imaging_pipeline_state(conn, imaging_unit_id)
    band        = unit_row["band"]
    config_type = unit_row["config_type"]
    conn.close()

    print(
        f"[imaging] {field_name}  B{band}  {config_type}  "
        f"robust={robust}  n_mous={len(mous_list)}  run_id={run_id}",
        flush=True,
    )

    os.makedirs(imaging_dir, exist_ok=True)

    try:
        from alma_ops.imaging.imaging_utils import run_continuum_imaging
        products = run_continuum_imaging(
            field_name=field_name,
            band=band,
            config_type=config_type,
            imaging_dir=imaging_dir,
            robust=robust,
            mous_list=mous_list,
            parallel=parallel,
            is_mosaic=is_mosaic,
        )
    except Exception:
        err = traceback.format_exc()
        print(f"[ERROR]\n{err}", flush=True)
        conn = db.get_connection(db_path)
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

    products_entry = dict(products)
    products_entry["robust"] = robust
    products_entry["gridder"] = "mosaic" if is_mosaic else "standard"
    products_entry["completed_at"] = datetime.now(timezone.utc).isoformat()
    # Which code produced this product. batch_continuum_reimage.py decides what
    # is still outstanding by looking for a post-fix commit here -- without it
    # every unit looks unprocessed forever and the campaign never converges.
    products_entry["code_commit"] = code_version()

    conn = db.get_connection(db_path)
    db.record_imaging_transition(
        conn,
        imaging_unit_id=imaging_unit_id,
        from_state="running",
        to_state="complete",
        run_id=run_id,
        note="imaging complete",
        step="complete",
        imaging_products=products_entry,
        payload={"imaging_products": products_entry, "imaging_dir": imaging_dir, "robust": robust},
    )
    conn.close()
    print("[imaging] done.", flush=True)


main()