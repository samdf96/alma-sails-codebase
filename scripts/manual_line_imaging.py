"""
manual_line_imaging.py
------------------------
User-facing CASA script for deep (niter>0) line imaging of one
human-confirmed ADMIT detection.

Run from a CANFAR c-casa container:

    casa --nologger --nogui -c alma-sails-codebase/scripts/manual_line_imaging.py \\
        --detection-id 2 \\
        [--robust 0.5] \\
        [--ncores 4] \\
        [--pad-fraction 1.0] \\
        [--pad-floor-kms 5.0] \\
        [--force] \\
        [--dry-run] \\
        [--keep-casa-products] \\
        [--db-path /arc/.../db/serpens_v2.db]

The detection's review_status must already be 'confirmed' (see
review_line_detections.py at the project root) -- confirming a detection and
launching deep imaging for it are two separate, explicit actions; nothing
auto-launches this stage off a confirm. Resolves the parent triage/MOUS
chain (alma_ops.imaging.line_imaging_utils.resolve_line_imaging_inputs),
computes a frequency window centered on the confirmed line
(compute_line_window), runs tclean -> impbcor -> exportfits
(run_line_imaging), and advances the line_imaging_unit to 'complete'.
Output: imaging/lines/final/<target>/B<band>_<config>/ (config.LINE_FINAL_DIR).
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
from alma_ops.config import DB_PATH, LINE_FINAL_DIR
from alma_ops.imaging.line_imaging_config import LINE_IMAGING_PAD_FLOOR_KMS, LINE_IMAGING_PAD_FRACTION
from alma_ops.imaging.species_shorthand import species_label
from alma_ops.utils import to_dir_mous_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually run deep line imaging for one confirmed ADMIT detection."
    )
    parser.add_argument("--detection-id", type=int, required=True, help="line_admit_detection_id")
    parser.add_argument("--robust", type=float, default=0.5, help="Briggs robust weighting (default 0.5)")
    parser.add_argument("--ncores", type=int, default=0, help="mpicasa cores; 0 = flexible/single-threaded")
    parser.add_argument("--pad-fraction", type=float, default=None,
                        help="Padding per side as a multiple of measured linewidth "
                             f"(default: {LINE_IMAGING_PAD_FRACTION}). Increase for blended/wide lines.")
    parser.add_argument("--pad-floor-kms", type=float, default=None,
                        help="Minimum per-side padding in km/s regardless of linewidth "
                             f"(default: {LINE_IMAGING_PAD_FLOOR_KMS} km/s).")
    parser.add_argument("--force", action="store_true", help="Re-run even if already 'complete'")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without running tclean")
    parser.add_argument("--keep-casa-products", action="store_true", help="Keep CASA-format intermediates")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    # --- Validate the detection is confirmed ---
    detection = db.get_line_admit_detection(conn, args.detection_id)
    if detection is None:
        print(f"[ERROR] No line_admit_detection_id={args.detection_id} found")
        conn.close()
        sys.exit(1)
    if detection["review_status"] != "confirmed":
        print(f"[ERROR] detection {args.detection_id} has review_status="
              f"'{detection['review_status']}'; expected 'confirmed'. "
              f"Confirm it first: review_line_detections.py --confirm {args.detection_id}")
        conn.close()
        sys.exit(1)

    from alma_ops.imaging.line_imaging_utils import (
        compute_line_window, resolve_line_imaging_inputs, run_line_imaging,
    )

    try:
        inputs = resolve_line_imaging_inputs(conn, args.detection_id)
    except ValueError as e:
        print(f"[ERROR] {e}")
        conn.close()
        sys.exit(1)

    mous_dir_id = to_dir_mous_id(inputs["mous_id"])
    species_name = detection["species_name"]

    print(f"[line-imaging] detection_id={args.detection_id}  "
          f"species={species_label(species_name, detection['formula'])}  "
          f"frequency={detection['frequency_ghz']:.4f} GHz")
    print(f"  mous_id   : {inputs['mous_id']}")
    print(f"  target    : {inputs['target_name']}  spw={inputs['spw_idx']}")
    print(f"  ms_paths  : {inputs['ms_paths']}")
    print(f"  vlsr_kms  : {inputs['vlsr_kms']}")
    print(f"  is_mosaic : {inputs['is_mosaic']}")

    pad_fraction  = args.pad_fraction  if args.pad_fraction  is not None else LINE_IMAGING_PAD_FRACTION
    pad_floor_kms = args.pad_floor_kms if args.pad_floor_kms is not None else LINE_IMAGING_PAD_FLOOR_KMS
    window = compute_line_window(
        inputs["ms_paths"][0], inputs["spw_idx"], detection, inputs["vlsr_kms"], inputs["bin_factor"],
        pad_fraction=pad_fraction, pad_floor_kms=pad_floor_kms,
    )
    print(f"  freq_peak_ghz : {window['freq_peak_ghz']:.6f}")
    print(f"  linewidth_kms : {window['linewidth_kms']:.3f}")
    print(f"  pad_fraction  : {pad_fraction}  pad_floor_kms: {pad_floor_kms}")
    print(f"  window        : [{window['lo_ghz']:.6f}, {window['hi_ghz']:.6f}] GHz")

    imaging_dir = LINE_FINAL_DIR / inputs["target_name"]
    print(f"  imaging_dir   : {imaging_dir}")

    if args.dry_run:
        print("[OK] dry-run — nothing executed.")
        conn.close()
        return

    unit_id = db.get_or_create_line_imaging_unit(conn, args.detection_id)
    ips = db.get_line_imaging_pipeline_state(conn, unit_id)
    current_state = ips["current_state"]

    if current_state == "running":
        print("[SKIP] already running.")
        conn.close()
        return
    if current_state == "complete" and not args.force:
        print("[SKIP] already complete (use --force to re-run).")
        conn.close()
        return

    imaging_dir.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    db.record_line_imaging_transition(
        conn, unit_id, from_state=current_state, to_state="running",
        run_id=run_id, note="manual run", step="start",
    )
    conn.close()

    try:
        result = run_line_imaging(
            mous_dir_id=mous_dir_id,
            target_name=inputs["target_name"],
            spw_idx=inputs["spw_idx"],
            detection_id=args.detection_id,
            species_name=species_name,
            frequency_ghz=detection["frequency_ghz"],
            freq_peak_ghz=window["freq_peak_ghz"],
            lo_ghz=window["lo_ghz"],
            hi_ghz=window["hi_ghz"],
            native_chanwidth_hz=window["native_chanwidth_hz"],
            ms_paths=inputs["ms_paths"],
            imaging_dir=str(imaging_dir),
            robust=args.robust,
            is_mosaic=inputs["is_mosaic"],
            keep_casa_products=args.keep_casa_products,
            parallel=args.ncores > 1,
        )
    except Exception:
        err = traceback.format_exc()
        print(f"[ERROR]\n{err}", flush=True)
        conn = db.get_connection(args.db_path)
        db.record_line_imaging_failure(
            conn, unit_id, from_state="running", error=err[-4000:],
            run_id=run_id, step="line_imaging",
        )
        conn.close()
        sys.exit(1)

    imaging_products = {
        **result, "robust": args.robust,
        "pad_fraction": pad_fraction, "pad_floor_kms": pad_floor_kms,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    conn = db.get_connection(args.db_path)
    db.record_line_imaging_transition(
        conn, unit_id, from_state="running", to_state="complete",
        run_id=run_id, note="line imaging complete", step="complete",
        imaging_products=imaging_products,
        payload={"imaging_products": imaging_products},
    )
    conn.close()

    print(f"[OK] {result['image_fits']}")


if __name__ == "__main__":
    main()
