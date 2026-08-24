"""
manual_scouse_prep.py
----------------------
Register a SCOUSE unit and record its run parameters.  Headless — no display
required.  Run from the VM shell or inside any CASA container.

    python alma-sails-codebase/scripts/manual_scouse_prep.py \\
        --source Per-emb-2 \\
        --line-label "HC3N(10-9)" \\
        --cube-fits /arc/projects/ALMA-SAILS/imaging/lines/final/Per-emb-2/.../cube.fits \\
        --vel-range 4.9 9.0 \\
        --wsaa 11 \\
        --mask-below 0.104 \\
        --pixel-size 0.49 \\
        --min-radius 1.83 \\
        --velo-link 0.20

This script creates the scouse_unit + scouse_pipeline_state rows if they do
not already exist and advances state to 'prep_complete'.  Re-running with
--force updates the stored parameters and resets the state to 'prep_complete'
(safe as long as the output directory has not yet accumulated stage-1 output
that you want to keep).

ACORNS parameters (--pixel-size, --min-radius, --velo-link) should be derived
from the cube beam and channel width:
  pixel_size   = CDELT2 in arcsec (from FITS header)
  min_radius   ≈ sqrt(BMAJ × BMIN) / 2 in arcsec  (half geometric-mean beam)
  velo_link    ≈ 2 × |CDELT3|  in km/s             (two native channels)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    advance_scouse_state,
    register_scouse_unit,
    scouse_output_dir_path,
    update_scouse_parameters,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register a SCOUSE unit and record run parameters."
    )
    parser.add_argument("--source",     required=True, help="Source / protostar name")
    parser.add_argument("--line-label", required=True, help="Line label, e.g. 'HC3N(10-9)'")
    parser.add_argument("--cube-fits",  required=True,
                        help="Path to the final line cube FITS file (local or canonical)")
    parser.add_argument("--vel-range",  nargs=2, type=float, required=True,
                        metavar=("VMIN", "VMAX"),
                        help="Velocity range in km/s to fit (e.g. 4.9 9.0)")
    parser.add_argument("--wsaa",       nargs="+", type=int, required=True,
                        metavar="PIX",
                        help="SAA width(s) in pixels (e.g. --wsaa 11)")
    parser.add_argument("--mask-below", type=float, required=True,
                        metavar="JY_BEAM",
                        help="Intensity threshold below which pixels are masked (Jy/beam)")
    parser.add_argument("--pixel-size", type=float, required=True,
                        metavar="ARCSEC",
                        help="Pixel size in arcsec (FITS CDELT2 value)")
    parser.add_argument("--min-radius", type=float, required=True,
                        metavar="ARCSEC",
                        help="ACORNS spatial linking radius in arcsec (~half geometric-mean beam)")
    parser.add_argument("--velo-link",  type=float, required=True,
                        metavar="KMS",
                        help="ACORNS velocity linking tolerance in km/s (~2 native channels)")
    parser.add_argument("--nrefine",    type=int, default=1,
                        help="Number of SAA refinement passes (default: 1)")
    parser.add_argument("--njobs",      type=int, default=4,
                        help="Parallel SAA fitting workers (default: 4)")
    parser.add_argument("--acorns-relax", nargs=2, type=float, default=[2.0, 1.0],
                        metavar=("SPATIAL", "VELOCITY"),
                        help="ACORNS relax factors [spatial, velocity] (default: 2.0 1.0)")
    parser.add_argument("--acorns-stop", type=float, default=3.0,
                        help="ACORNS stop criterion (default: 3.0)")
    parser.add_argument("--force", action="store_true",
                        help="Update parameters and reset to prep_complete even if already registered")
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    cube_fits_canonical = db.to_canonical(args.cube_fits)
    cube_fits_local = Path(db.to_local(cube_fits_canonical))
    if not cube_fits_local.exists():
        print(f"[ERROR] Cube FITS not found: {cube_fits_local}")
        sys.exit(1)

    parameters = {
        "vel_range":        args.vel_range,
        "wsaa":             args.wsaa,
        "mask_below":       args.mask_below,
        "nrefine":          args.nrefine,
        "njobs":            args.njobs,
        "pixel_size_arcsec": args.pixel_size,
        "min_radius_arcsec": args.min_radius,
        "velo_link_kms":     args.velo_link,
        "acorns_relax":      args.acorns_relax,
        "acorns_stop":       args.acorns_stop,
    }

    conn = db.get_connection(args.db_path)
    unit_id = register_scouse_unit(
        conn, args.source, args.line_label, cube_fits_canonical
    )

    state_row = db.fetch_one(
        conn,
        "SELECT current_state FROM scouse_pipeline_state WHERE scouse_unit_id=?",
        (unit_id,),
    )
    current = state_row["current_state"]

    if current != "pending" and not args.force:
        print(
            f"[SKIP] {args.source} / {args.line_label} is already in state '{current}'. "
            f"Use --force to update parameters and reset to prep_complete."
        )
        conn.close()
        return

    update_scouse_parameters(conn, unit_id, parameters)

    output_dir = scouse_output_dir_path(args.source, args.line_label)
    output_dir.mkdir(parents=True, exist_ok=True)

    from_state = current if args.force and current != "pending" else "pending"
    if args.force and current != "pending":
        conn.execute(
            "UPDATE scouse_pipeline_state SET current_state='pending', updated_at=datetime('now') "
            "WHERE scouse_unit_id=?",
            (unit_id,),
        )
        conn.commit()
        from_state = "pending"

    advance_scouse_state(
        conn, unit_id, args.source, args.line_label,
        from_state="pending", to_state="prep_complete",
        note=f"cube={cube_fits_canonical}",
    )

    print(f"[OK] {args.source} / {args.line_label} → prep_complete")
    print(f"     output_dir: {output_dir}")
    print(f"     vel_range={args.vel_range}  wsaa={args.wsaa}  mask_below={args.mask_below}")
    print(f"     pixel_size={args.pixel_size}  min_radius={args.min_radius}  velo_link={args.velo_link}")
    conn.close()


if __name__ == "__main__":
    main()
