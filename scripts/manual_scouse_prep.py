"""
manual_scouse_prep.py
----------------------
Register a SCOUSE unit and auto-derive all run parameters from the final line
cube.  Headless — run from a c-casa container (CASA has astropy built in).

    casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_scouse_prep.py \\
        --detection-id 4722

    # Override the noise multiplier (default 5σ):
    casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_scouse_prep.py \\
        --detection-id 4722 --mask-sigma 8

    # Override the threshold directly:
    casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_scouse_prep.py \\
        --detection-id 4722 --mask-below 0.012

Parameters derived automatically from the cube FITS header and data:
    pixel_size_arcsec  CDELT2 in arcsec
    vel_range          [v_first_chan, v_last_chan] in km/s (full cube extent)
    wsaa               round(sqrt(BMAJ×BMIN) / pixel_size) — one beam in pixels
    min_radius         sqrt(BMAJ×BMIN) / 2 in arcsec (half geometric-mean beam)
    velo_link          2 × channel_width in km/s
    mask_below         mask_sigma × RMS(edge channels) in Jy/beam

mask_below is a holding value — the stage-1 GUI lets you adjust it interactively
before fitting begins.  Re-run with --force to update it without touching the
output directory.

The line_label is derived from line_admit_detection.uid when formula/transition
are available, e.g. "HC3N(10-9)".  Override with --line-label if needed.
"""

from __future__ import annotations

import argparse
import json
import math
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


# ── Cube inspection ────────────────────────────────────────────────────────────

def _derive_cube_params(cube_fits_path: str, mask_sigma: float, mask_below_override: float | None) -> dict:
    """Open the cube FITS file and derive all SCOUSE/ACORNS parameters.

    Uses astropy (available inside a CASA container).  Handles both the
    simple single-beam FITS (BMAJ/BMIN in primary header) and the
    multi-beam per-channel BEAMS table extension that CASA exportfits writes.
    """
    try:
        from astropy.io import fits
        import numpy as np
    except ImportError:
        raise RuntimeError(
            "astropy is required to derive cube parameters. "
            "Run this script from inside a c-casa container."
        )

    C = 299792.458  # km/s

    with fits.open(cube_fits_path) as hdul:
        hdr   = hdul[0].header
        data  = hdul[0].data.squeeze()  # (nchan, ny, nx)

        # Beam — prefer BEAMS extension (per-channel) over primary header scalars
        bmaj_arcsec = bmin_arcsec = None
        beam_ext_names = [h.name for h in hdul]
        if "BEAMS" in beam_ext_names:
            bt = hdul["BEAMS"].data
            bmaj_col = bt["BMAJ"]
            bmin_col = bt["BMIN"]
            # Units are stored in the column header; CASA uses arcsec
            bmaj_unit = hdul["BEAMS"].columns["BMAJ"].unit or "arcsec"
            scale = 3600.0 if bmaj_unit.lower() in ("deg", "degree", "degrees") else 1.0
            bmaj_arcsec = float(np.nanmedian(bmaj_col)) * scale
            bmin_arcsec = float(np.nanmedian(bmin_col)) * scale

        if bmaj_arcsec is None:
            # Fall back to primary header
            if "BMAJ" in hdr:
                bmaj_arcsec = float(hdr["BMAJ"]) * 3600.0
                bmin_arcsec = float(hdr["BMIN"]) * 3600.0
            else:
                raise RuntimeError(
                    "Could not find BMAJ/BMIN in primary header or BEAMS extension."
                )

        pixel_arcsec = abs(float(hdr["CDELT2"])) * 3600.0

        restfreq = float(hdr.get("RESTFRQ", hdr.get("RESTFREQ")))
        naxis3   = int(hdr["NAXIS3"])
        crval3   = float(hdr["CRVAL3"])
        cdelt3   = float(hdr["CDELT3"])
        crpix3   = float(hdr["CRPIX3"])
        ctype3   = str(hdr.get("CTYPE3", "FREQ"))

        if "FREQ" in ctype3.upper():
            freq_ch1 = crval3 + (1 - crpix3) * cdelt3
            freq_chN = crval3 + (naxis3 - crpix3) * cdelt3
            v_start  = (restfreq - freq_ch1) / restfreq * C
            v_end    = (restfreq - freq_chN) / restfreq * C
            chan_vel = abs(cdelt3 / restfreq * C)
        else:
            # VRAD/VOPT axis already in m/s
            v_start  = crval3 / 1000.0
            v_end    = v_start + (naxis3 - 1) * cdelt3 / 1000.0
            chan_vel = abs(cdelt3) / 1000.0

        # RMS from first and last 5 channels (emission-free baseline)
        edge = np.concatenate([data[:5].ravel(), data[-5:].ravel()])
        edge = edge[np.isfinite(edge)]
        rms  = float(np.nanstd(edge))

    geom_beam  = math.sqrt(bmaj_arcsec * bmin_arcsec)
    min_radius = geom_beam / 2.0
    wsaa_px    = max(1, round(geom_beam / pixel_arcsec))
    velo_link  = round(2.0 * chan_vel, 4)
    vel_range  = [round(min(v_start, v_end), 3), round(max(v_start, v_end), 3)]
    mask_below = mask_below_override if mask_below_override is not None else round(mask_sigma * rms, 6)

    print(f"  [cube] beam:       {bmaj_arcsec:.3f} × {bmin_arcsec:.3f} arcsec "
          f"(geom-mean {geom_beam:.3f})")
    print(f"  [cube] pixel:      {pixel_arcsec:.4f} arcsec  → wsaa={wsaa_px} px")
    print(f"  [cube] vel range:  {vel_range[0]:.3f} – {vel_range[1]:.3f} km/s "
          f"({naxis3} channels, {chan_vel:.4f} km/s/chan)")
    print(f"  [cube] velo_link:  {velo_link} km/s  (2 channels)")
    print(f"  [cube] min_radius: {min_radius:.3f} arcsec")
    print(f"  [cube] RMS:        {rms*1e3:.2f} mJy/beam  → mask_below={mask_below:.6f} Jy/beam "
          f"({mask_sigma:.0f}σ)")

    return {
        "vel_range":          vel_range,
        "wsaa":               [wsaa_px],
        "mask_below":         mask_below,
        "nrefine":            1,
        "njobs":              4,
        "pixel_size_arcsec":  round(pixel_arcsec, 6),
        "min_radius_arcsec":  round(min_radius, 4),
        "velo_link_kms":      velo_link,
        "acorns_relax":       [2.0, 1.0],
        "acorns_stop":        3.0,
        # stored for reference
        "_bmaj_arcsec":       round(bmaj_arcsec, 4),
        "_bmin_arcsec":       round(bmin_arcsec, 4),
        "_chan_vel_kms":       round(chan_vel, 6),
        "_rms_jybeam":        round(rms, 8),
        "_mask_sigma":        mask_sigma,
    }


def _derive_line_label(det_row) -> str:
    """Build a line label like 'HC3N(10-9)' from a detection row."""
    formula    = det_row["formula"] or ""
    transition = det_row["transition"] or ""
    uid        = det_row["uid"] or ""

    # uid format from ADMIT: "HC3N_90.97900" or "U_90.9790"
    if formula and transition:
        return f"{formula}({transition})"
    if formula:
        return formula
    # Fall back to uid with underscores replaced
    return uid.replace("_", " ").strip()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register a SCOUSE unit; auto-derive parameters from the cube."
    )
    parser.add_argument("--detection-id", type=int, required=True,
                        help="line_admit_detection_id of the confirmed detection to decompose")
    parser.add_argument("--mask-sigma",   type=float, default=5.0,
                        help="RMS multiplier for the initial mask threshold (default: 5)")
    parser.add_argument("--mask-below",   type=float, default=None,
                        metavar="JY_BEAM",
                        help="Override mask threshold directly in Jy/beam (ignores --mask-sigma)")
    parser.add_argument("--line-label",   default=None,
                        help="Override the derived line label, e.g. 'HC3N(10-9)'")
    parser.add_argument("--force",        action="store_true",
                        help="Update parameters and reset to prep_complete even if already registered")
    parser.add_argument("--db-path",      default=str(DB_PATH))
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)

    # Look up the confirmed detection and its imaging product
    det_row = db.fetch_one(
        conn,
        "SELECT lad.*, laps.target_name FROM line_admit_detection lad "
        "JOIN line_admit_pipeline_state laps USING (line_admit_unit_id) "
        "WHERE lad.line_admit_detection_id=?",
        (args.detection_id,),
    )
    if det_row is None:
        print(f"[ERROR] No detection with id={args.detection_id}")
        sys.exit(1)

    # Get the imaging product for this detection
    lip_row = db.fetch_one(
        conn,
        "SELECT * FROM line_imaging_pipeline_state WHERE line_admit_detection_id=?",
        (args.detection_id,),
    )
    if lip_row is None:
        print(f"[ERROR] No line_imaging_pipeline_state row for detection {args.detection_id}. "
              f"Has this detection been imaged with manual_line_imaging.py?")
        sys.exit(1)
    if lip_row["current_state"] != "complete":
        print(f"[ERROR] line_imaging_unit is in state '{lip_row['current_state']}'; "
              f"expected 'complete'. Image the detection first.")
        sys.exit(1)

    products = json.loads(lip_row["imaging_products"])
    # Use the pbcor FITS if available, otherwise the plain image
    cube_canonical = products[-1].get("image_pbcor_fits") or products[-1].get("image_fits")
    if not cube_canonical:
        print(f"[ERROR] No image_fits path found in imaging_products for detection {args.detection_id}")
        sys.exit(1)

    cube_local = Path(db.to_local(cube_canonical))
    if not cube_local.exists():
        print(f"[ERROR] Cube FITS not found on disk: {cube_local}")
        sys.exit(1)

    source_name = lip_row["target_name"]
    line_label  = args.line_label or _derive_line_label(det_row)

    print(f"[scouse-prep] detection {args.detection_id}: {source_name} / {line_label}")
    print(f"  cube: {cube_local.name}")

    # Derive parameters from the cube
    params = _derive_cube_params(str(cube_local), args.mask_sigma, args.mask_below)

    # Register unit
    unit_id = register_scouse_unit(conn, source_name, line_label, cube_canonical)

    state_row = db.fetch_one(
        conn,
        "SELECT current_state FROM scouse_pipeline_state WHERE scouse_unit_id=?",
        (unit_id,),
    )
    current = state_row["current_state"]

    if current != "pending" and not args.force:
        print(f"[SKIP] {source_name} / {line_label} is already in state '{current}'. "
              f"Use --force to update parameters and reset to prep_complete.")
        conn.close()
        return

    update_scouse_parameters(conn, unit_id, params)

    output_dir = scouse_output_dir_path(source_name, line_label)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.force and current != "pending":
        conn.execute(
            "UPDATE scouse_pipeline_state SET current_state='pending', updated_at=datetime('now') "
            "WHERE scouse_unit_id=?",
            (unit_id,),
        )
        conn.commit()

    advance_scouse_state(
        conn, unit_id, source_name, line_label,
        from_state="pending", to_state="prep_complete",
        note=f"detection_id={args.detection_id} mask_sigma={args.mask_sigma}",
    )

    print(f"[OK] {source_name} / {line_label} → prep_complete")
    print(f"     output_dir: {output_dir}")
    conn.close()


if __name__ == "__main__":
    main()
