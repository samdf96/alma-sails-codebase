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

    # Keep the full cube velocity range instead of the emission window:
    casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_scouse_prep.py \\
        --detection-id 4722 --full-vel-range

Parameters derived automatically from the cube FITS header and data:
    pixel_size_arcsec  CDELT2 in arcsec
    vel_range          the emission window plus 2 km/s of line-free channels each side
                       (see below); the full cube extent with --full-vel-range or when no
                       emission is found
    wsaa               round(sqrt(BMAJ×BMIN) / pixel_size) — one beam in pixels
    min_radius         radius of a circle with one beam's area (ACORNS minimum structure size)
    velo_link          2 × channel_width in km/s
    mask_below         mask_sigma × RMS(edge channels) in Jy/beam

Emission window: a channel holds emission when at least one beam's area of pixels is
above 5σ (σ = the edge-channel RMS below).  The window is the run of emission channels
around the channel with the most such pixels, joined across gaps of up to 3 km/s, with
the 2 channels at each cube edge ignored (first/last channels are often noisy). The
3 km/s join keeps both peaks of a self-absorbed line: before 2026-11-06 it was 1 km/s,
and the CO lines of eHOPS-aql-46 / Serp_15 lost their red half to a ~2 km/s dip.
Stage 3 fits every channel in vel_range, so trimming line-free channels is the main
lever on its run time; the 2 km/s padding keeps a baseline either side of the line.
The full cube range and the unpadded window are stored as _vel_range_cube and
_vel_range_emission, and a diagnostic plot is written to
<output_dir>/prep_velocity_window.png (skipped if matplotlib is unavailable).

mask_below and vel_range are holding values — the stage-1 GUI lets you adjust them
interactively before fitting begins.  Re-run with --force to re-derive them: that starts
the unit over at prep_complete, deleting its cluster rows, its acorns_input.dat and its
stage_1-4 and acorns_output directories (scousepy would otherwise load the old
sN.scousepy files and skip fitting). The scousepy.config and the summary PDF are kept.

The line_label is the detection's isotopologue plus its transition without
hyperfine numbers, e.g. "HC3N(10-9)", "13CS(2-1)".  Override with --line-label if needed.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    ACORNS_SIGMA_SPLIT_KMS,
    acorns_min_radius_arcsec,
    JOB_ACTIVE_STATUSES,
    advance_scouse_state,
    clear_acorns_results,
    clear_scouse_job,
    cube_stem,
    register_scouse_unit,
    scouse_connection,
    scouse_output_dir_path,
    update_scouse_parameters,
)


# ── Cube inspection ────────────────────────────────────────────────────────────

# Emission window (see module docstring)
WINDOW_SIGMA    = 5.0   # per-pixel detection threshold, in units of the edge-channel RMS
WINDOW_GAP_KMS  = 3.0   # emission blocks separated by at most this much are joined; spans a
                        # self-absorption dip (CO lines here dip ~2 km/s at the cloud velocity)
WINDOW_PAD_KMS  = 2.0   # line-free margin added on each side
WINDOW_EDGE_CH  = 2     # channels ignored at each end of the non-blank cube


def _emission_window(counts, vel, c0: int, c1: int, beam_px: float, chan_vel: float):
    """Velocity extent [lo, hi] of the emission block around the brightest channel, or None.

    counts[k] is the number of pixels above WINDOW_SIGMA in channel k; vel[k] its velocity.
    """
    import numpy as np

    lo_ch, hi_ch = c0 + WINDOW_EDGE_CH, c1 - WINDOW_EDGE_CH
    idx = np.array([k for k in range(lo_ch, hi_ch + 1) if counts[k] >= beam_px])
    if idx.size == 0:
        return None
    max_empty = int(WINDOW_GAP_KMS / chan_vel)
    blocks = np.split(idx, np.where(np.diff(idx) - 1 > max_empty)[0] + 1)
    peak = lo_ch + int(np.argmax(counts[lo_ch:hi_ch + 1]))
    main = next(b for b in blocks if b[0] <= peak <= b[-1])
    lo, hi = sorted((float(vel[main[0]]), float(vel[main[-1]])))
    return [lo, hi]


def _plot_velocity_window(png_path: Path, title: str, vel, counts, peak, rms: float, beam_px: float,
                          cube_range, emission, chosen) -> None:
    """Per-channel pixel count above WINDOW_SIGMA and the peak spectrum, with the window shaded."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [plot] matplotlib not available — skipping prep_velocity_window.png")
        return
    import numpy as np

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 5.5), sharex=True)
    ax1.step(vel, np.asarray(counts) / beam_px, where="mid", color="k", lw=0.8)
    ax1.axhline(1.0, color="grey", ls=":", lw=0.8)
    ax1.set_yscale("symlog", linthresh=1.0)
    ax1.set_ylim(bottom=0)
    ax1.set_ylabel(f"beams above {WINDOW_SIGMA:.0f}σ")
    ax2.step(vel, np.asarray(peak) / rms, where="mid", color="k", lw=0.8)
    ax2.axhline(WINDOW_SIGMA, color="grey", ls=":", lw=0.8)
    ax2.set_ylabel("peak / σ")
    ax2.set_xlabel("velocity (km/s)")
    for ax in (ax1, ax2):
        ax.axvspan(*chosen, color="tab:blue", alpha=0.15, lw=0)
        if emission:
            for v in emission:
                ax.axvline(v, color="tab:blue", lw=0.8)
    ax2.set_xlim(*cube_range)
    label = (f"vel_range {chosen[0]:.2f} to {chosen[1]:.2f} km/s (shaded)"
             + (f"; emission {emission[0]:.2f} to {emission[1]:.2f} km/s (lines)" if emission else "; no emission found"))
    fig.suptitle(f"{title}\n{label}", fontsize=9)
    fig.tight_layout()
    fig.savefig(png_path, dpi=110)
    plt.close(fig)
    print(f"  [plot] {png_path}")


def _derive_cube_params(cube_fits_path: str, mask_sigma: float, mask_below_override: float | None,
                        full_vel_range: bool = False) -> tuple[dict, dict]:
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

        # tclean leaves the first and/or last channel of most final cubes blank
        # (all zero); counted as baseline they pull the RMS down by ~10%.
        def _blank(plane) -> bool:
            return not np.any(np.isfinite(plane) & (plane != 0))

        c0, c1 = 0, naxis3 - 1
        while c0 < c1 and _blank(data[c0]):
            c0 += 1
        while c1 > c0 and _blank(data[c1]):
            c1 -= 1
        n_blank = naxis3 - 1 - (c1 - c0)

        chan_axis = np.arange(naxis3) + 1 - crpix3
        if "FREQ" in ctype3.upper():
            vel      = (restfreq - (crval3 + chan_axis * cdelt3)) / restfreq * C
            chan_vel = abs(cdelt3 / restfreq * C)
        else:
            # VRAD/VOPT axis already in m/s
            vel      = (crval3 + chan_axis * cdelt3) / 1000.0
            chan_vel = abs(cdelt3) / 1000.0
        v_start, v_end = float(vel[c0]), float(vel[c1])

        # RMS from the first and last 5 non-blank channels (emission-free baseline)
        edge = np.concatenate([data[c0:c0 + 5].ravel(), data[c1 - 4:c1 + 1].ravel()])
        edge = edge[np.isfinite(edge)]
        rms  = float(np.nanstd(edge))

        # Per-channel pixel count above WINDOW_SIGMA and peak, one plane at a time
        # (a whole-cube comparison would allocate another cube-sized array)
        counts = np.array([int(np.count_nonzero(plane > WINDOW_SIGMA * rms)) for plane in data])
        peak   = np.array([float(np.nanmax(plane)) if np.isfinite(plane).any() else np.nan
                           for plane in data])

    geom_beam  = math.sqrt(bmaj_arcsec * bmin_arcsec)
    min_radius = acorns_min_radius_arcsec(bmaj_arcsec, bmin_arcsec)
    wsaa_px    = max(1, round(geom_beam / pixel_arcsec))
    velo_link  = round(2.0 * chan_vel, 4)
    cube_range = [round(min(v_start, v_end), 3), round(max(v_start, v_end), 3)]
    mask_below = mask_below_override if mask_below_override is not None else round(mask_sigma * rms, 6)

    beam_px  = math.pi / (4 * math.log(2)) * bmaj_arcsec * bmin_arcsec / pixel_arcsec ** 2
    emission = _emission_window(counts, vel, c0, c1, beam_px, chan_vel)
    if emission is not None:
        emission = [round(emission[0], 3), round(emission[1], 3)]
    if full_vel_range or emission is None:
        vel_range = cube_range
    else:
        vel_range = [round(max(cube_range[0], emission[0] - WINDOW_PAD_KMS), 3),
                     round(min(cube_range[1], emission[1] + WINDOW_PAD_KMS), 3)]
    n_window = int(round((vel_range[1] - vel_range[0]) / chan_vel)) + 1

    print(f"  [cube] beam:       {bmaj_arcsec:.3f} × {bmin_arcsec:.3f} arcsec "
          f"(geom-mean {geom_beam:.3f})")
    print(f"  [cube] pixel:      {pixel_arcsec:.4f} arcsec  → wsaa={wsaa_px} px")
    print(f"  [cube] cube range: {cube_range[0]:.3f} – {cube_range[1]:.3f} km/s "
          f"({c1 - c0 + 1} channels, {chan_vel:.4f} km/s/chan; {n_blank} blank edge channel(s) dropped)")
    if emission is None:
        print(f"  [cube] emission:   none found (no channel with ≥1 beam above {WINDOW_SIGMA:.0f}σ) "
              f"— vel_range left at the full cube")
    else:
        print(f"  [cube] emission:   {emission[0]:.3f} – {emission[1]:.3f} km/s "
              f"(≥1 beam above {WINDOW_SIGMA:.0f}σ, gaps ≤{WINDOW_GAP_KMS:g} km/s joined)")
    print(f"  [cube] vel_range:  {vel_range[0]:.3f} – {vel_range[1]:.3f} km/s "
          f"(~{n_window} of {c1 - c0 + 1} channels"
          + ("; --full-vel-range)" if full_vel_range else
             f"; emission ± {WINDOW_PAD_KMS:g} km/s)" if emission else ")"))
    print(f"  [cube] velo_link:  {velo_link} km/s  (2 channels)")
    print(f"  [cube] min_radius: {min_radius:.3f} arcsec")
    print(f"  [cube] RMS:        {rms*1e3:.2f} mJy/beam  → mask_below={mask_below:.6f} Jy/beam "
          + (f"(--mask-below, {mask_below / rms:.1f}σ)" if mask_below_override is not None
             else f"({mask_sigma:.0f}σ)"))

    profile = {"vel": vel, "counts": counts, "peak": peak, "rms": rms, "beam_px": beam_px,
               "cube_range": cube_range, "emission": emission, "chosen": vel_range}

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
        "acorns_sigma_split_kms": ACORNS_SIGMA_SPLIT_KMS,
        "acorns_stop":        3.0,
        # stored for reference
        "_bmaj_arcsec":       round(bmaj_arcsec, 4),
        "_bmin_arcsec":       round(bmin_arcsec, 4),
        "_chan_vel_kms":       round(chan_vel, 6),
        "_rms_jybeam":        round(rms, 8),
        "_mask_sigma":        mask_sigma,
        "_vel_range_cube":    cube_range,
        "_vel_range_emission": emission,
    }, profile


def _derive_line_label(det_row) -> str:
    """Build a line label like 'HC3N(10-9)' or '13CS(2-1)' from a detection row.

    The species part is the detection's own isotopologue/vibrational state
    (species_shorthand.isotopologue_for, the same source as the cube filename),
    not the family shorthand: 13CS and C34S must not both become "CS", and
    two transitions of one species in one source must not share a label,
    since (source_name, line_label) is the scouse_unit key.

    The transition part is ADMIT's quantum numbers with hyperfine (F, F1, F2)
    components and a lone "J=" prefix dropped: 'J=10-9,F=11-10' → '10-9',
    'N=2-1,J=3/2-1/2,F1=2-1,F=3-2' → 'N=2-1,J=3/2-1/2', 'J=24-23,l=1e' → '24-23,l=1e'.
    With no transition recorded, the rest frequency stands in for it.
    """
    from alma_ops.imaging.species_shorthand import isotopologue_for

    species = (isotopologue_for(det_row["species_name"], det_row["formula"], det_row["frequency_ghz"])
               or (det_row["uid"] or "").split("_")[0])
    parts = [p.strip() for p in (det_row["transition"] or "").split(",")
             if p.strip() and not re.match(r"F\d*=", p.strip())]
    if len(parts) == 1 or (parts and not any(p.startswith("N=") for p in parts)):
        parts = [re.sub(r"^J=", "", p) for p in parts]
    transition = ",".join(parts) or f"{det_row['frequency_ghz']:.4f}GHz"
    return f"{species}({transition})"


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
    parser.add_argument("--full-vel-range", action="store_true",
                        help="Set vel_range to the full cube instead of the emission window ± 2 km/s")
    parser.add_argument("--force",        action="store_true",
                        help="Update parameters and reset to prep_complete even if already registered")
    parser.add_argument("--db-path",      default=str(DB_PATH))
    args = parser.parse_args()

    with scouse_connection(args.db_path) as conn:
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
        # Use the plain image (uniform noise) for SCOUSE fitting; pbcor has
        # spatially varying noise that corrupts moment maps and the mask threshold.
        cube_canonical = products[-1].get("image_fits") or products[-1].get("image_pbcor_fits")
        if not cube_canonical:
            print(f"[ERROR] No image_fits path found in imaging_products for detection {args.detection_id}")
            sys.exit(1)

        source_name = lip_row["target_name"]
        line_label  = args.line_label or _derive_line_label(det_row)

    cube_local = Path(db.to_local(cube_canonical))
    if not cube_local.exists():
        print(f"[ERROR] Cube FITS not found on disk: {cube_local}")
        sys.exit(1)

    print(f"[scouse-prep] detection {args.detection_id}: {source_name} / {line_label}")
    print(f"  cube: {cube_local.name}")

    # Derive parameters from the cube
    params, profile = _derive_cube_params(str(cube_local), args.mask_sigma, args.mask_below,
                                          full_vel_range=args.full_vel_range)

    output_dir = scouse_output_dir_path(source_name, line_label)

    with scouse_connection(args.db_path) as conn:
        # Register unit
        unit_id = register_scouse_unit(conn, source_name, line_label, cube_canonical)

        state_row = db.fetch_one(
            conn,
            "SELECT current_state, job_stage, job_status, job_id FROM scouse_pipeline_state "
            "WHERE scouse_unit_id=?",
            (unit_id,),
        )
        current = state_row["current_state"]
        if state_row["job_status"] in JOB_ACTIVE_STATUSES:
            print(f"[ERROR] {source_name} / {line_label}: stage {state_row['job_stage']} is "
                  f"{state_row['job_status']} (job {state_row['job_id']}). Stop it and clear it "
                  f"with reset_scouse.py --clear-job first.")
            sys.exit(1)

        if current != "pending" and not args.force:
            print(f"[SKIP] {source_name} / {line_label} (scouse unit {unit_id}) is already in state '{current}'. "
                  f"Use --force to update parameters and reset to prep_complete.")
            return

        # A forced re-prep starts the unit over: drop any ACORNS results (cluster rows and
        # payload columns, as reset_scouse.py does) so none outlive the new parameters.
        n_deleted = clear_acorns_results(conn, unit_id, params) if current != "pending" else 0
        catalogue = output_dir / cube_stem(str(cube_local)) / "acorns_input.dat"
        if current != "pending" and catalogue.exists():
            catalogue.unlink()   # stage-4 export; manual_acorns_run.py would otherwise reuse it
            print(f"  removed stale stage-4 catalogue {catalogue}")
        # scousepy's stage_N loads an existing sN.scousepy and returns without fitting, so
        # the old run's stage outputs would silently stand in for the new parameters.
        if current != "pending":
            for sub in ("stage_1", "stage_2", "stage_3", "stage_4", "acorns_output"):
                stale = catalogue.parent / sub
                if stale.exists():
                    shutil.rmtree(stale)
                    print(f"  removed stale {stale}")
        update_scouse_parameters(conn, unit_id, params)
        clear_scouse_job(conn, unit_id)   # a failed job of the old run must not block the new one

        output_dir.mkdir(parents=True, exist_ok=True)
        _plot_velocity_window(output_dir / "prep_velocity_window.png",
                              f"{source_name} / {line_label}  (detection {args.detection_id})", **profile)

        mask_note = (f"mask_below={args.mask_below}" if args.mask_below is not None
                     else f"mask_sigma={args.mask_sigma}")
        force_note = (f"; --force re-prep, {n_deleted} scouse_cluster rows deleted"
                      if current != "pending" else "")
        # Logged as the real transition (e.g. stage2_complete → prep_complete), not via pending
        advance_scouse_state(
            conn, unit_id, source_name, line_label,
            from_state=current, to_state="prep_complete",
            note=f"detection_id={args.detection_id} {mask_note}{force_note}",
        )

    print(f"[OK] {source_name} / {line_label} → prep_complete (scouse unit {unit_id})")
    print(f"     output_dir: {output_dir}")
    print(f"     stage 1:    alma-sails-codebase/scripts/scouse_run.sh --stage 1 --unit-id {unit_id}")


if __name__ == "__main__":
    main()
