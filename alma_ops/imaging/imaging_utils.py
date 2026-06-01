"""
imaging_utils.py
----------------
Utilities for computing CASA tclean imaging parameters from measurement set
metadata. Must be called from inside a CASA session (uses casatools).

Cell size
---------
Derived from the maximum baseline length and mean observing frequency,
targeting PIXELS_PER_BEAM pixels across the synthesised beam minor axis.
A Briggs robust scale factor is applied to the natural-weighting estimate.

Image size
----------
Covers the primary beam to PB_COVERAGE_FACTOR × FWHM diameter, rounded up
to an FFT-friendly integer (only prime factors 2, 3, 5, 7 are allowed).

B75
---
75th-percentile baseline length computed from pairwise antenna separations
across all input MSes (from the ANTENNA subtable POSITION column). Used by
continuum_config.select_automask() to choose auto-multithresh parameters.
"""

from __future__ import annotations

import math

from alma_ops.imaging.continuum_config import (
    B75_COMPACT_MAX,
    B75_EXTENDED_MIN,
    MINOR_AXIS_FRACTION,
    PIXELS_PER_BEAM,
    PB_COVERAGE_FACTOR,
)


# Briggs robust value → fraction of the natural-weighting beam estimate.
# Interpolated linearly for values between the listed keys.
_ROBUST_SCALE: dict[float, float] = {
    -2.0: 0.65,
    -1.0: 0.72,
     0.0: 0.82,
     0.5: 0.88,
     1.0: 0.95,
     2.0: 1.10,
}


def _robust_scale_factor(robust: float) -> float:
    """Interpolate the beam-scale factor for a given Briggs robust value."""
    keys = sorted(_ROBUST_SCALE)
    if robust <= keys[0]:
        return _ROBUST_SCALE[keys[0]]
    if robust >= keys[-1]:
        return _ROBUST_SCALE[keys[-1]]
    for lo, hi in zip(keys, keys[1:]):
        if lo <= robust <= hi:
            frac = (robust - lo) / (hi - lo)
            return _ROBUST_SCALE[lo] + frac * (_ROBUST_SCALE[hi] - _ROBUST_SCALE[lo])
    return 1.0


def _next_fft_friendly_size(n: int) -> int:
    """Return the smallest integer >= n whose prime factors are all in {2, 3, 5}.

    CASA tclean's FFT routines are efficient only for sizes composed of these
    primes. 7 is excluded despite being mathematically valid for FFTW, because
    tclean warns and overrides sizes containing factor 7 (e.g. 2688 → 2700).
    """
    if n <= 1:
        return 2
    candidate = n
    while True:
        m = candidate
        for p in (2, 3, 5):
            while m % p == 0:
                m //= p
        if m == 1:
            return candidate
        candidate += 1


def compute_b75(ms_paths: list) -> float:
    """Compute the 75th-percentile baseline length from antenna positions.

    Reads the ANTENNA subtable of each MS, computes all pairwise separations
    between antennas, and returns the 75th percentile across all input MSes.
    Duplicate antenna entries (same array configuration) are deduplicated by
    position so that combining 12m MSes for the same MOUS does not bias the
    result.

    Parameters
    ----------
    ms_paths:
        Paths to measurement sets (str or Path).

    Returns
    -------
    float
        75th-percentile baseline length in metres.
    """
    try:
        import numpy as np
        from casatools import table as tb_tool
    except ImportError:
        raise RuntimeError("compute_b75 must be called from inside a CASA session.")

    # Collect unique antenna positions across all MSes
    positions_set = []
    seen = set()

    for ms_path in ms_paths:
        tb = tb_tool()
        tb.open(f"{ms_path}/ANTENNA")
        try:
            pos = tb.getcol("POSITION")  # shape (3, N_ant), ECEF metres
        finally:
            tb.close()

        for i in range(pos.shape[1]):
            key = tuple(round(float(x), 1) for x in pos[:, i])
            if key not in seen:
                seen.add(key)
                positions_set.append(pos[:, i])

    if len(positions_set) < 2:
        raise ValueError("Fewer than 2 unique antenna positions found across provided MSes.")

    positions = np.array(positions_set)  # (N, 3)
    lengths = []
    N = len(positions)
    for i in range(N):
        for j in range(i + 1, N):
            diff = positions[i] - positions[j]
            lengths.append(float(np.sqrt(np.dot(diff, diff))))

    return float(np.percentile(lengths, 75))


def compute_imaging_params(
    ms_paths,
    robust=0.5,
    pixels_per_beam=PIXELS_PER_BEAM,
    pb_coverage_factor=PB_COVERAGE_FACTOR,
):
    """Compute tclean cell size and image size from measurement set metadata.

    Examines all provided MS files and returns parameters appropriate for
    the highest-resolution MS (largest max baseline → smallest cell) and the
    largest primary beam (lowest-frequency MS → largest image).

    Parameters
    ----------
    ms_paths:
        One or more paths to measurement sets (str or Path).
    robust:
        Briggs robust weighting parameter.
    pixels_per_beam:
        Target pixels across the synthesised beam minor axis.
    pb_coverage_factor:
        Image extent as a multiple of the primary beam FWHM diameter.

    Returns
    -------
    cell_str : str
        Cell size string for tclean, e.g. '0.15arcsec'.
    imsize : int
        Square image size in pixels (FFT-friendly).

    Raises
    ------
    RuntimeError
        If called outside a CASA session.
    ValueError
        If ms_paths is empty or metadata cannot be read.
    """
    if not ms_paths:
        raise ValueError("ms_paths must contain at least one path.")

    try:
        import casatools
        import numpy as np
    except ImportError:
        raise RuntimeError(
            "compute_imaging_params must be called from inside a CASA session."
        )

    C_MS = 299_792_458.0  # speed of light, m/s

    all_baselines_m = []
    min_mean_freq_hz = float("inf")
    antenna_diam_m = None

    msmd = casatools.msmetadata()
    tb   = casatools.table()

    for ms_path in ms_paths:
        # Collect all pairwise baseline lengths from ANTENNA subtable positions
        tb.open(f"{ms_path}/ANTENNA")
        try:
            pos = tb.getcol("POSITION")  # shape (3, N_ant), ECEF metres
            N = pos.shape[1]
            for i in range(N):
                for j in range(i + 1, N):
                    diff = pos[:, i] - pos[:, j]
                    all_baselines_m.append(float(np.sqrt(np.dot(diff, diff))))
        finally:
            tb.close()

        try:
            msmd.open(str(ms_path))

            try:
                n_spw = msmd.nspw()
                freqs = [msmd.meanfreq(spw=i) for i in range(n_spw) if msmd.nchan(i) > 0]
                if freqs:
                    ms_mean_freq = float(np.mean(freqs))
                    min_mean_freq_hz = min(min_mean_freq_hz, ms_mean_freq)
            except Exception:
                pass

            if antenna_diam_m is None:
                try:
                    diams = msmd.antennadiameter()
                    vals = [v["value"] for v in diams.values() if v["value"] > 0]
                    if vals:
                        antenna_diam_m = float(np.median(vals))
                except Exception:
                    pass

        finally:
            msmd.done()

    if not all_baselines_m:
        raise ValueError("Could not determine maximum baseline from the provided MS files.")
    if min_mean_freq_hz == float("inf"):
        raise ValueError("Could not determine mean frequency from the provided MS files.")
    if antenna_diam_m is None:
        antenna_diam_m = 12.0

    # 95th-percentile baseline — ignores the top 5% of outlier long baselines,
    # matching au.pickCellSize(maxBaselinePercentile=95) behaviour.
    ref_baseline_m = float(np.percentile(all_baselines_m, 95))

    # Cell size
    wavelength_m = C_MS / min_mean_freq_hz
    theta_nat_rad = wavelength_m / ref_baseline_m
    scale = _robust_scale_factor(robust)
    theta_minor_rad = theta_nat_rad * scale * MINOR_AXIS_FRACTION
    theta_minor_arcsec = math.degrees(theta_minor_rad) * 3600.0
    cell_arcsec = theta_minor_arcsec / pixels_per_beam

    # Round to 3 significant figures
    magnitude = 10 ** math.floor(math.log10(cell_arcsec))
    cell_arcsec = round(cell_arcsec / magnitude, 2) * magnitude
    cell_str = f"{cell_arcsec:.4f}arcsec"

    # Image size
    theta_pb_rad = 1.13 * wavelength_m / antenna_diam_m
    theta_pb_arcsec = math.degrees(theta_pb_rad) * 3600.0
    extent_arcsec = pb_coverage_factor * theta_pb_arcsec
    min_imsize = int(math.ceil(extent_arcsec / cell_arcsec))
    min_imsize = max(min_imsize, 64)
    try:
        import analysisUtils as au
        imsize = au.getOptimumSize(min_imsize)
        print(f"  [imsize] au.getOptimumSize({min_imsize}) → {imsize}", flush=True)
    except (ImportError, NameError):
        imsize = _next_fft_friendly_size(min_imsize)

    return cell_str, imsize


def _automask_label(b75: float) -> str:
    if b75 < B75_COMPACT_MAX:
        return "compact"
    if b75 < B75_EXTENDED_MIN:
        return "intermediate"
    return "extended"


def run_continuum_imaging(
    field_name: str,
    band: str,
    config_type: str,
    imaging_dir: str,
    robust: float,
    mous_list: list,
    keep_casa_products: bool = False,
    parallel: bool = False,
) -> dict:
    """Run tclean + impbcor + exportfits for one imaging unit.

    Must be called from inside a CASA session (imports casatasks internally).

    Parameters
    ----------
    field_name:
        CASA field/source name used in tclean's field= parameter.
    band:
        Band number as a string (e.g. '6', '7').
    config_type:
        Configuration type ('12m', '7m', 'tp', 'combined').
    imaging_dir:
        Directory in which all output products are written.
    robust:
        Briggs robust weighting parameter.
    mous_list:
        Each entry is a dict with keys ``ms_paths`` (list[str]) and
        ``cont_dat_path`` (str).
    keep_casa_products:
        If False (default), delete all CASA-format tclean intermediates
        (.image, .pb, .psf, .residual, .mask, .model, .sumwt, .image.pbcor)
        after FITS export. Set True to retain them.
    """
    import os
    import shutil
    from pathlib import Path as _Path

    try:
        from casatasks import exportfits, impbcor, tclean
    except ImportError:
        raise RuntimeError("run_continuum_imaging must be called from inside a CASA session.")

    # Redirect the CASA log into the imaging directory so all task output
    # (tclean, impbcor, exportfits) is co-located with the image products.
    _log_name = f"{field_name}_B{band}_{config_type}_rob{robust:.1f}.casa.log"
    _log_path = os.path.join(imaging_dir, _log_name)
    try:
        import casatools as _casatools
        _casatools.casalog.setlogfile(_log_path)
        print(f"  [log] redirected to {_log_path}", flush=True)
    except (ImportError, AttributeError, Exception):
        pass

    from alma_ops.imaging.cont_dat_utils import cont_dat_to_spw_string, parse_cont_dat
    from alma_ops.imaging.continuum_config import TCLEAN_SHARED, select_automask

    # 1. Build vis/spw lists from cont.dat per MOUS
    vis_list: list = []
    spw_list: list = []
    all_ms_paths: list = []

    for entry in mous_list:
        ms_paths      = entry["ms_paths"]
        cont_dat_path = entry["cont_dat_path"]

        print(f"  [cont.dat] {_Path(cont_dat_path).parent.name}", flush=True)
        cont_ranges = parse_cont_dat(cont_dat_path, field_name)

        if not cont_ranges:
            print(
                f"  [WARNING] No cont.dat entries for field '{field_name}' in "
                f"{cont_dat_path} — skipping this MOUS.",
                flush=True,
            )
            continue

        for ms_path in ms_paths:
            spw_str = cont_dat_to_spw_string(ms_path, cont_ranges)
            if not spw_str:
                print(f"  [WARNING] Empty SPW selection for {ms_path} — skipping.", flush=True)
                continue
            vis_list.append(ms_path)
            spw_list.append(spw_str)
            all_ms_paths.append(ms_path)
            print(f"    {_Path(ms_path).name}  spw: {spw_str}", flush=True)

    if not vis_list:
        raise ValueError(f"No usable MS/cont.dat pairs for {field_name} B{band} {config_type}.")

    # 2. Automask parameters from B75 across all input MSes
    print("  [params] computing B75 …", flush=True)
    b75 = compute_b75(all_ms_paths)
    automask = select_automask(b75)
    print(f"    B75={b75:.0f}m  →  {_automask_label(b75)}", flush=True)

    # 3. Cell size and image size from MS metadata
    print("  [params] computing cell size and image size …", flush=True)
    cell_str, imsize = compute_imaging_params(vis_list, robust=robust)
    print(f"    cell={cell_str}  imsize={imsize}", flush=True)

    # 4. tclean
    img_base = os.path.join(imaging_dir, f"{field_name}_B{band}_{config_type}_rob{robust:.1f}_cont")

    tclean_params = dict(TCLEAN_SHARED)
    tclean_params.update(automask)
    tclean_params.update({
        "vis":       vis_list,
        "spw":       spw_list,
        "imagename": img_base,
        "field":     field_name,
        "imsize":    [imsize, imsize],
        "cell":      cell_str,
        "robust":    robust,
        "parallel":  parallel,
    })

    print(f"  [tclean] starting … (parallel={parallel})", flush=True)
    tclean(**tclean_params)
    print("  [tclean] finished.", flush=True)

    # 5. Primary beam correction
    image_path       = img_base + ".image"
    pbcor_image      = img_base + ".image.pbcor"
    image_fits_path  = img_base + ".image.fits"
    pbcor_fits_path  = img_base + ".image.pbcor.fits"

    print("  [impbcor] …", flush=True)
    impbcor(
        imagename=image_path,
        pbimage=img_base + ".pb",
        outfile=pbcor_image,
        overwrite=True,
    )

    # 6. Export FITS for both image and pbcor
    print(f"  [exportfits] → {image_fits_path}", flush=True)
    exportfits(
        imagename=image_path,
        fitsimage=image_fits_path,
        dropdeg=True,
        dropstokes=True,
        overwrite=True,
    )

    print(f"  [exportfits] → {pbcor_fits_path}", flush=True)
    exportfits(
        imagename=pbcor_image,
        fitsimage=pbcor_fits_path,
        dropdeg=True,
        dropstokes=True,
        overwrite=True,
    )

    # 7. Clean up CASA-format intermediates (kept only when explicitly requested)
    _CASA_SUFFIXES = [
        ".image", ".image.pbcor", ".mask", ".model",
        ".pb", ".psf", ".residual", ".sumwt", ".weight",
    ]
    if not keep_casa_products:
        for suffix in _CASA_SUFFIXES:
            p = img_base + suffix
            if os.path.exists(p):
                shutil.rmtree(p, ignore_errors=True)
        print("  [cleanup] CASA intermediates removed.", flush=True)

    print(f"  [done] products in {imaging_dir}", flush=True)

    result = {
        "image_fits":           image_fits_path,
        "image_pbcor_fits":     pbcor_fits_path,
        "casa_products_cleaned": not keep_casa_products,
    }
    if keep_casa_products:
        result["image"]      = image_path
        result["image_pbcor"] = pbcor_image
    return result
