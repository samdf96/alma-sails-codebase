"""
imaging_utils.py
----------------
Utilities for computing CASA tclean imaging parameters (cell size, image size)
from measurement set metadata.

Must be called from inside a CASA session (uses casatools.msmetadata).

Overview
--------
Cell size is derived from the maximum baseline length and the mean observing
frequency, targeting a user-specified number of pixels across the synthesised
beam minor axis (default 5).  Image size is set to cover the primary beam to
a specified multiple (default 1.5x the FWHM diameter), rounded to an integer
that is efficient for the FFT used internally by tclean.

The robust (Briggs) weighting parameter affects the effective resolution.  A
scale factor is applied to the natural-weighting beam estimate so that the cell
size remains appropriate for the chosen robust value:

    robust  -2.0  -1.0   0.0   0.5   1.0   2.0
    factor   0.65  0.72  0.82  0.88  0.95  1.10

These are empirical approximations; the true beam is only known after imaging.
For a first-pass continuum image they are sufficient to keep the beam well
sampled without grossly oversizing the image.
"""

from __future__ import annotations

import math


# =====================================================================
# Robust weighting scale factors
# =====================================================================

# Maps Briggs robust value → fraction of the natural-weighting beam estimate.
# Interpolated linearly for values between the listed keys.
_ROBUST_SCALE = {
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


# =====================================================================
# FFT-friendly image size
# =====================================================================

def _next_fft_friendly_size(n: int) -> int:
    """Return the smallest integer >= n whose prime factors are all in {2, 3, 5, 7}.

    tclean uses FFTs internally; sizes with only small prime factors are more
    efficient.  This mirrors the approach used in CASA's clean helper utilities.
    """
    if n <= 1:
        return 2

    candidate = n
    while True:
        m = candidate
        for p in (2, 3, 5, 7):
            while m % p == 0:
                m //= p
        if m == 1:
            return candidate
        candidate += 1


# =====================================================================
# Main public function
# =====================================================================

def compute_imaging_params(
    ms_paths: list[str],
    robust: float = 0.5,
    pixels_per_beam: int = 5,
    pb_coverage_factor: float = 1.5,
) -> tuple[str, int]:
    """Compute tclean cell size and image size from measurement set metadata.

    Examines all provided MS files and returns parameters appropriate for
    the highest-resolution MS (largest max baseline → smallest cell) and the
    largest primary beam (lowest-frequency MS → largest image).

    Parameters
    ----------
    ms_paths : list[str]
        One or more paths to measurement sets.  All should be for the same
        source, band, and array configuration.
    robust : float
        Briggs robust weighting parameter passed to tclean.  Used to scale
        the cell size estimate.  Default 0.5.
    pixels_per_beam : int
        Target number of pixels across the synthesised beam minor axis.
        Default 5 (standard ALMA convention).
    pb_coverage_factor : float
        Multiple of the primary beam FWHM diameter to set the image extent.
        Default 1.5 (image out to 1.5× the PB FWHM diameter).

    Returns
    -------
    cell_str : str
        Cell size string suitable for tclean, e.g. '0.15arcsec'.
    imsize : int
        Square image size in pixels (FFT-friendly, same value for both axes).

    Raises
    ------
    RuntimeError
        If called outside a CASA session (casatools not importable).
    ValueError
        If ms_paths is empty or none of the MS files can be opened.
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

    # Collect per-MS metadata
    max_baseline_m = 0.0
    min_mean_freq_hz = float("inf")
    antenna_diam_m = None  # will use the first MS's median antenna diameter

    msmd = casatools.msmetadata()

    for ms_path in ms_paths:
        try:
            msmd.open(ms_path)

            # Maximum baseline length
            try:
                bl = msmd.baselines(unit="m")
                ms_max_bl = float(np.max(np.abs(bl)))
                max_baseline_m = max(max_baseline_m, ms_max_bl)
            except Exception:
                pass

            # Mean frequency across all SPWs (use the lowest to set imsize)
            try:
                n_spw = msmd.nspw()
                freqs = [msmd.meanfreq(spw=i) for i in range(n_spw) if msmd.nchan(i) > 0]
                if freqs:
                    ms_mean_freq = float(np.mean(freqs))
                    min_mean_freq_hz = min(min_mean_freq_hz, ms_mean_freq)
            except Exception:
                pass

            # Antenna diameter (primary beam size)
            if antenna_diam_m is None:
                try:
                    diams = msmd.antennadiameter()
                    # diams is a dict {antenna_id: {'unit': 'm', 'value': d}}
                    vals = [v["value"] for v in diams.values() if v["value"] > 0]
                    if vals:
                        antenna_diam_m = float(np.median(vals))
                except Exception:
                    pass

        finally:
            msmd.done()

    if max_baseline_m <= 0:
        raise ValueError("Could not determine maximum baseline from the provided MS files.")
    if min_mean_freq_hz == float("inf"):
        raise ValueError("Could not determine mean frequency from the provided MS files.")
    if antenna_diam_m is None:
        # Fallback: assume 12m antennas
        antenna_diam_m = 12.0

    # -----------------------------------------------------------------
    # Cell size
    # -----------------------------------------------------------------
    # Natural-weighting resolution: θ_nat ≈ λ / B_max  (radians)
    wavelength_m = C_MS / min_mean_freq_hz
    theta_nat_rad = wavelength_m / max_baseline_m

    # Apply robust scale factor and minor-axis fraction.
    # The minor axis of the synthesised beam is typically ~70–80 % of the
    # major axis for ALMA configurations; 0.75 is a conservative mid-point.
    minor_axis_fraction = 0.75
    scale = _robust_scale_factor(robust)
    theta_minor_rad = theta_nat_rad * scale * minor_axis_fraction

    theta_minor_arcsec = math.degrees(theta_minor_rad) * 3600.0
    cell_arcsec = theta_minor_arcsec / pixels_per_beam

    # Round to 3 significant figures (avoids spurious precision)
    magnitude = 10 ** math.floor(math.log10(cell_arcsec))
    cell_arcsec = round(cell_arcsec / magnitude, 2) * magnitude
    cell_str = f"{cell_arcsec:.4f}arcsec"

    # -----------------------------------------------------------------
    # Image size
    # -----------------------------------------------------------------
    # Primary beam FWHM: θ_pb ≈ 1.13 × λ / D  (radians)
    theta_pb_rad = 1.13 * wavelength_m / antenna_diam_m
    theta_pb_arcsec = math.degrees(theta_pb_rad) * 3600.0

    # Extent = pb_coverage_factor × diameter (radius × 2)
    extent_arcsec = pb_coverage_factor * theta_pb_arcsec
    min_imsize = int(math.ceil(extent_arcsec / cell_arcsec))

    # Ensure minimum of 64 pixels and FFT-friendly
    min_imsize = max(min_imsize, 64)
    imsize = _next_fft_friendly_size(min_imsize)

    return cell_str, imsize
