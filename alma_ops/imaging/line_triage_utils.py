"""
line_triage_utils.py
---------------------
Utilities for ADMIT-bound line-triage cube imaging. Must be called from
inside a CASA session (uses casatools/casatasks).

Spw classification
-------------------
Two independent signals, either of which is enough to call a spw
'continuum':

1. cont.dat (alma_ops.imaging.cont_dat_utils.parse_cont_dat) — a spw marked
   "ALL" is entirely line-free; a spw with line-free sub-ranges has a line in
   the gaps; a spw missing from cont.dat entirely is treated as a line
   candidate (inclusive).
2. Native channel width — cont.dat only reports where auto_selfcal's
   findcont happened to flag line emission, not whether the window was ever
   designed to search for one. A wide, coarse-channel window (TDM, or
   channel-averaged FDM) is a continuum-correlator-setup window regardless
   of what findcont reported, so it's excluded by
   CONTINUUM_CHANWIDTH_THRESHOLD_KHZ even if cont.dat reports sub-ranges.

Channel binning
---------------
No rest frequency or molecule ID is known ahead of ADMIT's line search, so
the bin factor is derived purely from the spw's own center frequency: native
channel width is converted to km/s via the non-relativistic Doppler
approximation, then binned as coarsely as possible while still resolving
THERMAL_LINEWIDTH_FLOOR_KMS with MIN_CHANNELS_ACROSS_LINE channels.
"""

from __future__ import annotations

import math

from alma_ops.imaging.line_triage_config import (
    CONTINUUM_CHANWIDTH_THRESHOLD_KHZ,
    MAX_BIN_FACTOR,
    MIN_CHANNELS_ACROSS_LINE,
    THERMAL_LINEWIDTH_FLOOR_KMS,
)

C_KMS = 299_792.458  # speed of light, km/s


def classify_spws(
    cont_dat_path,
    field_name: str,
    ms_path,
    continuum_chanwidth_threshold_khz: float = CONTINUUM_CHANWIDTH_THRESHOLD_KHZ,
) -> dict[int, str]:
    """Classify every spw in ms_path as 'continuum' or 'line'.

    Parameters
    ----------
    cont_dat_path:
        Path to the MOUS's auto_selfcal/cont.dat file.
    field_name:
        Field name to extract from cont.dat (must match a 'Field:' entry).
    ms_path:
        Path to a measurement set sharing this MOUS's spw structure (any one
        of its selfcal product MSes).
    continuum_chanwidth_threshold_khz:
        Native channel width at or above which a spw is treated as
        continuum regardless of cont.dat (see module docstring).

    Returns
    -------
    dict
        {spw_idx: "continuum" | "line"} for every spw present in the MS.
    """
    try:
        import casatools
        import numpy as np
    except ImportError:
        raise RuntimeError("classify_spws must be called from inside a CASA session.")

    from alma_ops.imaging.cont_dat_utils import parse_cont_dat

    cont_ranges = parse_cont_dat(cont_dat_path, field_name)

    msmd = casatools.msmetadata()
    msmd.open(str(ms_path))
    try:
        n_spw = msmd.nspw()
        chanwidths_khz = {
            spw_idx: float(abs(np.asarray(msmd.chanwidths(spw_idx))[0])) / 1e3
            for spw_idx in range(n_spw)
        }
    finally:
        msmd.done()

    classification = {}
    for spw_idx in range(n_spw):
        if cont_ranges.get(spw_idx) == "ALL":
            classification[spw_idx] = "continuum"
        elif chanwidths_khz[spw_idx] >= continuum_chanwidth_threshold_khz:
            classification[spw_idx] = "continuum"
        else:
            classification[spw_idx] = "line"
    return classification


def compute_bin_factor(
    ms_path,
    spw_idx: int,
    linewidth_floor_kms: float = THERMAL_LINEWIDTH_FLOOR_KMS,
    min_channels: int = MIN_CHANNELS_ACROSS_LINE,
    max_bin_factor: int = MAX_BIN_FACTOR,
) -> dict:
    """Compute the channel-binning factor for one spw, protecting narrow lines.

    Parameters
    ----------
    ms_path:
        Path to the measurement set.
    spw_idx:
        Spectral window index (local to this MS).
    linewidth_floor_kms:
        Narrowest line FWHM (km/s) that must remain resolved by at least
        ``min_channels`` channels after binning.
    min_channels:
        Native channels required across the floor linewidth, post-bin.
    max_bin_factor:
        Hard cap on the bin factor regardless of the computed value.

    Returns
    -------
    dict
        {"bin_factor", "native_chanwidth_hz", "center_freq_hz", "nchan"}.
        ``nchan`` is the native channel count floor-divided by bin_factor —
        pass this explicitly to tclean rather than nchan=-1, which (in
        specmode='cube' + outframe='LSRK') can compute an output grid wider
        than the input data actually covers, leaving zero-valued channels at
        the edges.
    """
    try:
        import casatools
        import numpy as np
    except ImportError:
        raise RuntimeError("compute_bin_factor must be called from inside a CASA session.")

    msmd = casatools.msmetadata()
    msmd.open(str(ms_path))
    try:
        native_chanwidth_hz = float(abs(np.asarray(msmd.chanwidths(spw_idx))[0]))
        center_freq_hz = float(msmd.meanfreq(spw_idx))
        native_nchan = int(msmd.nchan(spw_idx))
    finally:
        msmd.done()

    native_width_kms = native_chanwidth_hz / center_freq_hz * C_KMS
    target_max_width_kms = linewidth_floor_kms / min_channels

    bin_factor = math.floor(target_max_width_kms / native_width_kms)
    bin_factor = max(1, min(bin_factor, max_bin_factor))

    return {
        "bin_factor": bin_factor,
        "native_chanwidth_hz": native_chanwidth_hz,
        "center_freq_hz": center_freq_hz,
        "nchan": native_nchan // bin_factor,
    }


def run_line_triage_imaging(
    mous_dir_id: str,
    target_name: str,
    spw_idx: int,
    ms_paths: list,
    imaging_dir: str,
    bin_factor: int,
    nchan: int,
    is_mosaic: bool = False,
    keep_casa_products: bool = False,
) -> dict:
    """Run a dirty tclean cube for one (MOUS, spw) and export it to FITS.

    Must be called from inside a CASA session (imports casatasks internally).

    Parameters
    ----------
    mous_dir_id:
        Filesystem-safe MOUS id (alma_ops.utils.to_dir_mous_id), used in the
        output filename.
    target_name:
        CASA field name.
    spw_idx:
        Spectral window index (local to ms_paths).
    ms_paths:
        Selfcal product measurement sets for this MOUS.
    imaging_dir:
        Directory in which all output products are written.
    bin_factor:
        Number of native channels averaged per output cube channel.
    nchan:
        Explicit output channel count (compute_bin_factor's "nchan") — not
        -1, which can overshoot the input data's real coverage in
        specmode='cube' + outframe='LSRK' and leave zero-valued edge channels.
    is_mosaic:
        If True, use gridder='mosaic' and compute_mosaic_imaging_params
        instead of compute_imaging_params — selecting field=target_name
        alone only gathers the visibilities; 'standard' still rephases every
        pointing to one phase center, and a single pointing's primary beam
        only covers one pointing's worth of the image.
    keep_casa_products:
        If False (default), delete CASA-format tclean intermediates after
        FITS export.

    Returns
    -------
    dict
        {"cube_fits": <path>}.
    """
    import os
    import shutil

    try:
        from casatasks import exportfits, tclean
    except ImportError:
        raise RuntimeError("run_line_triage_imaging must be called from inside a CASA session.")

    from alma_ops.imaging.imaging_utils import compute_imaging_params, compute_mosaic_imaging_params
    from alma_ops.imaging.line_triage_config import TCLEAN_LINE_TRIAGE_SHARED

    img_base = os.path.join(
        imaging_dir, f"{target_name}_{mous_dir_id}_spw{spw_idx}_triage"
    )

    _log_path = img_base + ".casa.log"
    import sys as _sys
    _casalog = getattr(_sys.modules.get("__main__"), "casalog", None)
    if _casalog is None:
        try:
            import casatools as _ct
            _casalog = getattr(_ct, "casalog", None)
        except ImportError:
            pass
    if _casalog is not None:
        try:
            _casalog.setlogfile(_log_path)
            print(f"  [log] CASA log → {_log_path}", flush=True)
        except Exception as _e:
            print(f"  [WARNING] casalog.setlogfile({_log_path!r}) failed: {type(_e).__name__}: {_e}", flush=True)
    else:
        print(f"  [WARNING] casalog not found; CASA log will not be redirected to {_log_path}", flush=True)

    print("  [params] computing cell size and image size …", flush=True)
    phasecenter = None
    if is_mosaic:
        cell_str, imsize, phasecenter = compute_mosaic_imaging_params(ms_paths, target_name, robust=2.0)
        print(f"    cell={cell_str}  imsize={imsize}  gridder=mosaic  phasecenter={phasecenter}", flush=True)
    else:
        cell_str, imsize = compute_imaging_params(ms_paths, robust=2.0)
        print(f"    cell={cell_str}  imsize={imsize}  gridder=standard", flush=True)

    tclean_params = dict(TCLEAN_LINE_TRIAGE_SHARED)
    tclean_params["gridder"] = "mosaic" if is_mosaic else "standard"
    if phasecenter is not None:
        tclean_params["phasecenter"] = phasecenter
    tclean_params.update({
        "vis":       ms_paths,
        "spw":       str(spw_idx),
        "imagename": img_base,
        "field":     target_name,
        "imsize":    [imsize, imsize],
        "cell":      cell_str,
        "start":     0,
        "width":     str(bin_factor),
        "nchan":     nchan,
    })

    _orig_dir = os.getcwd()
    os.chdir(imaging_dir)
    try:
        print(f"  [tclean] starting (spw={spw_idx}, bin_factor={bin_factor}, nchan={nchan}) …", flush=True)
        tclean(**tclean_params)
        print("  [tclean] finished.", flush=True)
    finally:
        os.chdir(_orig_dir)

    image_path = img_base + ".image"
    cube_fits_path = img_base + ".image.fits"

    print(f"  [exportfits] → {cube_fits_path}", flush=True)
    exportfits(
        imagename=image_path,
        fitsimage=cube_fits_path,
        dropdeg=True,
        dropstokes=True,
        overwrite=True,
    )

    _CASA_SUFFIXES = [
        ".image", ".mask", ".model",
        ".pb", ".psf", ".residual", ".sumwt", ".weight",
    ]
    if not keep_casa_products:
        for suffix in _CASA_SUFFIXES:
            p = img_base + suffix
            if os.path.exists(p):
                shutil.rmtree(p, ignore_errors=True)
        print("  [cleanup] CASA intermediates removed.", flush=True)

    print(f"  [done] {cube_fits_path}", flush=True)

    result = {"cube_fits": cube_fits_path}
    if phasecenter is not None:
        result["phasecenter"] = phasecenter
    return result
