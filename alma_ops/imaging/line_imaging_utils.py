"""
line_imaging_utils.py
------------------------
Utilities for deep (niter>0) line imaging of one human-confirmed ADMIT
detection. Must be called from inside a CASA session (uses casatools/casatasks).

Required inputs are the continuum-subtracted MS (db.get_selfcal_products(...,
continuum_subtracted=True)) and two DB rows/columns (line_triage_pipeline_state
.triage_products, line_admit_detection). None of these are touched by
line_admit_utils.purge_triage_cube_if_reviewed, which only deletes the
on-disk triage FITS file -- so review_line_detections.py --purge-triage-cube
is safe to run at any time relative to deep imaging, independent of ordering.

Frequency-window math
----------------------
A confirmed line_admit_detection row stores frequency_ghz (matched rest
frequency, or peak frequency if unidentified), velocity_kms, and
startchan/endchan on the triage cube's coarse/binned channel grid -- not
directly reusable against the MS's native channels.

Two frame transforms are involved, not one -- this was gotten wrong in an
earlier version of this module (only the first was inverted), which
systematically mis-centered the imaging window by ~2x vlsr for every
detection (confirmed against a real run: ~8.5 km/s off for a source with
vlsr=8.5 km/s, verified in CARTA -- real emission sat right at the edge of
the produced cube instead of centered).

1. ADMIT's specutil.getspectrum() runs *every* frequency through
   utils.undoppler(freq_sky, vlsr) = freq_sky / (1 - vlsr/c) -- shifting the
   raw sky/LSRK frequency into a "source rest frame" -- before any line
   searching happens at all. So every "freq" LineID_AT.py subsequently works
   with (including the one in the velocity formula below) is this shifted
   quantity, not the raw sky frequency.
2. LineID_AT.py's own velocity formula (utils.freqtovel), applied to that
   already-shifted freq:

       velocity = c * (freq_rf - frequency_ghz) / freq_rf + vlsr

Inverting both steps to recover the true sky/LSRK frequency to center the
imaging window on:

    freq_rf       = frequency_ghz / (1 - (velocity_kms - vlsr_kms) / C_KMS)   # undo step 2
    freq_peak_ghz = freq_rf * (1 - vlsr_kms / C_KMS)                          # undo step 1 (undoppler)

This holds uniformly for identified and "Unknown" detections -- for Unknown
rows ADMIT sets velocity_kms == vlsr_kms, so step 2's denominator is exactly
1 and freq_rf == frequency_ghz, but step 1 (the undoppler inversion) still
applies and is NOT a no-op even then.

restfreq for tclean uses frequency_ghz (the detection's own matched/peak
value), not freq_peak_ghz -- the former is the transition's rest frequency
(the image's velocity-axis zero point), the latter is where that transition
actually falls in the sky/LSRK frame for this source's own vlsr.
"""

from __future__ import annotations

import math
import os
import re
import shutil

from alma_ops.imaging.continuum_config import DEFAULT_ROBUST, select_automask
from alma_ops.imaging.line_imaging_config import (
    LINE_IMAGING_PAD_MAX_CHANNELS,
    BLANK_CHANNEL_WARN_FRAC,
    LINE_IMAGING_PAD_FLOOR_KMS,
    LINE_IMAGING_PAD_FRACTION,
    TCLEAN_LINE_IMAGING_SHARED,
)
from alma_ops.imaging.species_shorthand import shorthand_for

C_KMS = 299_792.458  # speed of light, km/s


def resolve_line_imaging_inputs(conn, line_admit_detection_id: int) -> dict:
    """DB-only chain-walk from a confirmed line_admit_detection to everything
    needed to image it. No CASA needed -- pure DB reads, mirroring
    line_admit_utils.purge_triage_cube_if_reviewed's own
    line_admit_detection -> line_admit_unit -> line_triage_unit walk.

    Returns
    -------
    dict
        {"detection", "mous_id", "target_name", "spw_idx", "ms_paths",
         "bin_factor", "vlsr_kms", "is_mosaic"}.
    """
    import json

    from alma_ops import db

    detection = db.get_line_admit_detection(conn, line_admit_detection_id)
    if detection is None:
        raise ValueError(f"No line_admit_detection_id={line_admit_detection_id} found")

    admit_state = db.get_line_admit_pipeline_state(conn, detection["line_admit_unit_id"])
    if admit_state is None:
        raise ValueError(
            f"No line_admit_pipeline_state for line_admit_unit_id={detection['line_admit_unit_id']}"
        )

    triage_state = db.get_line_triage_pipeline_state(conn, admit_state["line_triage_unit_id"])
    if triage_state is None or not triage_state["triage_products"]:
        raise ValueError(
            f"No triage_products for line_triage_unit_id={admit_state['line_triage_unit_id']}"
        )
    triage_products = json.loads(triage_state["triage_products"])[-1]

    mous_id = admit_state["mous_id"]
    target_name = admit_state["target_name"]

    ms_paths = db.get_selfcal_products(conn, mous_id, continuum_subtracted=True)
    if not ms_paths:
        raise ValueError(f"No continuum-subtracted selfcal products for {mous_id}")

    target_row = next(
        (r for r in db.get_mous_targets(conn, mous_id) if r["alma_source_name"] == target_name),
        None,
    )
    if target_row is None or target_row["vlsr_kms"] is None:
        raise ValueError(f"No vlsr_kms set for source '{target_name}' (mous {mous_id})")

    return {
        "detection": detection,
        "mous_id": mous_id,
        "target_name": target_name,
        "spw_idx": admit_state["spw_idx"],
        "ms_paths": [str(p) for p in ms_paths],
        "bin_factor": triage_products["bin_factor"],
        "vlsr_kms": target_row["vlsr_kms"],
        "is_mosaic": target_row["is_mosaic"] == "T",
    }


def compute_line_window(
    ms_path,
    spw_idx: int,
    detection,
    vlsr_kms: float,
    bin_factor: int,
    pad_fraction: float = LINE_IMAGING_PAD_FRACTION,
    pad_floor_kms: float = LINE_IMAGING_PAD_FLOOR_KMS,
    pad_max_channels: int = LINE_IMAGING_PAD_MAX_CHANNELS,
) -> dict:
    """Compute the deep-imaging frequency window for one confirmed detection.

    Re-queries native_chanwidth_hz/center_freq_hz live via casatools.msmetadata
    on the actual MS being imaged (matching line_triage_utils.compute_bin_factor's
    own approach) rather than trusting the stored triage-time value; bin_factor
    itself must come from the stored triage_products value, since it's a
    triage-time design choice, not a re-derivable MS property.

    Parameters
    ----------
    ms_path:
        Any one contsub MS for this MOUS (used to query the native channel grid).
    spw_idx:
        Spectral window index (local to ms_path).
    detection:
        A line_admit_detection row (frequency_ghz, velocity_kms, startchan, endchan).
    vlsr_kms:
        Source VLSR, from targets.vlsr_kms.
    bin_factor:
        Native channels averaged per triage-cube channel (from triage_products).
    pad_fraction, pad_floor_kms, pad_max_channels:
        Per-side padding around the detection's empirical linewidth. The pad is
        pad_fraction x linewidth, floored at pad_floor_kms so a narrow line still
        gets usable baseline, and capped at pad_max_channels native channels so a
        broad one does not run away -- padding proportional to a very wide line
        buys nothing and pushes the window past the SPW edge.

    Returns
    -------
    dict
        {"freq_peak_ghz", "linewidth_kms", "lo_ghz", "hi_ghz",
         "native_chanwidth_hz", "center_freq_hz"}.
    """
    try:
        import casatools
        import numpy as np
    except ImportError:
        raise RuntimeError("compute_line_window must be called from inside a CASA session.")

    msmd = casatools.msmetadata()
    msmd.open(str(ms_path))
    try:
        native_chanwidth_hz = float(abs(np.asarray(msmd.chanwidths(spw_idx))[0]))
        center_freq_hz = float(msmd.meanfreq(spw_idx))
        _chan_freqs = np.asarray(msmd.chanfreqs(spw_idx), dtype=float)
        spw_lo_ghz = float(_chan_freqs.min()) / 1e9
        spw_hi_ghz = float(_chan_freqs.max()) / 1e9
    finally:
        msmd.done()

    frequency_ghz = detection["frequency_ghz"]
    velocity_kms = detection["velocity_kms"]
    # Two frame transforms to undo, not one -- see module docstring. ADMIT's
    # own specutil.getspectrum() undopplers every frequency into a source
    # rest frame *before* line searching, so LineID_AT.py's velocity formula
    # operates on that shifted quantity (freq_rf), not the raw sky frequency.
    freq_rf = frequency_ghz / (1 - (velocity_kms - vlsr_kms) / C_KMS)   # undo LineID_AT's velocity formula
    freq_peak_ghz = freq_rf * (1 - vlsr_kms / C_KMS)                    # undo ADMIT's own undoppler()

    n_chan_detection = detection["endchan"] - detection["startchan"] + 1
    linewidth_kms = n_chan_detection * bin_factor * native_chanwidth_hz / center_freq_hz * C_KMS

    chan_kms = native_chanwidth_hz / center_freq_hz * C_KMS
    pad_kms = max(pad_fraction * linewidth_kms, pad_floor_kms)
    # Ceiling in channels: a broad line needs enough baseline to sit against,
    # not padding proportional to its own width.
    pad_kms = min(pad_kms, pad_max_channels * chan_kms)

    half_window_kms = linewidth_kms / 2 + pad_kms
    lo_ghz = freq_peak_ghz * (1 - half_window_kms / C_KMS)
    hi_ghz = freq_peak_ghz * (1 + half_window_kms / C_KMS)

    # Clip to the SPW's own extent. Channels beyond it can never be filled --
    # tclean blanks them, inflating the cube for no signal (det 461 came back
    # 43% empty this way).
    clipped_lo = lo_ghz < spw_lo_ghz
    clipped_hi = hi_ghz > spw_hi_ghz
    lo_ghz = max(lo_ghz, spw_lo_ghz)
    hi_ghz = min(hi_ghz, spw_hi_ghz)
    if clipped_lo or clipped_hi:
        print(f"  [window] clipped to SPW extent "
              f"({'low ' if clipped_lo else ''}{'high' if clipped_hi else ''}): "
              f"{lo_ghz:.6f}-{hi_ghz:.6f} GHz", flush=True)

    return {
        "freq_peak_ghz": freq_peak_ghz,
        "linewidth_kms": linewidth_kms,
        "lo_ghz": lo_ghz,
        "hi_ghz": hi_ghz,
        "native_chanwidth_hz": native_chanwidth_hz,
        "center_freq_hz": center_freq_hz,
        "pad_kms": pad_kms,
        "clipped_to_spw": bool(clipped_lo or clipped_hi),
    }


def _blank_channel_fraction(image_path: str) -> float | None:
    """Fraction of channels in a CASA image with no data at all.

    A channel that gridded no visibilities is exactly zero everywhere, so the
    per-channel maximum is the cheapest reliable test. Returns None if the image
    cannot be inspected -- a diagnostic must never be the thing that fails a run.
    """
    try:
        import casatools
        ia = casatools.image()
        ia.open(image_path)
        try:
            stats = ia.statistics(axes=[0, 1])
        finally:
            ia.close()
    except Exception as exc:
        print(f"  [WARNING] blank-channel check skipped: {type(exc).__name__}: {exc}",
              flush=True)
        return None

    maxima = stats.get("max", [])
    try:
        values = [float(v) for v in maxima]
    except TypeError:
        values = [float(maxima)]
    if not values:
        return None
    return sum(1 for v in values if v == 0.0) / len(values)


def build_img_base(
    imaging_dir, target_name: str, mous_dir_id: str, spw_idx: int,
    species_name: str | None, frequency_ghz: float, detection_id: int,
    robust: float = DEFAULT_ROBUST,
) -> str:
    """Build the shared image basename, used by both run_line_imaging (to
    write to) and reset_line_imaging.py (to know what to delete without
    depending on imaging_products, which may not exist yet after a crash).
    Trailing det{detection_id} guarantees uniqueness even if two "Unknown"
    lines round to the same frequency in the same spw.

    Every robust value gets an explicit _r<NN> suffix (0.5 -> _r05, 2.0 -> _r20),
    so the weighting is always readable from the path and the same line can be
    imaged at several weightings side by side. Without it an r=2.0 run would
    silently overwrite the r=0.5 products. Note this renames products relative
    to earlier runs, which recorded no suffix at all -- any imaging_products
    paths written before this change point at filenames that no longer exist. Folds in the curated
    shorthand (species_shorthand.shorthand_for) alongside the full species
    slug when one exists, since the full descriptive name alone (e.g.
    "Cyclopropenylidene") is slower to scan on disk than its formula."""
    species_slug = re.sub(r"[^A-Za-z0-9]+", "_", (species_name or "Unknown")).strip("_")
    shorthand = shorthand_for(species_name)
    if shorthand:
        species_slug += "_" + re.sub(r"[^A-Za-z0-9]+", "_", shorthand).strip("_")
    # 0.5 -> "_r05", 2.0 -> "_r20", -1.0 -> "_rm10". Two digits keeps the
    # common values aligned on disk; the m prefix avoids a bare "-" in a filename.
    tenths = int(round(robust * 10))
    suffix = f"_rm{abs(tenths):02d}" if tenths < 0 else f"_r{tenths:02d}"
    return os.path.join(
        str(imaging_dir),
        f"{target_name}_{mous_dir_id}_spw{spw_idx}_{species_slug}_"
        f"{frequency_ghz:.4f}GHz_det{detection_id}{suffix}",
    )


def run_line_imaging(
    mous_dir_id: str,
    target_name: str,
    spw_idx: int,
    detection_id: int,
    species_name: str | None,
    frequency_ghz: float,
    freq_peak_ghz: float,
    lo_ghz: float,
    hi_ghz: float,
    native_chanwidth_hz: float,
    ms_paths: list,
    imaging_dir: str,
    robust: float = DEFAULT_ROBUST,
    is_mosaic: bool = False,
    keep_casa_products: bool = False,
    parallel: bool = False,
) -> dict:
    """Run tclean + impbcor + exportfits for one confirmed line detection.

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
    detection_id:
        line_admit_detection_id, used in the output filename for uniqueness.
    species_name:
        Detection's species_name (None/"Unknown" for unidentified lines).
    frequency_ghz:
        Detection's matched/peak rest frequency -- used as tclean's restfreq
        (the image's velocity-axis zero point).
    freq_peak_ghz, lo_ghz, hi_ghz, native_chanwidth_hz:
        Output of compute_line_window -- the sky/LSRK-frame window to image.
    ms_paths:
        Continuum-subtracted selfcal product measurement sets for this MOUS.
    imaging_dir:
        Directory in which all output products are written.
    robust:
        Briggs robust weighting parameter.
    is_mosaic:
        If True, use gridder='mosaic' and compute_mosaic_imaging_params
        instead of compute_imaging_params, same reasoning as continuum/triage.
    keep_casa_products:
        If False (default), delete CASA-format tclean intermediates after
        FITS export.
    parallel:
        Passed through to tclean's parallel= (mpicasa fixed-mode sessions only).

    Returns
    -------
    dict
        {"image_fits", "image_pbcor_fits", "freq_peak_ghz", "lo_ghz", "hi_ghz",
         "casa_products_cleaned"}.
    """
    from alma_ops.imaging.cont_dat_utils import cont_dat_to_spw_string
    from alma_ops.imaging.imaging_utils import (
        compute_b75,
        compute_imaging_params,
        compute_mosaic_imaging_params,
    )

    try:
        from casatasks import exportfits, impbcor, tclean
    except ImportError:
        raise RuntimeError("run_line_imaging must be called from inside a CASA session.")

    img_base = build_img_base(
        imaging_dir, target_name, mous_dir_id, spw_idx, species_name, frequency_ghz,
        detection_id, robust,
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

    print("  [params] computing B75 …", flush=True)
    b75 = compute_b75(ms_paths)
    automask = select_automask(b75)
    print(f"    B75={b75:.0f}m", flush=True)

    print("  [params] computing cell size and image size …", flush=True)
    phasecenter = None
    if is_mosaic:
        cell_str, imsize, phasecenter = compute_mosaic_imaging_params(ms_paths, target_name, robust=robust)
        print(f"    cell={cell_str}  imsize={imsize}  gridder=mosaic  phasecenter={phasecenter}", flush=True)
    else:
        cell_str, imsize = compute_imaging_params(ms_paths, robust=robust)
        print(f"    cell={cell_str}  imsize={imsize}  gridder=standard", flush=True)

    # Per-MS spw:channel selection, centered on this one line -- reuses
    # cont_dat_to_spw_string unchanged with a synthesized single-entry dict,
    # inheriting its existing convention of comparing LSRK-quoted frequencies
    # directly against msmd.chanfreqs() with no explicit TOPO->LSRK
    # conversion (an accepted simplification already used for triage).
    vis_list: list = []
    spw_list: list = []
    for ms_path in ms_paths:
        # lo_ghz/hi_ghz come from compute_line_window in LSRK; the MS stores TOPO
        # channel frequencies. ranges_frame makes the conversion explicit -- without
        # it the comparison picks channels 31-70 MHz off and tclean grids nothing.
        spw_str = cont_dat_to_spw_string(
            ms_path, {spw_idx: [(lo_ghz, hi_ghz)]},
            ranges_frame="LSRK", field=target_name,
        )
        if not spw_str:
            print(f"  [WARNING] Empty channel selection for {ms_path} — skipping.", flush=True)
            continue
        vis_list.append(ms_path)
        spw_list.append(spw_str)
        print(f"    {ms_path}  spw: {spw_str}", flush=True)

    if not vis_list:
        raise ValueError(
            f"No usable channels for detection {detection_id} ({species_name}, "
            f"{frequency_ghz:.4f} GHz) in window [{lo_ghz:.6f}, {hi_ghz:.6f}] GHz."
        )

    # Frequency-mode gridding (not channel-index gridding), to avoid the
    # nchan=-1 overshoot risk line_triage_utils.compute_bin_factor's own
    # docstring warns about.
    nchan = math.ceil((hi_ghz - lo_ghz) * 1e9 / native_chanwidth_hz)

    tclean_params = dict(TCLEAN_LINE_IMAGING_SHARED)
    tclean_params.update(automask)
    tclean_params.update({
        "vis":       vis_list,
        "spw":       spw_list,
        "imagename": img_base,
        "field":     target_name,
        "imsize":    [imsize, imsize],
        "cell":      cell_str,
        "start":     f"{lo_ghz:.10f}GHz",
        "width":     f"{native_chanwidth_hz:.2f}Hz",
        "nchan":     nchan,
        "restfreq":  [f"{frequency_ghz:.6f}GHz"],
        "robust":    robust,
        "parallel":  parallel,
        "gridder":   "mosaic" if is_mosaic else "standard",
    })
    if phasecenter is not None:
        tclean_params["phasecenter"] = phasecenter

    # TempLattice files are written to cwd by CASA, not to imagename's directory.
    _orig_dir = os.getcwd()
    os.chdir(imaging_dir)
    try:
        print(f"  [tclean] starting … (nchan={nchan}, parallel={parallel})", flush=True)
        tclean(**tclean_params)
        print("  [tclean] finished.", flush=True)
    finally:
        os.chdir(_orig_dir)

    image_path      = img_base + ".image"
    pbcor_image      = img_base + ".image.pbcor"
    image_fits_path   = img_base + ".image.fits"
    pbcor_fits_path    = img_base + ".image.pbcor.fits"

    # Guard: tclean raises nothing when its channel selection lands outside the
    # requested grid -- it grids no visibilities, writes an all-zero cube, and
    # exports cleanly. Six cubes were recorded 'complete' that way before anyone
    # noticed (BLANK_CUBES_REPORT.md). Fail loudly instead.
    blank_frac = _blank_channel_fraction(image_path)
    if blank_frac is not None:
        print(f"  [check] empty channels: {blank_frac*100:.1f}%", flush=True)
        if blank_frac >= 1.0:
            raise RuntimeError(
                f"tclean gridded no data for detection {detection_id} "
                f"({species_name}, {frequency_ghz:.4f} GHz): every channel of "
                f"{image_path} is empty. The channel selection almost certainly "
                f"falls outside the requested frequency grid -- check the frame of "
                f"the window against the MS channel frequencies."
            )
        if blank_frac > BLANK_CHANNEL_WARN_FRAC:
            print(f"  [WARNING] {blank_frac*100:.1f}% of channels are empty "
                  f"(> {BLANK_CHANNEL_WARN_FRAC*100:.0f}%). The window may only "
                  f"partly overlap the data.", flush=True)

    print("  [impbcor] …", flush=True)
    impbcor(imagename=image_path, pbimage=img_base + ".pb", outfile=pbcor_image, overwrite=True)

    print(f"  [exportfits] → {image_fits_path}", flush=True)
    exportfits(imagename=image_path, fitsimage=image_fits_path, dropdeg=True, dropstokes=True, overwrite=True)

    print(f"  [exportfits] → {pbcor_fits_path}", flush=True)
    exportfits(imagename=pbcor_image, fitsimage=pbcor_fits_path, dropdeg=True, dropstokes=True, overwrite=True)

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
        "image_fits":            image_fits_path,
        "image_pbcor_fits":      pbcor_fits_path,
        "freq_peak_ghz":          freq_peak_ghz,
        "lo_ghz":                  lo_ghz,
        "hi_ghz":                   hi_ghz,
        "casa_products_cleaned":     not keep_casa_products,
        "robust":                     robust,
    }
    if phasecenter is not None:
        result["phasecenter"] = phasecenter
    if keep_casa_products:
        result["image"]       = image_path
        result["image_pbcor"] = pbcor_image
    return result
