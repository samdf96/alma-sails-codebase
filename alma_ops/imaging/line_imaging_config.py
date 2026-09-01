"""
line_imaging_config.py
------------------------
Shared tclean configuration for deep (niter>0) line imaging of confirmed
ADMIT detections.

B75-based automask selection, DEFAULT_ROBUST, and image-geometry parameters
are not duplicated here -- imported directly from continuum_config, since
auto-multithresh threshold recommendations are driven by array configuration
(B75), not by specmode, and apply identically to a cube plane.

deconvolver='hogbom' (not continuum's 'multiscale') is a deliberate choice:
a per-channel cube plane has far less integration/bandwidth per pixel than a
full-bandwidth mfs continuum image, where multiscale is more prone to fitting
noise as spurious extended structure -- at a cost paid on every channel.
line_triage_config.py made the same choice for the (niter=0, so moot there)
dirty cubes. Revisit per-source if a target shows strong, well-resolved
extended line structure that would genuinely benefit from multiscale.
"""

from __future__ import annotations

# Padding applied on each side of a confirmed detection's empirical linewidth
# when building the deep-imaging frequency window (see line_imaging_utils).
LINE_IMAGING_PAD_FRACTION: float = 1.0   # per side, as a multiple of measured linewidth
LINE_IMAGING_PAD_FLOOR_KMS: float = 5.0  # minimum per-side padding regardless of measured width

# Ceiling on per-side padding, in NATIVE channels. Padding is a fraction of the
# measured linewidth, which is right for narrow lines and wrong for broad ones:
# a 564-channel SiO outflow (det 461) asked for a window three times its own
# width, 43% of which fell outside the SPW entirely and came back blank. A wide
# line needs only enough context to see its baseline, not proportionally more.
LINE_IMAGING_PAD_MAX_CHANNELS: int = 128

# Warn when more than this fraction of a finished cube's channels gridded no
# data. 100% blank is always a hard failure; a smaller fraction usually means
# the frequency window only partly overlaps the selected data.
BLANK_CHANNEL_WARN_FRAC: float = 0.10

# tclean parameters shared by all deep line-imaging jobs. Not included here
# (computed or supplied per job): vis, spw, imagename, field, imsize, cell,
# start, width, nchan, restfreq, robust, phasecenter (mosaic only), parallel.
TCLEAN_LINE_IMAGING_SHARED: dict = {
    "specmode":    "cube",
    "deconvolver": "hogbom",
    "gridder":     "standard",
    "weighting":   "briggs",
    "outframe":    "LSRK",
    "veltype":     "radio",
    "pbcor":       False,      # impbcor applied as a separate step, same as continuum
    # Raised from 10000 alongside nsigma below. On its own a larger budget would
    # just clean deeper into the noise; nsigma is what stops each cube at its own
    # noise level, and the budget is only consumed by cubes that need it to get
    # there. Measured on three cubes at niter=10000: det 1433 spent the whole
    # budget reaching 0.26 sigma (over-cleaned across 54 major cycles), while
    # det 435 and det 461 hit the limit still at 9.4 and 14.9 sigma with
    # auto-multithresh actively expanding the mask -- genuinely under-cleaned.
    "niter":       50000,
    "gain":        0.1,
    "cyclefactor": 2.0,
    "threshold":   "0.0mJy",   # superseded by nsigma; stop is max(threshold, nsigma*rms)
    # Per-channel for a cube, so each plane stops at its own noise rather than a
    # cube-wide value -- the per-channel rms varies by 1.7-3.3x across the cubes
    # measured here.
    "nsigma":      3.0,
    # As for the continuum: with nsigma setting the stopping point, a MAD noise
    # estimate biased by bright emission would stop the clean in the wrong place.
    "fastnoise":   False,
    "usemask":     "auto-multithresh",
    "interactive": False,
    "savemodel":   "none",

    # One beam for the whole cube instead of CASA's default per-channel beam.
    # A varying beam is what spectral_cube's VaryingResolutionSpectralCube
    # refuses to average over: real cubes here showed ~6% channel-to-channel
    # variation against its 1% default tolerance, which blocked scousepy's
    # moment maps until the threshold was relaxed by hand. These are final
    # science cubes feeding kinematic decomposition, so fix it at the source.
    "restoringbeam": "common",

    # Pinned rather than left to the CASA default (currently True). Correct for
    # Briggs-weighted cubes -- weight density is computed per channel instead of
    # over the collapsed cube, which keeps the beam and noise stable across the
    # band. Explicit so a CASA version bump cannot silently change final products.
    "perchanweightdensity": True,
}
