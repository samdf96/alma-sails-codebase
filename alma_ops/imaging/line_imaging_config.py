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
    "niter":       10000,
    "gain":        0.1,
    "cyclefactor": 2.0,
    "threshold":   "0.0mJy",   # masking drives stopping, not threshold
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
