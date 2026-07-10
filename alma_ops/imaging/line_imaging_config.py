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
}
