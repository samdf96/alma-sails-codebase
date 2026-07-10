"""
line_triage_config.py
----------------------
Shared tclean configuration for ADMIT-bound line-triage cube imaging.

Triage cubes are dirty (niter=0) per-spw images used only to let ADMIT scan
for line structures before committing to deep, science-grade line imaging.
No rest frequency or molecule ID is assumed; channel binning is bounded by a
minimum protected linewidth so that narrow, as-yet-unidentified lines aren't
smeared out (see line_triage_utils.compute_bin_factor).
"""

from __future__ import annotations

# Narrowest line FWHM (km/s) we guarantee survives binning — appropriate for
# cold (~10-20K), quiescent protostellar envelope/core gas (e.g. C18O, H13CO+).
THERMAL_LINEWIDTH_FLOOR_KMS: float = 0.3

# Native channels required across the floor linewidth, after binning.
MIN_CHANNELS_ACROSS_LINE: int = 3

# Hard cap on the bin factor regardless of the computed value.
MAX_BIN_FACTOR: int = 16

# Native channel width (kHz) at or above which a spw is treated as a
# continuum-correlator-setup window regardless of cont.dat — cont.dat only
# reports where auto_selfcal's findcont happened to flag line emission, not
# whether the window was ever designed to search for one. Wide, coarse
# windows (classic TDM continuum, or channel-averaged FDM continuum) are
# typically >=2000 kHz; real line-search FDM windows in this survey have
# been observed at 60-1000 kHz. See classify_spws.
CONTINUUM_CHANWIDTH_THRESHOLD_KHZ: float = 2000.0

# tclean parameters shared by all triage cubes. Not included here (computed
# or supplied per job): vis, spw, imagename, imsize, cell, start, width, nchan.
TCLEAN_LINE_TRIAGE_SHARED: dict = {
    "specmode":    "cube",
    "deconvolver": "hogbom",
    "gridder":     "standard",
    "weighting":   "natural",
    "outframe":    "LSRK",
    "niter":       0,          # dirty image — no cleaning, no mask needed
    "interactive": False,
    "pbcor":       False,
    "savemodel":   "none",
}
