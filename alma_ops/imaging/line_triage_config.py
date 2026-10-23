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

# Largest triage image (pixels per side) imaged without --taper-arcsec or
# --allow-large. Every survey triage cube through 2026-10 was <=2000 px; the
# two that were not (OOser X186d at 10000, EC_53 X20a0 at 8640, both
# 0.015-0.03" long-baseline data) needed 2-3 TB per spw at native resolution
# and could not run. A dirty triage cube only has to show ADMIT where lines
# are, so such data is tapered rather than imaged at full resolution.
TRIAGE_MAX_IMSIZE: int = 4096

# Peak tclean memory per image pixel per channel, measured on survey triage
# runs (X37f7: 741 GB for 1944^2 x 3838 ch). Used only for the size estimate
# printed before imaging.
TCLEAN_BYTES_PER_VOXEL: float = 51.0

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
