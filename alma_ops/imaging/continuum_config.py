"""
continuum_config.py
-------------------
Shared tclean configuration for ALMA-SAILS continuum imaging.

All parameters here are project-wide defaults. Per-source values (phasecenter,
imsize, cell, spw selection) are computed at runtime from the measurement sets.

Auto-multithresh masking
------------------------
Three parameter sets are provided, selected by the 75th-percentile baseline
length (B75) computed from the combined antenna positions across all input MSes.
Longer baselines resolve out extended emission, so tighter thresholds are
appropriate; shorter baselines (7m, compact 12m) capture extended emission and
need more permissive thresholds to avoid masking real signal.

    B75 < B75_COMPACT_MAX          →  AUTOMASK_COMPACT      (7m / compact 12m)
    B75_COMPACT_MAX ≤ B75 < B75_EXTENDED_MIN  →  AUTOMASK_INTERMEDIATE
    B75 ≥ B75_EXTENDED_MIN         →  AUTOMASK_EXTENDED     (extended 12m)

Threshold values follow the CASA 6 auto-multithresh documentation
recommendations for ALMA data.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# B75 threshold boundaries (metres)
# ---------------------------------------------------------------------------

B75_COMPACT_MAX   = 300.0    # below this → compact (7m, compact 12m)
B75_EXTENDED_MIN  = 400.0    # above this → extended 12m configs

# ---------------------------------------------------------------------------
# Auto-multithresh parameter sets
# ---------------------------------------------------------------------------

# Compact arrays / 7m — extended emission dominant, aggressive masking needed
AUTOMASK_COMPACT: dict[str, float] = {
    "sidelobethreshold": 1.25,
    "noisethreshold":    5.0,
    "lownoisethreshold": 2.0,
    "minbeamfrac":       0.1,
    "negativethreshold": 0.0,
}

# Typical 12m configurations — balanced extended + compact emission
AUTOMASK_INTERMEDIATE: dict[str, float] = {
    "sidelobethreshold": 2.0,
    "noisethreshold":    4.25,
    "lownoisethreshold": 1.5,
    "minbeamfrac":       0.3,
    "negativethreshold": 0.0,
}

# Extended 12m configurations — compact/point emission dominant
AUTOMASK_EXTENDED: dict[str, float] = {
    "sidelobethreshold": 3.0,
    "noisethreshold":    5.0,
    "lownoisethreshold": 1.5,
    "minbeamfrac":       0.3,
    "negativethreshold": 0.0,
}

# ---------------------------------------------------------------------------
# Shared tclean parameters
# ---------------------------------------------------------------------------

# Parameters that are constant across all ALMA-SAILS continuum jobs.
# Not included here (computed or supplied per job):
#   vis, spw, imagename, field, phasecenter, imsize, cell, robust,
#   usemask, sidelobethreshold, noisethreshold, lownoisethreshold,
#   minbeamfrac, negativethreshold
TCLEAN_SHARED: dict = {
    "specmode":    "mfs",
    "deconvolver": "multiscale",
    "scales":      [0, 5, 15, 45],   # pixels: point, ~1×beam, ~3×beam, ~9×beam
    "nterms":      1,
    "gridder":     "standard",
    "weighting":   "briggs",
    "pbcor":       False,
    "niter":       10000,
    "gain":        0.1,
    "cyclefactor": 2.0,
    "threshold":   "0.0mJy",         # masking drives stopping, not threshold
    "usemask":     "auto-multithresh",
    "interactive": False,
    "savemodel":   "none",
}

# Default Briggs robust weighting (overridable per job)
DEFAULT_ROBUST: float = 0.5

# ---------------------------------------------------------------------------
# Image geometry parameters
# ---------------------------------------------------------------------------

# Used by imaging_utils.compute_imaging_params()
PIXELS_PER_BEAM:      int   = 5      # pixels across the synthesised beam minor axis
PB_COVERAGE_FACTOR:   float = 1.5   # image extent as a multiple of the PB FWHM diameter
MINOR_AXIS_FRACTION:  float = 0.75  # beam minor/major axis ratio assumed for ALMA


# ---------------------------------------------------------------------------
# Automask selector
# ---------------------------------------------------------------------------

def select_automask(b75_m: float) -> dict[str, float]:
    """Return the auto-multithresh parameter dict appropriate for a given B75.

    Parameters
    ----------
    b75_m:
        75th-percentile baseline length in metres, computed from the antenna
        positions of all input MSes.

    Returns
    -------
    dict
        One of AUTOMASK_COMPACT, AUTOMASK_INTERMEDIATE, or AUTOMASK_EXTENDED.
    """
    if b75_m < B75_COMPACT_MAX:
        return AUTOMASK_COMPACT
    if b75_m < B75_EXTENDED_MIN:
        return AUTOMASK_INTERMEDIATE
    return AUTOMASK_EXTENDED
