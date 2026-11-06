"""
config.py
---------
Central configuration for the ALMA-SAILS pipeline.
"""

from pathlib import Path

# Project root resolves correctly on both the VM (via sshfs mount) and CANFAR platform
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Database
DB_PATH = PROJECT_ROOT / "db" / "serpens_v2.db"

# Key project directories
DATASETS_DIR           = PROJECT_ROOT / "datasets"
CONFIG_DIR             = PROJECT_ROOT / "config"
CONTINUUM_IMAGING_DIR  = PROJECT_ROOT / "imaging" / "continuum"
LINE_IMAGING_DIR       = PROJECT_ROOT / "imaging" / "lines"
LINE_TRIAGE_DIR        = LINE_IMAGING_DIR / "triage"
LINE_FINAL_DIR         = LINE_IMAGING_DIR / "final"
SCOUSE_DIR             = LINE_IMAGING_DIR / "scouse"
SCOUSE_SUMMARY_DIR     = SCOUSE_DIR / "summary_pdfs"   # shared copy of every unit's summary PDF
SRDP_WEBLOG_DIR        = PROJECT_ROOT / "SRDP_weblogs"

# Preset CASA rc files, for passing to a headless session as CASARCFILES
# (a flow's casarc= or a launcher's --casarc). Nothing applies them
# automatically. lowmem caps tclean at 24 GB for flexible sessions; highmem
# at 150 GB for a session pinned at ram_gb=192. Why a cap is needed and what
# an rc file must contain: docs/reference/casa_rc.md.
CODEBASE_ROOT  = Path(__file__).resolve().parents[1]
CASARC_LOWMEM  = CODEBASE_ROOT / "casarc" / "casa_lowmem.rc"
CASARC_HIGHMEM = CODEBASE_ROOT / "casarc" / "casa_highmem.rc"

# The complete database schema; scripts/init_database.py builds and checks against it.
SCHEMA_PATH = CODEBASE_ROOT / "schema" / "schema.sql"

# Container images
CASA_IMAGE    = "images.canfar.net/casa-6/casa:6.5.4-9-pipeline"
ASTROML_IMAGE = "images.canfar.net/skaha/astroml:26.03"

# Auto self-calibration entry script
AUTO_SELFCAL_SCRIPT = PROJECT_ROOT / "auto_selfcal" / "bin" / "auto_selfcal.py"

# ADMIT (ALMA Data Mining Toolkit) checkout used for line identification on
# triage cubes. Not pip-installed -- imported by inserting this path at the
# front of sys.path. See admit_local_patches.diff at the project root for
# the local Python-3-compatibility fixes this checkout needs (not upstream).
ADMIT_ROOT = PROJECT_ROOT / "admit"

# Distance used for physical scales in summary figures (au scale bars, km/s/pc
# gradients). Serpens Main / W40, Ortiz-Leon et al. 2017.
SURVEY_DISTANCE_PC = 436.0
