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
SRDP_WEBLOG_DIR        = PROJECT_ROOT / "SRDP_weblogs"

# Container images
CASA_IMAGE    = "images.canfar.net/casa-6/casa:6.5.4-9-pipeline"
ASTROML_IMAGE = "images.canfar.net/skaha/astroml:26.03"

# Auto self-calibration entry script
AUTO_SELFCAL_SCRIPT = PROJECT_ROOT / "auto_selfcal" / "bin" / "auto_selfcal.py"

