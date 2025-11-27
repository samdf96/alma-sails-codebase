from pathlib import Path

# Root directory of the project (ALMA-SAILS/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Central database location
DB_PATH = PROJECT_ROOT / "db" / "serpens_main.db"

# Related directories
DATASETS_DIR = PROJECT_ROOT / "datasets"
SRDP_WEBLOG_DIR = PROJECT_ROOT / "SRDP_weblogs"
CONFIG_DIR = PROJECT_ROOT / "config"

# CASA Image to use for processing
CASA_IMAGE = "images.canfar.net/casa-6/casa:6.6.4-34"
CASA_IMAGE_PIPE = "images.canfar.net/casa-6/casa:6.5.4-9-pipeline"

# Auto Self-Calibration files
AUTO_SELFCAL_DIR = PROJECT_ROOT / "auto_selfcal-1.3.1"

if __name__ == "__main__":

    print(f"PROJECT ROOT SET AS: {PROJECT_ROOT}")
    print(f"DB PATH SET AS: {DB_PATH}")
    print(f"DATASETS DIR SET AS: {DATASETS_DIR}")
    print(f"SDRP WEBLOG DIR SET AS: {SRDP_WEBLOG_DIR}")
    print(f"CONFIG DIR SET AS: {CONFIG_DIR}")
