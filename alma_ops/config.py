# config.py
from pathlib import Path

# Root directory of the project (ALMA-SAILS/)
# Resolves to VM mount path when imported from VM
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Central database location
DB_PATH = PROJECT_ROOT / "db" / "serpens_main.db"

# Related directories
DATASETS_DIR = PROJECT_ROOT / "datasets"
SRDP_WEBLOG_DIR = PROJECT_ROOT / "SRDP_weblogs"
CONFIG_DIR = PROJECT_ROOT / "config"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# =============================================================================
# Path Conversion Utilities
# =============================================================================

# What the VM sees vs what the platform sees
VM_MOUNT_PREFIX = "/home/ubuntu/canfar_arc/projects/ALMA-SAILS"  # Adjust to your actual mount
PLATFORM_PREFIX = "/arc/projects/ALMA-SAILS"


def to_platform_path(path: Path | str) -> str:
    """
    Convert VM mount path to platform native path for headless sessions.
    
    Example:
        /mnt/pspace/datasets/uid_123 → /arc/projects/ALMA-SAILS/datasets/uid_123
    """
    path_str = str(path)
    return path_str.replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX)

def to_vm_path(path: str) -> Path:
    """
    Convert platform native path to VM mount path.
    
    Example:
        /arc/projects/ALMA-SAILS/datasets/uid_123 → /mnt/pspace/datasets/uid_123
    """
    return Path(path.replace(PLATFORM_PREFIX, VM_MOUNT_PREFIX))

# CASA Image to use for processing
CASA_IMAGE = "images.canfar.net/casa-6/casa:6.6.4-34"
CASA_IMAGE_PIPE = "images.canfar.net/casa-6/casa:6.5.4-9-pipeline"
WGET2_IMAGE = "images.canfar.net/skaha/astroml:25.09"

# Auto Self-Calibration files
AUTO_SELFCAL_DIR = PROJECT_ROOT / "auto_selfcal-1.3.1"

if __name__ == "__main__":
    print(f"PROJECT ROOT SET AS: {PROJECT_ROOT}")
    print(f"DB PATH SET AS: {DB_PATH}")
    print(f"DATASETS DIR SET AS: {DATASETS_DIR}")
    print(f"SDRP WEBLOG DIR SET AS: {SRDP_WEBLOG_DIR}")
    print(f"CONFIG DIR SET AS: {CONFIG_DIR}")
    print(f"SCRIPTS DIR SET AS: {SCRIPTS_DIR}")
    print("\n=== Path Conversion ===")
    print(f"VM path: {DATASETS_DIR}")
    print(f"Platform path: {to_platform_path(DATASETS_DIR)}")
