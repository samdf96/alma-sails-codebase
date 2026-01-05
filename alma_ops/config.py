"""
config.py
--------------------
Central configuration for ALMA-SAILS project.
"""

# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------

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
VM_MOUNT_PREFIX = "/home/ubuntu/canfar_arc/projects/ALMA-SAILS"
PLATFORM_PREFIX = "/arc/projects/ALMA-SAILS"


def to_platform_path(path: Path | str | list[Path | str]) -> str | list[str]:
    """Convert VM mount path(s) to platform native path(s).

    Parameters
    ----------
    path : Path | str | list[Path  |  str]
        Single path or list of paths to convert.

    Returns
    -------
    str | list[str]
        Platform native path(s) corresponding to input.

    Example
    -------
        Single: /mnt/pspace/datasets/uid_123 → /arc/projects/ALMA-SAILS/datasets/uid_123
        List: ["/mnt/pspace/a", "/mnt/pspace/b"] → ["/arc/projects/ALMA-SAILS/a", ...]
    """
    if isinstance(path, list):
        return [str(p).replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX) for p in path]
    else:
        return str(path).replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX)


def to_vm_path(path: str | list[str]) -> Path | list[Path]:
    """Convert platform native path(s) to VM mount path(s).

    Parameters
    ----------
    path : str | list[str]
        Platform native path(s) to convert.

    Returns
    -------
    Path | list[Path]
        VM mount path(s) corresponding to input.

    Example
    -------
        Single: /arc/projects/ALMA-SAILS/datasets/uid_123 → /mnt/pspace/datasets/uid_123
        List: ["/arc/.../a", "/arc/.../b"] → [Path("/mnt/pspace/a"), ...]
    """
    if isinstance(path, list):
        return [Path(p.replace(PLATFORM_PREFIX, VM_MOUNT_PREFIX)) for p in path]
    else:
        return Path(path.replace(PLATFORM_PREFIX, VM_MOUNT_PREFIX))


# CASA Image to use for processing
CASA_IMAGE = "images.canfar.net/casa-6/casa:6.6.4-34"
CASA_IMAGE_PIPE = "images.canfar.net/casa-6/casa:6.5.4-9-pipeline"
WGET2_IMAGE = "images.canfar.net/skaha/astroml:25.10"

# Auto Self-Calibration files
AUTO_SELFCAL_ENTRY_SCRIPT = PROJECT_ROOT / "auto_selfcal" / "bin" / "auto_selfcal.py"

# =====================================================================
# CLI Entry
# =====================================================================

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
