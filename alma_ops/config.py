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
IMAGING_ROOT_DIR = PROJECT_ROOT / "reduction" / "imaging"

# =============================================================================
# Path Conversion Utilities
# =============================================================================

# What the VM sees vs what the platform sees
VM_MOUNT_PREFIX = "/home/ubuntu/canfar_arc/projects/ALMA-SAILS"
PLATFORM_PREFIX = "/arc/projects/ALMA-SAILS"


class DualPath:
    """A path that knows both its VM mount location and its CANFAR platform location.

    The Prefect VM mounts CANFAR storage via sshfs, so the same data has two path
    representations:
      - VM path:       /home/ubuntu/canfar_arc/projects/ALMA-SAILS/...
      - Platform path: /arc/projects/ALMA-SAILS/...

    DualPath auto-detects which prefix a given path uses and exposes both. Use
    ``.vm`` for local file operations on the Prefect server, and ``.platform``
    when passing paths to CANFAR headless sessions or storing in the database.

    The ``/`` operator creates a child DualPath, mirroring pathlib behaviour.

    Parameters
    ----------
    path : Path | str
        Either a VM mount path or a CANFAR platform path.

    Raises
    ------
    ValueError
        If the path does not start with either the VM or platform prefix.

    Examples
    --------
    >>> splits_dir = DualPath(DATASETS_DIR / "uid___A001_X1" / "splits")
    >>> splits_dir.vm.mkdir(parents=True, exist_ok=True)  # local operation
    >>> splits_dir.platform  # pass to headless session or store in DB
    '/arc/projects/ALMA-SAILS/datasets/uid___A001_X1/splits'
    """

    def __init__(self, path: Path | str):
        path_str = str(path)
        if path_str.startswith(VM_MOUNT_PREFIX):
            self._vm = Path(path_str)
            self._platform = path_str.replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX)
        elif path_str.startswith(PLATFORM_PREFIX):
            self._platform = path_str
            self._vm = Path(path_str.replace(PLATFORM_PREFIX, VM_MOUNT_PREFIX))
        else:
            raise ValueError(
                f"Path '{path_str}' does not begin with either the VM prefix "
                f"('{VM_MOUNT_PREFIX}') or the platform prefix ('{PLATFORM_PREFIX}')."
            )

    @property
    def vm(self) -> Path:
        """VM mount path — use for local file operations on the Prefect server."""
        return self._vm

    @property
    def platform(self) -> str:
        """CANFAR platform path — use for headless session arguments and DB storage."""
        return self._platform

    def __truediv__(self, other: str | Path) -> "DualPath":
        """Support path joining with ``/``, returning a new DualPath."""
        return DualPath(self._vm / other)

    def __str__(self) -> str:
        """Default string representation uses the VM path."""
        return str(self._vm)

    def __fspath__(self) -> str:
        """Support ``os.fspath()`` and pathlib operations using the VM path."""
        return str(self._vm)

    def __repr__(self) -> str:
        return f"DualPath(vm='{self._vm}', platform='{self._platform}')"


def to_platform_path(path: Path | str | list[Path | str]) -> str | list[str]:
    """Convert VM mount path(s) to platform native path(s).

    .. deprecated::
        Prefer constructing a :class:`DualPath` and accessing ``.platform``.

    Parameters
    ----------
    path : Path | str | list[Path  |  str]
        Single path or list of paths to convert.

    Returns
    -------
    str | list[str]
        Platform native path(s) corresponding to input.
    """
    if isinstance(path, list):
        return [str(p).replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX) for p in path]
    else:
        return str(path).replace(VM_MOUNT_PREFIX, PLATFORM_PREFIX)


def to_vm_path(path: str | list[str]) -> Path | list[Path]:
    """Convert platform native path(s) to VM mount path(s).

    .. deprecated::
        Prefer constructing a :class:`DualPath` and accessing ``.vm``.

    Parameters
    ----------
    path : str | list[str]
        Platform native path(s) to convert.

    Returns
    -------
    Path | list[Path]
        VM mount path(s) corresponding to input.
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
