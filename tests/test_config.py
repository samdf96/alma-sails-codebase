"""
test_config.py
--------------
Unit tests for DualPath in alma_ops/config.py.
"""

from pathlib import Path

import pytest

from alma_ops.config import PLATFORM_PREFIX, VM_MOUNT_PREFIX, DualPath


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VM_PATH = f"{VM_MOUNT_PREFIX}/datasets/uid___A001_X1234_Xtest"
PLATFORM_PATH = f"{PLATFORM_PREFIX}/datasets/uid___A001_X1234_Xtest"


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_from_vm_prefix_gives_correct_platform():
    p = DualPath(VM_PATH)
    assert p.platform == PLATFORM_PATH


def test_from_vm_prefix_gives_correct_vm():
    p = DualPath(VM_PATH)
    assert p.vm == Path(VM_PATH)


def test_from_platform_prefix_gives_correct_vm():
    p = DualPath(PLATFORM_PATH)
    assert p.vm == Path(VM_PATH)


def test_from_platform_prefix_gives_correct_platform():
    p = DualPath(PLATFORM_PATH)
    assert p.platform == PLATFORM_PATH


def test_accepts_pathlib_path_object():
    p = DualPath(Path(VM_PATH))
    assert p.platform == PLATFORM_PATH


def test_rejects_unrecognised_prefix():
    with pytest.raises(ValueError, match="does not begin with either"):
        DualPath("/tmp/random/path")


def test_rejects_relative_path():
    with pytest.raises(ValueError):
        DualPath("datasets/uid___A001_X1234_Xtest")


# ---------------------------------------------------------------------------
# Round-trip consistency
# ---------------------------------------------------------------------------


def test_vm_to_platform_to_vm_roundtrip():
    p = DualPath(VM_PATH)
    # re-wrapping the platform path should give back the same VM path
    assert DualPath(p.platform).vm == p.vm


def test_platform_to_vm_to_platform_roundtrip():
    p = DualPath(PLATFORM_PATH)
    assert DualPath(p.vm).platform == p.platform


# ---------------------------------------------------------------------------
# / operator
# ---------------------------------------------------------------------------


def test_slash_operator_returns_dualpath():
    p = DualPath(f"{VM_MOUNT_PREFIX}/datasets") / "uid___A001_X1234_Xtest"
    assert isinstance(p, DualPath)


def test_slash_operator_appends_segment():
    base = DualPath(f"{VM_MOUNT_PREFIX}/datasets")
    child = base / "uid___A001_X1234_Xtest"
    assert child.vm.name == "uid___A001_X1234_Xtest"


def test_slash_operator_platform_path_is_correct():
    base = DualPath(f"{VM_MOUNT_PREFIX}/datasets")
    child = base / "uid___A001_X1234_Xtest"
    assert child.platform == f"{PLATFORM_PREFIX}/datasets/uid___A001_X1234_Xtest"


def test_slash_operator_chaining():
    p = DualPath(f"{VM_MOUNT_PREFIX}/datasets") / "uid___A001_X1234_Xtest" / "splits"
    assert p.vm.name == "splits"
    assert p.platform.endswith("/splits")


# ---------------------------------------------------------------------------
# String / fspath protocol
# ---------------------------------------------------------------------------


def test_str_returns_vm_path():
    p = DualPath(VM_PATH)
    assert str(p) == VM_PATH


def test_fspath_returns_vm_path():
    import os
    p = DualPath(VM_PATH)
    assert os.fspath(p) == VM_PATH


def test_repr_contains_both_paths():
    p = DualPath(VM_PATH)
    r = repr(p)
    assert "vm=" in r
    assert "platform=" in r