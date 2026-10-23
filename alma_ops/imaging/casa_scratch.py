"""
casa_scratch.py
---------------
Per-run working directory for tclean, shared by continuum imaging, line triage
and deep line imaging.

CASA writes its scratch (TempLattice<n>_<pid>, IMAGING_WEIGHT_*) to the current
working directory, not to imagename's directory. Every CANFAR headless container
starts CASA at nearly the same pid, so two runs using one directory as cwd
generate the *same* scratch names and delete each other's files mid-gridding:
"RegularFile: .../TempLattice7_201/table.dat_tmp does not exist and cannot be
created" (line-imaging dets 2989, 3160, 3286; several line-triage runs). Running
each tclean from <img_base>.scratch, unique per product, keeps them apart.

No CASA import, so the reset scripts can use SCRATCH_SUFFIX from a plain VM shell.
"""

from __future__ import annotations

import contextlib
import os
import shutil

SCRATCH_SUFFIX = ".scratch"


@contextlib.contextmanager
def scratch_cwd(img_base: str):
    """Run the enclosed block with cwd = <img_base>.scratch.

    The directory is removed when the block succeeds and kept when it raises, so
    a failed run's leftovers sit beside its partial products for the reset
    scripts to delete. A hard kill (OOM) never reaches either branch, which
    leaves it in place the same way.
    """
    scratch_dir = img_base + SCRATCH_SUFFIX
    os.makedirs(scratch_dir, exist_ok=True)
    orig_dir = os.getcwd()
    os.chdir(scratch_dir)
    try:
        yield scratch_dir
    finally:
        os.chdir(orig_dir)
    shutil.rmtree(scratch_dir, ignore_errors=True)
