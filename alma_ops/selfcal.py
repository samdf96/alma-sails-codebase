"""
selfcal.py
----------
Post-autoselfcal work unit. Runs inside a CASA environment.

After mpicasa completes autoselfcal, this module:
  1. Finds *_targets.ms output in the auto_selfcal/ working directory
  2. For each MS, extracts CORRECTED_DATA → *_targets.corrected.ms via CASA split()
     - Validates extraction with MD5 comparison before deleting the original
     - If no CORRECTED_DATA column (selfcal did not help), uses the MS as-is
  3. Collects *_targets.contsub.ms as the continuum-subtracted products
  4. Returns a SelfcalResult with the final product paths

The calling script (manual_selfcal.sh) is responsible for:
  - Running mpicasa before calling this module
  - Writing the result to the database (record_transition / record_failure)
"""

from __future__ import annotations

import hashlib
import shutil
import time
from pathlib import Path

from casatasks import split
from alma_ops.models import SelfcalResult

def _has_column(ms_path: Path, column: str) -> bool:
    from casatools import table as tb_tool
    tb = tb_tool()
    tb.open(str(ms_path))
    result = column in tb.colnames()
    tb.close()
    return result


def _md5_column(ms_path: Path, column: str) -> str:
    """MD5 hash of a data column, normalised per DATA_DESC_ID.

    Rows within each DDID are sorted by (TIME, ANTENNA1, ANTENNA2) before
    hashing so physical row order on disk does not affect the result.  This
    is necessary because split() (via MSTransformManager) and multi-pass
    field-specific applycal can produce a different physical row order in
    the output MS than the source, even when the data values are identical.
    Peak RAM is bounded by the largest single DDID loaded at once.
    """
    import numpy as np
    from casatools import table as tb_tool

    tb = tb_tool()
    tb.open(str(ms_path))
    ddids = sorted(set(tb.getcol("DATA_DESC_ID").tolist()))
    h = hashlib.md5()
    for ddid in ddids:
        sub = tb.query(f"DATA_DESC_ID == {ddid}")
        if sub.nrows() == 0:
            sub.close()
            continue
        data = sub.getcol(column)
        order = np.lexsort((
            sub.getcol("ANTENNA2"),
            sub.getcol("ANTENNA1"),
            sub.getcol("TIME"),
        ))
        sub.close()
        h.update(data[..., order].tobytes())
    tb.close()
    return h.hexdigest()


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _fmt(b: int) -> str:
    for unit, thresh in [("TB", 1e12), ("GB", 1e9), ("MB", 1e6)]:
        if b >= thresh:
            return f"{b/thresh:.2f} {unit}"
    return f"{b/1e6:.2f} MB"


def run_post_selfcal(
    mous_id: str,
    mous_directory: Path,
    skip_integrity_checks: bool = False,
) -> SelfcalResult:
    """Extract selfcal products, validate, and clean up.

    Finds *_targets.ms files in auto_selfcal/, extracts CORRECTED_DATA into
    *_targets.corrected.ms, validates with MD5, and deletes originals if valid.
    Collects *_targets.contsub.ms as the subtracted products.

    Parameters
    ----------
    mous_id:
        MOUS ID (for labelling in SelfcalResult).
    mous_directory:
        Root MOUS directory (contains auto_selfcal/ as a subdirectory).

    Returns
    -------
    SelfcalResult
        success=True with product paths, or success=False with an error message.
    """
    start = time.monotonic()
    autoselfcal_dir = mous_directory / "auto_selfcal"

    if not autoselfcal_dir.exists():
        return SelfcalResult(
            mous_id=mous_id, success=False,
            products_nonsub=[], products_sub=[],
            duration_s=time.monotonic() - start,
            error=f"auto_selfcal directory not found: {autoselfcal_dir}",
        )

    source_mses = sorted(
        p for p in autoselfcal_dir.glob("*_targets.ms")
        if not p.name.endswith(".contsub.ms") and ".corrected" not in p.name
    )

    if not source_mses:
        return SelfcalResult(
            mous_id=mous_id, success=False,
            products_nonsub=[], products_sub=[],
            duration_s=time.monotonic() - start,
            error=f"no *_targets.ms found in {autoselfcal_dir}",
        )

    corrected = []
    failed = False
    integrity_skipped = False

    for src_ms in source_mses:
        out_ms = src_ms.with_name(
            src_ms.name.replace("_targets.ms", "_targets.corrected.ms")
        )
        print(f"[post-selfcal] {src_ms.name}  ({_fmt(_dir_size(src_ms))})")

        if not _has_column(src_ms, "CORRECTED_DATA"):
            print(f"[post-selfcal]   No CORRECTED_DATA — using original as product")
            corrected.append(src_ms)
            continue

        if out_ms.exists():
            print(f"[post-selfcal]   Already extracted: {out_ms.name}")
            corrected.append(out_ms)
            continue

        print(f"[post-selfcal]   Extracting CORRECTED_DATA → {out_ms.name}")
        split(
            vis=str(src_ms),
            outputvis=str(out_ms),
            datacolumn="corrected",
            keepflags=True,
        )

        if not out_ms.exists():
            print(f"[post-selfcal]   ERROR: split() did not produce {out_ms}")
            failed = True
            continue

        if skip_integrity_checks:
            print(f"[post-selfcal]   INTEGRITY CHECKS SKIPPED — deleting original without MD5 validation")
            shutil.rmtree(src_ms)
            corrected.append(out_ms)
            integrity_skipped = True
            continue

        print(f"[post-selfcal]   Validating MD5...")
        h_orig = _md5_column(src_ms, "CORRECTED_DATA")
        h_out  = _md5_column(out_ms, "DATA")
        print(f"[post-selfcal]   MD5 original : {h_orig}")
        print(f"[post-selfcal]   MD5 extracted: {h_out}")

        if h_orig != h_out:
            print(f"[post-selfcal]   ERROR: MD5 mismatch — keeping both, skipping deletion")
            failed = True
            corrected.append(out_ms)
            continue

        print(f"[post-selfcal]   MD5 match — deleting original ({_fmt(_dir_size(src_ms))})")
        shutil.rmtree(src_ms)
        corrected.append(out_ms)

    if failed:
        return SelfcalResult(
            mous_id=mous_id, success=False,
            products_nonsub=corrected,
            products_sub=[],
            duration_s=time.monotonic() - start,
            error="one or more CORRECTED_DATA extractions failed or had MD5 mismatch",
            integrity_checks_skipped=integrity_skipped,
        )

    sub = sorted(autoselfcal_dir.glob("*_targets.contsub.ms"))

    return SelfcalResult(
        mous_id=mous_id, success=True,
        products_nonsub=corrected,
        products_sub=sub,
        duration_s=time.monotonic() - start,
        integrity_checks_skipped=integrity_skipped,
    )
