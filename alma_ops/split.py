"""
split.py
--------
Work unit for the MOUS split step.

Must run inside a CASA environment (casa -c ...) with casatasks available.
Has no DB access.

The full step encompasses:
  1. listobs on original calibrated MSes (skip if .listobs.txt already present)
  2. split() each calibrated MS to target-only science SPWs
  3. listobs on each output split MS
  4. delete original calibrated MSes
  5. return SplitResult with output paths and SPW re-indexing map

The SPW map records how original SPW numbers (in the calibrated MS) translate
to new 0-based indices in the split output. This mapping is stored in the
run_log payload by the calling script and needed when configuring autoselfcal.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from casatasks import listobs, split
from alma_ops.models import SplitResult


def _find_calibrated_ms(mous_directory: Path) -> list[Path]:
    """Discover top-level calibrated .ms directories (not inside splits/)."""
    return sorted(
        p for p in mous_directory.iterdir()
        if p.is_dir() and p.name.endswith(".ms")
    )


def _detect_datacolumn(ms_path: Path) -> str:
    """Return 'corrected' if CORRECTED_DATA exists in the MS, otherwise 'data'.

    SRDP pipeline products carry both DATA (raw) and CORRECTED_DATA (calibrated).
    Splitting from CORRECTED_DATA gives the science-ready visibilities.
    """
    from casatools import table as tb_tool
    tb = tb_tool()
    tb.open(str(ms_path))
    cols = tb.colnames()
    tb.close()
    return "corrected" if "CORRECTED_DATA" in cols else "data"


def _listobs(vis_path: Path, output_path: Path) -> None:
    """Run CASA listobs and write output to a text file."""
    listobs(vis=str(vis_path), listfile=str(output_path), verbose=True, overwrite=True)


def run_split(
    mous_id: str,
    mous_directory: Path,
    spws: list[int],
    field_names: list[str],
    datacolumn: str | None = None,
) -> SplitResult:
    """Split calibrated MSes, run listobs, and delete originals.

    Parameters
    ----------
    mous_id:
        MOUS ID (used for labelling in SplitResult).
    mous_directory:
        Root MOUS directory containing calibrated .ms files directly.
    spws:
        Science SPW numbers (as they appear in the calibrated MS) to retain.
        CASA will re-index these from 0 in the output.
    field_names:
        Science target field names to select. Passed as the CASA split()
        field parameter so only the relevant targets are kept, excluding any
        additional science targets present in large survey-type datasets.
    datacolumn:
        Data column to read. If None (default), auto-detected per MS:
        'corrected' if CORRECTED_DATA exists (standard for SRDP pipeline
        products), otherwise 'data'.

    Returns
    -------
    SplitResult
        Output paths, SPW re-indexing map, and timing. The spw_map field is
        {original_spw_int: new_index_int} ordered by the sorted input SPW list.
    """
    start = time.monotonic()
    calibrated = _find_calibrated_ms(mous_directory)

    if not calibrated:
        return SplitResult(
            mous_id=mous_id,
            success=False,
            split_products=[],
            source_names=field_names,
            duration_s=time.monotonic() - start,
            error=f"no calibrated .ms files found in {mous_directory}",
        )

    splits_dir = mous_directory / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)

    field_str = ",".join(field_names)
    spw_str   = ",".join(map(str, sorted(spws)))
    spw_map   = {orig: idx for idx, orig in enumerate(sorted(spws))}
    split_products = []

    try:
        # ------------------------------------------------------------------
        # 1. listobs on originals (skip if already done)
        # ------------------------------------------------------------------
        for ms_path in calibrated:
            txt = mous_directory / f"{ms_path.name}.listobs.txt"
            if not txt.exists():
                _listobs(ms_path, txt)

        # ------------------------------------------------------------------
        # 2. Split each calibrated MS
        # ------------------------------------------------------------------
        for ms_path in calibrated:
            stem = ms_path.stem
            out_ms = splits_dir / f"{stem}_targets.ms"
            col = datacolumn or _detect_datacolumn(ms_path)
            print(f"[split] {ms_path.name} — datacolumn='{col}', fields='{field_str}'")
            split(
                vis=str(ms_path),
                outputvis=str(out_ms),
                field=field_str,
                intent="*OBSERVE_TARGET*",
                spw=spw_str,
                datacolumn=col,
            )
            split_products.append(out_ms)

        # ------------------------------------------------------------------
        # 3. listobs on split outputs
        # ------------------------------------------------------------------
        for out_ms in split_products:
            txt = splits_dir / f"{out_ms.name}.listobs.txt"
            _listobs(out_ms, txt)

        # ------------------------------------------------------------------
        # 4. Delete original calibrated MSes
        # ------------------------------------------------------------------
        for ms_path in calibrated:
            shutil.rmtree(ms_path)

    except Exception as e:
        return SplitResult(
            mous_id=mous_id,
            success=False,
            split_products=split_products,
            source_names=field_names,
            spw_map=spw_map,
            duration_s=time.monotonic() - start,
            error=str(e),
        )

    return SplitResult(
        mous_id=mous_id,
        success=True,
        split_products=split_products,
        source_names=field_names,
        spw_map=spw_map,
        duration_s=time.monotonic() - start,
    )
