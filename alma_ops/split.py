"""
split.py
--------
Work unit for the MOUS split step.

Must run inside a CASA environment (casa -c ...) with casatasks available.
Has no DB access.

The full step encompasses:
  0. resolve each MS's own science SPWs and verify the MSes agree
  1. listobs on original calibrated MSes (skip if .listobs.txt already present)
  2. split() each calibrated MS to target-only science SPWs
  3. listobs on each output split MS
  4. delete original calibrated MSes (unless keep_originals)
  5. return SplitResult with output paths and SPW re-indexing map

The SPW map records how original SPW numbers (in the calibrated MS) translate
to new 0-based indices in the split output. This mapping is stored in the
run_log payload by the calling script and needed when configuring autoselfcal.

SPW NUMBERS ARE PER-MS, NOT PER-MOUS
  Execution blocks within one MOUS do not have to number their spectral windows
  the same way. An EB carrying an extra early window (a WVR entry, say) shifts
  every later SPW by one, so the same physical window is spw 16 in twelve EBs
  and spw 17 in the thirteenth -- this is real, see uid://A001/X2d20/X335f.

  Passing one SPW list to every MS therefore selects the wrong windows in the
  odd EB, and because the neighbouring numbers are valid CH_AVG windows it does
  not raise: it silently writes a single channel where 512 were wanted. So each
  MS's science SPWs are read from the MS itself (OBSERVE_TARGET#ON_SOURCE,
  discarding the 1-channel CH_AVG/SQLD and 4-channel WVR windows).

  The database cannot stand in for this. alminer_metadata holds exactly one
  asdm_uid per MOUS, and for X335f the SPW numbers recorded against that ASDM
  are not even the ones its own measurement set uses.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from casatasks import listobs, split
from alma_ops.models import SplitResult

# A window narrower than this many channels is not science data: ALMA writes
# 1-channel CH_AVG and SQLD windows and a 4-channel WVR window, and the WVR one
# carries the target intent, so intent alone does not separate them.
MIN_SCIENCE_CHANNELS = 4

# Two EBs are observing the same physical window when the correlator was set up
# the same way -- same channel count, same width -- and the centres land close
# enough that they cannot be a different window.
#
# Centres are NOT expected to be equal. ALMA Doppler-tracks the source, so the
# sky frequency of a window shifts between EBs observed months apart: X1465/X15b
# sits ~20 MHz apart at 220 GHz, about 27 km/s of Earth's orbital motion. That is
# a third of a narrow 59 MHz window, so the tolerance cannot scale off the window
# width alone. It scales off the observing frequency instead, with the width as a
# floor for broad windows. Distinct windows in these datasets are separated by
# ~800 MHz or more, well clear of either.
WIDTH_MATCH_FRACTION = 0.01     # correlator setting, expected to match closely
FREQ_MATCH_FRACTION  = 0.5      # of the narrower window, for broad windows
DOPPLER_MATCH_FRACTION = 3e-4   # of the sky frequency, about 90 km/s


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


def _science_spw_setup(ms_path: Path) -> list[tuple]:
    """This MS's own science windows: [(spw, nchan, centre_hz, width_hz, name)].

    Sorted by SPW number, which is the order split() re-indexes them into.
    """
    import numpy as np
    from casatools import msmetadata as msmd_tool

    msmd = msmd_tool()
    msmd.open(str(ms_path))
    try:
        setup = []
        for spw in sorted(int(x) for x in msmd.spwsforintent("OBSERVE_TARGET#ON_SOURCE")):
            nchan = int(msmd.nchan(spw))
            try:
                name = (msmd.namesforspws(spw) or [""])[0] or ""
            except Exception:
                name = ""
            if nchan <= MIN_SCIENCE_CHANNELS or "WVR" in name.upper():
                continue
            width = float(np.abs(msmd.chanwidths(spw)).sum())
            setup.append((spw, nchan, float(msmd.meanfreq(spw)), width, name))
    finally:
        msmd.done()
    return setup


def _setup_mismatch(a: list[tuple], b: list[tuple]) -> str | None:
    """Why two MSes are not observing the same thing, or None if they are.

    Compares the physical setup -- how many windows, how wide, centred where --
    and deliberately not the SPW numbers, which are allowed to differ.
    """
    if len(a) != len(b):
        return f"{len(a)} science SPWs against {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
        if x[1] != y[1]:
            return f"window {i}: {x[1]} channels against {y[1]}"
        if abs(x[3] - y[3]) > WIDTH_MATCH_FRACTION * min(x[3], y[3]):
            return (f"window {i}: {x[3]/1e6:.1f} MHz wide against "
                    f"{y[3]/1e6:.1f} MHz")
        tol = max(FREQ_MATCH_FRACTION * min(x[3], y[3]),
                  DOPPLER_MATCH_FRACTION * min(x[2], y[2]))
        if abs(x[2] - y[2]) > tol:
            return (f"window {i}: centred {x[2]/1e9:.4f} GHz against "
                    f"{y[2]/1e9:.4f} GHz, further apart than {tol/1e6:.1f} MHz")
    return None


def check_spw_consistency(ms_paths: list[Path],
                          expected_spws: list[int] | None = None) -> dict:
    """Resolve each MS's science SPWs and confirm the MSes agree.

    Differing SPW *numbers* between EBs are fine and handled -- each MS is split
    on its own numbers, and because the windows are sorted the outputs still line
    up at the same 0-based indices. A differing physical *setup* is not fine and
    is reported as a problem, because the split outputs could not then be
    combined.

    Parameters
    ----------
    ms_paths:
        Calibrated MSes about to be split.
    expected_spws:
        What the database believes the science SPWs are. Used only to report a
        disagreement -- the MS wins, see the module docstring.

    Returns
    -------
    dict with
        per_ms      {Path: [spw, ...]}  what to pass to split() for that MS
        setups      {Path: [(spw, nchan, centre, width, name), ...]}
        problems    [str, ...]  non-empty means do not split
        notes       [str, ...]  worth printing, not worth stopping for
        renumbered  True if the MSes do not all use the same SPW numbers
    """
    out = {"per_ms": {}, "setups": {}, "problems": [], "notes": [], "renumbered": False}
    for ms_path in ms_paths:
        try:
            setup = _science_spw_setup(ms_path)
        except Exception as e:
            out["problems"].append(f"{ms_path.name}: could not read SPW metadata ({e})")
            continue
        if not setup:
            out["problems"].append(
                f"{ms_path.name}: no science SPWs found "
                f"(no window with OBSERVE_TARGET intent and >{MIN_SCIENCE_CHANNELS} channels)")
            continue
        out["setups"][ms_path] = setup
        out["per_ms"][ms_path] = [w[0] for w in setup]

    if not out["setups"] or out["problems"]:
        return out

    # Compare every MS against the first, which is only a reference, not a truth
    ref_path = next(iter(out["setups"]))
    ref = out["setups"][ref_path]
    for ms_path, setup in out["setups"].items():
        if ms_path is ref_path:
            continue
        why = _setup_mismatch(ref, setup)
        if why:
            out["problems"].append(
                f"{ms_path.name} is not observing the same setup as "
                f"{ref_path.name}: {why}")

    numbering = {tuple(v) for v in out["per_ms"].values()}
    if len(numbering) > 1:
        out["renumbered"] = True
        out["notes"].append(
            "SPW numbers differ between execution blocks; each MS will be split "
            "on its own numbers. The windows themselves match, so the outputs "
            "still line up at the same indices.")

    if expected_spws:
        exp = sorted(expected_spws)
        disagree = {ms.name: v for ms, v in out["per_ms"].items() if v != exp}
        if disagree:
            out["notes"].append(
                f"the database records SPWs {exp}, which is not what "
                + "; ".join(f"{k} uses ({v})" for k, v in disagree.items())
                + ". Using each MS's own numbers.")
    return out


def format_spw_report(plan: dict) -> str:
    """Human-readable rendering of check_spw_consistency, for logs and aborts."""
    lines = ["[split] science SPWs resolved from each measurement set:"]
    for ms_path, setup in plan["setups"].items():
        spws = ",".join(str(w[0]) for w in setup)
        chans = ",".join(str(w[1]) for w in setup)
        lines.append(f"[split]   {ms_path.name}")
        lines.append(f"[split]       spw   {spws}")
        lines.append(f"[split]       nchan {chans}")
    for note in plan["notes"]:
        lines.append(f"[split] note: {note}")
    for prob in plan["problems"]:
        lines.append(f"[split] PROBLEM: {prob}")
    return "\n".join(lines)


def run_split(
    mous_id: str,
    mous_directory: Path,
    spws: list[int],
    field_names: list[str],
    datacolumn: str | None = None,
    keep_originals: bool = False,
    resolve_spws: bool = True,
) -> SplitResult:
    """Split calibrated MSes, run listobs, and delete originals.

    Parameters
    ----------
    mous_id:
        MOUS ID (used for labelling in SplitResult).
    mous_directory:
        Root MOUS directory containing calibrated .ms files directly.
    spws:
        Science SPW numbers the database believes this MOUS uses. With
        resolve_spws (the default) these are a cross-check only and each MS is
        split on the SPW numbers it declares itself; with resolve_spws=False
        they are used verbatim for every MS. CASA re-indexes from 0 in the
        output either way.
    field_names:
        Science target field names to select. Passed as the CASA split()
        field parameter so only the relevant targets are kept, excluding any
        additional science targets present in large survey-type datasets.
    datacolumn:
        Data column to read. If None (default), auto-detected per MS:
        'corrected' if CORRECTED_DATA exists (standard for SRDP pipeline
        products), otherwise 'data'.
    resolve_spws:
        If True (default), read each MS's own science SPWs and refuse to split
        when the MSes are not observing the same setup. Pass False only to force
        an explicit SPW list through, e.g. manual_split.py --spw.
    keep_originals:
        If True, leave the calibrated MSes in place instead of deleting them
        once the split succeeds. They are normally deleted because they are
        large and reproducible from the archive -- but when they are the only
        surviving copy, or when the split output has yet to be verified,
        deleting them forecloses a second attempt. Costs disk, buys a redo.

    Returns
    -------
    SplitResult
        Output paths, SPW re-indexing map, and timing. The spw_map field is
        {original_spw_int: new_index_int} ordered by the sorted input SPW list;
        where EBs number their SPWs differently it holds the first MS's map, and
        spw_map_by_ms holds one map per MS.
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

    # ----------------------------------------------------------------------
    # 0. Resolve each MS's science SPWs and refuse to proceed on a mismatch
    # ----------------------------------------------------------------------
    if resolve_spws:
        plan = check_spw_consistency(calibrated, expected_spws=spws)
        print(format_spw_report(plan))
        if plan["problems"]:
            return SplitResult(
                mous_id=mous_id,
                success=False,
                split_products=[],
                source_names=field_names,
                duration_s=time.monotonic() - start,
                error="science SPWs are inconsistent across the measurement sets, "
                      "nothing was split: " + "; ".join(plan["problems"]),
            )
        spws_by_ms = plan["per_ms"]
    else:
        print(f"[split] SPW resolution disabled, using {sorted(spws)} for every MS")
        spws_by_ms = {ms_path: sorted(spws) for ms_path in calibrated}

    splits_dir = mous_directory / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)

    field_str = ",".join(field_names)
    spw_map_by_ms = {ms_path: {orig: idx for idx, orig in enumerate(sorted(sp))}
                     for ms_path, sp in spws_by_ms.items()}
    spw_map   = spw_map_by_ms[calibrated[0]]
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
            spw_str = ",".join(map(str, sorted(spws_by_ms[ms_path])))
            print(f"[split] {ms_path.name} — datacolumn='{col}', spw='{spw_str}', "
                  f"fields='{field_str}'")
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
        if keep_originals:
            print(f"[split] keeping {len(calibrated)} original calibrated MS(es) "
                  f"in {mous_directory}")
        else:
            for ms_path in calibrated:
                shutil.rmtree(ms_path)

    except Exception as e:
        return SplitResult(
            mous_id=mous_id,
            success=False,
            split_products=split_products,
            source_names=field_names,
            spw_map=spw_map,
            spw_map_by_ms={k.name: v for k, v in spw_map_by_ms.items()},
            duration_s=time.monotonic() - start,
            error=str(e),
        )

    return SplitResult(
        mous_id=mous_id,
        success=True,
        split_products=split_products,
        source_names=field_names,
        spw_map=spw_map,
        spw_map_by_ms={k.name: v for k, v in spw_map_by_ms.items()},
        duration_s=time.monotonic() - start,
    )
