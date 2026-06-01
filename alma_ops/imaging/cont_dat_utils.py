"""
cont_dat_utils.py
-----------------
Utilities for parsing ALMA cont.dat files and converting their line-free
frequency ranges into CASA SPW:channel selection strings for use in tclean.

The cont.dat format is produced by the ALMA pipeline and the auto_selfcal
script. It specifies, per field and per spectral window, the frequency ranges
(in LSRK) that are free of line emission — i.e. the channels to be used for
continuum imaging.

The SPW:channel selection approach (cont_dat_to_spw_string) is inspired by
phangsPipeline.casaVisRoutines.spw_string_for_freq_ranges from the PHANGS-ALMA
imaging pipeline v3.2 (Leroy et al. 2021, ApJS 255 19), rewritten here to use
CASA's built-in msmd tool and to operate directly on the per-SPW cont.dat
structure.

Usage (inside CASA):
    from alma_ops.imaging.cont_dat_utils import parse_cont_dat, cont_dat_to_spw_string

    ranges = parse_cont_dat('/path/to/cont.dat', field_name='EC_53')
    spw_str = cont_dat_to_spw_string('/path/to/ms', ranges)
    # spw_str -> "0:10~150;200~350,1:0~4095,2:30~120;400~500"
"""

from __future__ import annotations

import re
from pathlib import Path


# Sentinel value used when an entire SPW is line-free ("ALL" in cont.dat)
_ALL = "ALL"


def parse_cont_dat(
    cont_dat_path,
    field_name: str,
) -> dict:
    """Parse a cont.dat file and return line-free frequency ranges for a field.

    Parameters
    ----------
    cont_dat_path:
        Path to the cont.dat file.
    field_name:
        The field name to extract (must match a 'Field: <name>' entry).
        Matching is case-insensitive and strips leading/trailing whitespace.

    Returns
    -------
    dict
        Mapping of SPW index (int) to either:
          - list of (lo_ghz, hi_ghz) tuples  — the line-free frequency ranges
          - the sentinel string "ALL"          — the entire SPW is line-free

        Returns an empty dict if the field is not found in the file.

    Raises
    ------
    FileNotFoundError
        If cont_dat_path does not exist.
    """
    cont_dat_path = Path(cont_dat_path)
    if not cont_dat_path.exists():
        raise FileNotFoundError(f"cont.dat not found: {cont_dat_path}")

    text = cont_dat_path.read_text()

    # Split into per-field blocks (each starts with "Field: <name>")
    field_blocks = re.split(r"(?im)^Field:\s*", text)

    target_block = None
    for block in field_blocks[1:]:
        first_line, _, rest = block.partition("\n")
        if first_line.strip().lower() == field_name.strip().lower():
            target_block = rest
            break

    if target_block is None:
        return {}

    result = {}

    spw_blocks = re.split(r"(?im)^SpectralWindow:\s*", target_block)
    for spw_block in spw_blocks[1:]:
        lines = spw_block.strip().splitlines()
        if not lines:
            continue

        try:
            spw_idx = int(lines[0].strip())
        except ValueError:
            continue

        ranges = []
        all_flag = False

        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            if line.lower().startswith("field:") or line.lower().startswith("spectralwindow:"):
                break

            if line.upper() == "ALL":
                all_flag = True
                continue

            match = re.match(r"([\d.]+)~([\d.]+)\s*GHz", line, re.IGNORECASE)
            if match:
                lo = float(match.group(1))
                hi = float(match.group(2))
                ranges.append((min(lo, hi), max(lo, hi)))

        if all_flag and not ranges:
            result[spw_idx] = _ALL
        elif ranges:
            result[spw_idx] = ranges

    return result


def cont_dat_to_spw_string(ms_path, cont_ranges: dict) -> str:
    """Convert parsed cont.dat ranges to a CASA SPW:channel selection string.

    Must be called from inside a CASA session (uses casatools.msmetadata).

    Parameters
    ----------
    ms_path:
        Path to the measurement set.
    cont_ranges:
        Output of parse_cont_dat: {spw_idx: [(lo_ghz, hi_ghz)...] or 'ALL'}.

    Returns
    -------
    str
        A CASA SPW:channel selection string, e.g.
        "0:10~150;200~350,1:0~4095,2:30~120"
        Returns an empty string if no usable channels are found.
    """
    import numpy as np

    try:
        import casatools
        msmd = casatools.msmetadata()
    except ImportError:
        raise RuntimeError(
            "cont_dat_to_spw_string must be called from inside a CASA session."
        )

    msmd.open(str(ms_path))
    spw_parts = []

    try:
        n_spw = msmd.nspw()

        for spw_idx, ranges in cont_ranges.items():
            if spw_idx >= n_spw:
                continue

            chan_freqs_hz = msmd.chanfreqs(spw_idx)
            n_chan = len(chan_freqs_hz)

            if ranges == _ALL:
                spw_parts.append(f"{spw_idx}:0~{n_chan - 1}")
                continue

            mask = np.zeros(n_chan, dtype=bool)
            for lo_ghz, hi_ghz in ranges:
                lo_hz = lo_ghz * 1e9
                hi_hz = hi_ghz * 1e9
                mask |= (chan_freqs_hz >= lo_hz) & (chan_freqs_hz <= hi_hz)

            if not np.any(mask):
                continue

            chan_ranges = _mask_to_channel_ranges(mask)
            if chan_ranges:
                spw_parts.append(f"{spw_idx}:" + ";".join(chan_ranges))

    finally:
        msmd.done()

    return ",".join(spw_parts)


def _mask_to_channel_ranges(mask) -> list:
    """Convert a boolean channel mask to a list of 'lo~hi' channel range strings."""
    ranges = []
    in_run = False
    run_start = 0

    for i, val in enumerate(mask):
        if val and not in_run:
            run_start = i
            in_run = True
        elif not val and in_run:
            ranges.append(f"{run_start}~{i - 1}")
            in_run = False

    if in_run:
        ranges.append(f"{run_start}~{len(mask) - 1}")

    return ranges
