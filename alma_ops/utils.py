"""
utils.py
--------
Utility functions for ALMA dataset operations.

No module-level pandas import: to_dir_mous_id/to_db_mous_id are imported by
scripts running inside CASA's embedded Python (no pandas bundled there), so
pandas is imported lazily inside infer_array_config() -- the one function
that actually needs it, and the only one ever called from VM-shell scripts
(seed_database.py) rather than CASA.
"""


def to_db_mous_id(mous_id: str) -> str:
    """Convert a MOUS ID to database form (uid://A001/X123/Xabc).

    Accepts either filesystem form (uid___A001_X123_Xabc) or database form.
    """
    mous_id = mous_id.strip()
    if mous_id.startswith("uid://"):
        return mous_id
    if mous_id.startswith("uid___"):
        core = mous_id.replace("uid___", "")
        parts = core.split("_")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid___ format: {mous_id}")
        return f"uid://{parts[0]}/{parts[1]}/{parts[2]}"
    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")


def infer_array_config(antenna_arrays: str) -> str:
    """Infer 12m / 7m / tp from the antenna_arrays field.

    TP antennas (PM on T7xx pads) are routinely included in 12m and 7m
    observations as reference antennas, so T7xx presence alone is not a
    reliable TP indicator. Classification is based on the non-TP antennas:

      1. Any CM antenna among non-TP antennas → 7m
      2. ≥5 non-TP antennas                  → 12m
      3. Any T7xx pad present                 → tp  (only PM antennas remain)
      4. ≤4 total antennas                    → tp
      5. Otherwise                            → 12m

    Used by seed_database.py for initial seeding; lives in alma_ops rather
    than there so it isn't tied to that one script (the one-off repair that
    also depended on this, fix_targets_array_config.py, is archived —
    see project root _archive/).
    """
    import pandas as pd

    if not antenna_arrays or pd.isna(antenna_arrays):
        return "unknown"
    parts = str(antenna_arrays).split()
    pads  = [p.split(":")[0].upper() for p in parts if ":" in p]
    names = [p.split(":")[1].upper() for p in parts if ":" in p]
    non_tp_names = [n for n, p in zip(names, pads) if not p.startswith("T7")]
    if any(n.startswith("CM") for n in non_tp_names):
        return "7m"
    if len(non_tp_names) >= 5:
        return "12m"
    if any(p.startswith("T7") for p in pads):
        return "tp"
    if len(parts) <= 4:
        return "tp"
    return "12m"


def to_dir_mous_id(mous_id: str) -> str:
    """Convert a MOUS ID to directory form (uid___A001_X123_Xabc).

    Accepts either database form (uid://A001/X123/Xabc) or directory form.
    """
    mous_id = mous_id.strip()
    if mous_id.startswith("uid___"):
        return mous_id
    if mous_id.startswith("uid://"):
        core = mous_id.replace("uid://", "")
        parts = core.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid:// format: {mous_id}")
        return "uid___" + "_".join(parts)
    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")
