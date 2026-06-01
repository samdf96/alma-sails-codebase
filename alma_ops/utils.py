"""
utils.py
--------
Utility functions for ALMA dataset operations.
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
