"""
utils.py
--------------------
Utility functions for ALMA dataset operations.
"""


def to_db_mous_id(mous_id: str) -> str:
    """Converts a given mous_id to the database form.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to convert. Can be in either filesystem form (uid___A001_X123_Xabc)
        or database form (uid://A001/X123/Xabc).

    Returns
    -------
    str
        The MOUS ID in database form (uid://A001/X123/Xabc).

    Raises
    ------
    ValueError
        If the input MOUS ID cannot be interpreted.
    ValueError
        If the input MOUS ID is in an invalid format.
    """
    mous_id = mous_id.strip()

    # check if already in database form
    if mous_id.startswith("uid://"):
        return mous_id

    # check if in filesystem form → convert
    if mous_id.startswith("uid___"):
        core = mous_id.replace("uid___", "")
        parts = core.split("_")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid___ format: {mous_id}")
        return f"uid://{parts[0]}/{parts[1]}/{parts[2]}"

    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")


def to_dir_mous_id(mous_id: str) -> str:
    """Converts a given mous_id to the directory form.

    Parameters
    ----------
    mous_id : str
        The MOUS ID to convert. Can be in either filesystem form (uid___A001_X123_Xabc)
        or database form (uid://A001/X123/Xabc).

    Returns
    -------
    str
        The MOUS ID in directory form (uid___A001_X123_Xabc).

    Raises
    ------
    ValueError
        If the input MOUS ID cannot be interpreted.
    ValueError
        If the input MOUS ID is in an invalid format.
    """
    mous_id = mous_id.strip()

    # check if already in directory form
    if mous_id.startswith("uid___"):
        return mous_id

    # check if in database form → convert
    if mous_id.startswith("uid://"):
        core = mous_id.replace("uid://", "")
        parts = core.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid:// format: {mous_id}")
        return "uid___" + "_".join(parts)

    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")
