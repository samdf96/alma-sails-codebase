# alma_ops/utils.py

def to_db_mous_id(mous_id: str) -> str:
    """
    Convert either:
      • uid___A001_X123_Xabc
      • uid://A001/X123/Xabc
    into the canonical database form:
      → uid://A001/X123/Xabc
    """
    mous_id = mous_id.strip()

    # Case 1: Already database form
    if mous_id.startswith("uid://"):
        return mous_id

    # Case 2: Filesystem form → convert
    if mous_id.startswith("uid___"):
        core = mous_id.replace("uid___", "")
        parts = core.split("_")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid___ format: {mous_id}")
        return f"uid://{parts[0]}/{parts[1]}/{parts[2]}"

    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")

def to_dir_mous_id(mous_id: str) -> str:
    """
    Convert either:
      • uid___A001_X123_Xabc
      • uid://A001/X123/Xabc
    into the filesystem-safe format:
      → uid___A001_X123_Xabc
    """
    mous_id = mous_id.strip()

    # Case 1: Already dir form
    if mous_id.startswith("uid___"):
        return mous_id

    # Case 2: Database form → convert
    if mous_id.startswith("uid://"):
        core = mous_id.replace("uid://", "")
        parts = core.split("/")
        if len(parts) != 3:
            raise ValueError(f"Invalid uid:// format: {mous_id}")
        return "uid___" + "_".join(parts)

    raise ValueError(f"Cannot interpret MOUS ID: {mous_id}")



def normalize_asdm_dir(asdm_uid: str) -> str:
    """Convert uid://A002/Xce3de5/X6314 → uid___A002_Xce3de5_X6314."""
    return normalize_mous_dir(asdm_uid)


def uid_to_filename(uid: str, suffix: str = ".ms") -> str:
    """Convert uid to safe filename like uid___...ms"""
    return normalize_mous_dir(uid) + suffix