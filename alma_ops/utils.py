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

from __future__ import annotations


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


def code_version() -> str | None:
    """Short git commit of the codebase, for stamping into product records.

    Products are only as trustworthy as the code that made them: a TOPO/LSRK
    frame bug silently produced wrong cubes for months, and telling a good
    product from a bad one afterwards meant inferring it from timestamps.

    Reads .git directly rather than shelling out to `git`. The stamp has to work
    inside CANFAR headless containers, where invoking git is unreliable -- it may
    not be installed, and when the repo is owned by a different uid than the
    container user, git refuses with "dubious ownership" (exit 128, no output).
    Both failures return None silently, which is how two completed runs ended up
    unstamped and would have been re-queued forever.

    Plain file reads have neither problem. The git binary is still used, best
    effort, only to mark a dirty tree.

    Returns None if the commit genuinely cannot be determined.
    """
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    git_dir = repo / ".git"
    rev = None
    try:
        head = (git_dir / "HEAD").read_text().strip()
        if head.startswith("ref:"):
            ref = head.split(None, 1)[1].strip()
            ref_file = git_dir / ref
            if ref_file.exists():
                rev = ref_file.read_text().strip()
            else:
                # ref may only exist in packed-refs
                packed = git_dir / "packed-refs"
                if packed.exists():
                    for line in packed.read_text().splitlines():
                        if line.startswith("#") or not line.strip():
                            continue
                        parts = line.split()
                        if len(parts) == 2 and parts[1] == ref:
                            rev = parts[0]
                            break
        else:
            rev = head          # detached HEAD holds the sha directly
    except Exception:
        return None

    if not rev or len(rev) < 7:
        return None
    rev = rev[:7]

    # Dirty marker is best effort: if git is unavailable the commit alone is
    # still far better than nothing.
    try:
        import subprocess

        out = subprocess.run(
            ["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no"],
            capture_output=True, text=True, timeout=30,
        )
        if out.returncode == 0 and out.stdout.strip():
            rev += "-dirty"
    except Exception:
        pass
    return rev


def commits_since(baseline: str, repo_root=None) -> list:
    """Short SHAs of `baseline` and every commit after it, newest first.

    For deciding which recorded code_commit stamps count as current. A single
    accepted commit stops working the moment anything else lands: the campaign
    baseline is "the commit at which the output changed", and every commit after
    it produces equally valid products, so the acceptable set is a range.

    Unlike code_version(), this shells out to git and is therefore VM-only --
    the CANFAR containers have no git, or refuse with "dubious ownership".
    Callers are the project-root reporting/launcher scripts, never a headless
    job. Returns [] if git is unavailable or the baseline is unknown, so the
    caller can fall back rather than crash.
    """
    import subprocess
    from pathlib import Path

    root = str(repo_root or Path(__file__).resolve().parents[1])
    try:
        out = subprocess.run(
            ["git", "-C", root, "rev-list", f"{baseline}^..HEAD"],
            capture_output=True, text=True, timeout=30,
        )
        if out.returncode != 0:
            return []
        return [c[:7] for c in out.stdout.split()]
    except Exception:
        return []
