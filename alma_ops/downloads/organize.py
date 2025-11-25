# alma_ops/downloads/organize.py

import os
import shutil
from pathlib import Path
from typing import List, Tuple
from alma_ops.utils import to_dir_mous_id
from alma_ops.logging import get_logger

log = get_logger(__name__)

def organize_downloaded_files(mous_id: str, tmpdir: str,
                              download_dir: str, weblog_dir: str):
    """
    Move .ms and weblog_restore folders pulled from SRDP service
    into standardized project locations.
    """
    mous_dir_name = to_dir_mous_id(mous_id)
    ms_dirs, weblog_dirs = [], []

    for root, dirs, files in os.walk(tmpdir):
        for d in list(dirs):
            if d.endswith(".ms"):
                ms_dirs.append(os.path.join(root, d))
                dirs.remove(d)
            elif d == "weblog_restore":
                weblog_dirs.append(os.path.join(root, d))
                dirs.remove(d)

    ms_dirs = sorted(set(ms_dirs))
    weblog_dirs = sorted(set(weblog_dirs))

    asdm_paths = []
    moved_ms = False
    moved_weblog = False

    if ms_dirs:
        dest_root = Path(download_dir) / mous_dir_name
        dest_root.mkdir(parents=True, exist_ok=True)
        for ms in ms_dirs:
            dest = dest_root / Path(ms).name
            shutil.move(ms, dest)
            asdm_paths.append(str(dest))
        moved_ms = True
        log.info(f"[{mous_id}] Moved {len(ms_dirs)} .ms → {dest_root}")
    else:
        log.warning(f"[{mous_id}] No .ms directories found.")

    if weblog_dirs:
        dest_root = Path(weblog_dir) / mous_dir_name / "weblog_restore"
        dest_root.mkdir(parents=True, exist_ok=True)
        for wb in weblog_dirs:
            dest = dest_root / Path(wb).name
            shutil.move(wb, dest)
        moved_weblog = True
        log.info(f"[{mous_id}] Moved weblog_restore → {dest_root}")
    else:
        log.warning(f"[{mous_id}] No weblog_restore found.")

    return asdm_paths, moved_ms, moved_weblog