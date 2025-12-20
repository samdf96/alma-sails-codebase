# alma_ops/downloads/organize.py

import os
import shutil
from pathlib import Path

from alma_ops.utils import to_dir_mous_id

def organize_downloaded_files(mous_id: str, tmpdir: str,
                              download_dir: str, weblog_dir: str,
                              logger=None):
    """
    Move .ms and weblog_restore folders pulled from SRDP service
    into standardized project locations.
    """
    if logger is None:
        from alma_ops.logging import get_logger
        log = get_logger(__name__)
    else:
        log = logger
        
    log.info(f"[{mous_id}] Starting organization of downloaded files...")
    
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
    log.info(f"Found {len(ms_dirs)} .ms directories:\n" + "\n".join(f"  {i+1}. {ms}" for i, ms in enumerate(ms_dirs)))
    weblog_dirs = sorted(set(weblog_dirs))
    log.info(f"Found {len(weblog_dirs)} weblog_restore directories:\n" + "\n".join(f"  {i+1}. {wb}" for i, wb in enumerate(weblog_dirs)))

    asdm_paths = []
    moved_ms = False
    moved_weblog = False
    
    # Always define destination directories
    dest_root = Path(download_dir) / mous_dir_name
    log.info(f"[{mous_id}] Creating destination root directory: {dest_root}")
    dest_root.mkdir(parents=True, exist_ok=True)
    
    weblog_dest_root = Path(weblog_dir) / mous_dir_name / "weblog_restore"
    log.info(f"[{mous_id}] Creating weblog_restore destination root directory: {weblog_dest_root}")
    weblog_dest_root.mkdir(parents=True, exist_ok=True)

    if ms_dirs:
        for ms in ms_dirs:
            dest = dest_root / Path(ms).name
            shutil.move(ms, dest)
            asdm_paths.append(str(dest))
        moved_ms = True
        log.info(f"[{mous_id}] Moved {len(ms_dirs)} .ms → {dest_root}")
    else:
        log.warning(f"[{mous_id}] No .ms directories found.")

    if weblog_dirs:
        for wb in weblog_dirs:
            dest = weblog_dest_root / Path(wb).name
            shutil.move(wb, dest)
        moved_weblog = True
        log.info(f"[{mous_id}] Moved weblog_restore → {weblog_dest_root}")
    else:
        log.warning(f"[{mous_id}] No weblog_restore found.")

    return asdm_paths, moved_ms, moved_weblog, dest_root