# alma_ops/downloads/fetch.py

import os
import re
import shutil
import tempfile
import subprocess
from pathlib import Path
from typing import Optional
from collections import defaultdict

from alma_ops.logging import get_logger

log = get_logger(__name__)

def download_archive(url: str, tmpdir: str, mous_id: str,
                     depth: int = 10,
                     returncode_tolerate: Optional[list[int]] = None):
    """
    Download ALMA MOUS content recursively using wget2.

    Parameters
    ----------
    url : str
        Base URL for the MOUS on the ALMA Archive (from serpens_main.db).
    tmpdir : str
        Temporary directory where wget2 writes the mirrored structure.
    mous_id : str
        Identifier (e.g. uid://A001/...); used only for logging.
    depth : int
        Maximum recursion depth for wget2 (-l flag).
    returncode_tolerate : list[int] or None
        Return codes that should NOT raise an exception. 
        Defaults to [8] which corresponds to HTTP errors.

    Notes
    -----
    wget2 returncode 8 = "Server issued an error response"  
    which is common for the SRDP service; often harmless.
    """
    if returncode_tolerate is None:
        returncode_tolerate = [8]

    Path(tmpdir).mkdir(parents=True, exist_ok=True)

    wget_cmd = [
        "wget2",
        "-r", f"-l{depth}",
        "--reject-regex=index.html*",
        "-np", "-nH",
        "--cut-dirs=3",
        "-P", tmpdir,
        url,
    ]

    log.info(f"[{mous_id}] Starting wget2 download…")
    log.debug(f"Command: {' '.join(wget_cmd)}")

    try:
        subprocess.run(wget_cmd, check=True)
        log.info(f"[{mous_id}] wget2 completed successfully.")
    except subprocess.CalledProcessError as e:
        if e.returncode in returncode_tolerate:
            log.warning(
                f"[{mous_id}] wget2 returned tolerated error {e.returncode}; "
                f"continuing anyway."
            )
        else:
            log.error(f"[{mous_id}] wget2 failed with code {e.returncode}.")
            raise

def _build_tree(url: str, file_list: list[str], depth_limit: int):
    """
    Build a nested dictionary tree structure up to `depth_limit` levels.
    """
    def tree():
        return defaultdict(tree)

    root = tree()
    url_base = url.rstrip("/")

    for f in file_list:
        rel = f.replace(url_base, "").strip("/")
        if not rel:
            continue
        parts = rel.split("/")[:depth_limit]
        node = root
        for p in parts:
            node = node[p]

    return root


def _print_tree(node, prefix: str = "", depth: int = 0, depth_limit: int = 3):
    """
    Nicely-print a nested dictionary tree up to depth_limit.
    """
    keys = sorted(node.keys())
    for i, k in enumerate(keys):
        branch = "└── " if i == len(keys) - 1 else "├── "
        sub_prefix = "    " if i == len(keys) - 1 else "│   "

        print(f"{prefix}{branch}{k}/")

        if isinstance(node[k], dict) and node[k] and depth < depth_limit - 1:
            _print_tree(node[k], prefix + sub_prefix, depth + 1, depth_limit)


def dry_run_preview(url: str, depth: int = 3):
    """
    Perform wget2 spider-mode dry-run and print a simplified directory tree.

    Parameters
    ----------
    url : str
        The archive root URL for the MOUS.
    depth : int
        How many directory levels to display (default = 3).

    Notes
    -----
    - Uses wget2 --spider to avoid downloading actual data.
    - Cleans up temporary files afterward.
    """
    temp_dir = tempfile.mkdtemp(prefix="dry_run_")

    wget_cmd = [
        "wget2",
        "--spider",
        "-r", "-l", "10",
        "--reject-regex=index.html*",
        "-np", "-nH",
        "--cut-dirs=3",
        "-P", temp_dir,
        url,
    ]

    log.info(f"[DRY-RUN] Checking structure for {url}")

    result = subprocess.run(wget_cmd, capture_output=True, text=True)
    stdout = result.stdout + "\n" + result.stderr

    # Extract URLs found by spidering
    file_matches = re.findall(r'https?://[^\s\'"<>]+', stdout)

    clean_urls = sorted({
        f.strip().rstrip("]")
        for f in file_matches
        if f.startswith(url)
        and not "?" in f
        and not f.endswith("index.html")
    })

    # Build nested tree
    tree_root = _build_tree(url, clean_urls, depth_limit=depth)

    # Print result
    log.info(f"[DRY-RUN] Directory structure (up to {depth} levels):")
    print(f"┬ {url}")
    _print_tree(tree_root, depth_limit=depth)

    # Cleanup
    shutil.rmtree(temp_dir)