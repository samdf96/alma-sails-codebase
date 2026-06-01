"""
download.py
-----------
Work unit for downloading MOUS datasets from the NRAO SRDP service.

This module is a pure callable — no DB access, no Prefect, no awareness of
how it is invoked. It is called by the orchestration layer (manual script or
Prefect flow) which is responsible for reading the SRDP URL and writing the
result back to the database.

Requires wget2 to be available in the execution environment (c-astroml container).
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from alma_ops.models import DownloadResult
from alma_ops.utils import to_dir_mous_id


# wget2 flags tuned for SRDP directory structure
_WGET2_FLAGS = [
    "-r",                       # recursive
    "-l", "10",                 # max recursion depth
    "--reject-regex=index.html*",
    "-np",                      # no parent directories
    "-nH",                      # strip hostname from paths
    "--cut-dirs=3",             # strip first 3 URL path components
    "--progress=bar",           # force progress bar even when not attached to a TTY
]


def _run_wget2(url: str, dest_dir: Path) -> tuple[bool, str]:
    """Run wget2 and return (success, error string).

    stdout and stderr inherit from the parent process so wget2 progress
    streams directly to the terminal, matching the behaviour of running
    wget2 in a shell script.
    """
    result = subprocess.run(
        ["wget2"] + _WGET2_FLAGS + [url],
        cwd=str(dest_dir),
    )
    # Exit code 8 means server errors (e.g. some 404s) but data was still downloaded
    success = result.returncode in (0, 8)
    error = "" if success else f"wget2 exited with code {result.returncode}"
    return success, error


def _sha1_file(path: Path) -> str:
    """Compute SHA1 of a file using chunked reads to handle large files."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _verify_sha1sums(directory: Path) -> tuple[int, int, int]:
    """Verify SHA1SUMS file in directory. Returns (passed, failed, missing)."""
    sha1_file = directory / "SHA1SUMS"
    if not sha1_file.exists():
        return 0, 0, 0

    passed = failed = missing = 0
    with open(sha1_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            expected_hash, filepath = line.split(None, 1)
            full_path = directory / filepath
            if not full_path.exists():
                missing += 1
                continue
            if _sha1_file(full_path) == expected_hash:
                passed += 1
            else:
                failed += 1

    return passed, failed, missing


def _collect_directories(root: Path, suffix: str) -> list[Path]:
    """Walk root and collect all directories ending with the given suffix."""
    found = []
    for path in root.rglob("*"):
        if path.is_dir() and path.name.endswith(suffix):
            found.append(path)
    return sorted(found)


def run_download(
    mous_id: str,
    srdp_url: str,
    datasets_dir: Path,
    weblog_dir: Path,
) -> DownloadResult:
    """Download a MOUS dataset from the NRAO SRDP service and organize the output.

    Steps:
      1. Create a temporary directory inside datasets_dir for the raw download
      2. Run wget2 to fetch all files from srdp_url
      3. Verify SHA1SUMS if present
      4. Move .ms directories into the MOUS dataset directory
      5. Move weblog_restore into weblog_dir
      6. Remove the temporary directory
      7. Return a DownloadResult

    Parameters
    ----------
    mous_id:
        MOUS ID in database form (uid://A001/...).
    srdp_url:
        Direct URL to the SRDP data product directory.
    datasets_dir:
        Root datasets directory. The MOUS directory will be created here.
    weblog_dir:
        Root weblog directory. weblog_restore will be moved here.

    Returns
    -------
    DownloadResult
        Result object including product paths, SHA1 outcome, and duration.
    """
    start = time.monotonic()
    mous_dir_name = to_dir_mous_id(mous_id)
    tmpdir = Path(tempfile.mkdtemp(prefix=f"{mous_dir_name}_", dir=datasets_dir))

    try:
        # ----------------------------------------------------------------
        # 1. Download
        # ----------------------------------------------------------------
        wget_ok, wget_stderr = _run_wget2(srdp_url, tmpdir)
        if not wget_ok:
            return DownloadResult(
                mous_id=mous_id,
                success=False,
                download_path=None,
                size_bytes=None,
                sha1_verified=False,
                duration_s=time.monotonic() - start,
                error=f"wget2 failed: {wget_stderr[-500:]}",
            )

        # ----------------------------------------------------------------
        # 2. SHA1 verification
        # ----------------------------------------------------------------
        print(f"[download] wget2 complete — verifying SHA1 checksums...")
        passed, failed, missing = _verify_sha1sums(tmpdir)
        sha1_ok = failed == 0

        if passed or failed or missing:
            print(f"[download] SHA1: {passed} passed, {failed} failed, {missing} missing")
        else:
            print(f"[download] No SHA1SUMS file found — skipping verification")

        if not sha1_ok:
            return DownloadResult(
                mous_id=mous_id,
                success=False,
                download_path=None,
                size_bytes=None,
                sha1_verified=False,
                duration_s=time.monotonic() - start,
                error=f"SHA1 verification failed: {failed} file(s) corrupted, {missing} missing",
            )

        # ----------------------------------------------------------------
        # 3. Create MOUS directory and move .ms products
        # ----------------------------------------------------------------
        mous_dir = datasets_dir / mous_dir_name
        mous_dir.mkdir(parents=True, exist_ok=True)

        ms_dirs = _collect_directories(tmpdir, ".ms")
        print(f"[download] Moving {len(ms_dirs)} MS director{'y' if len(ms_dirs) == 1 else 'ies'} to {mous_dir.name}/")
        for ms in ms_dirs:
            dest = mous_dir / ms.name
            if dest.exists():
                print(f"[download]   {ms.name} already exists — skipping")
                continue
            shutil.move(str(ms), str(dest))

        ms_products = [mous_dir / ms.name for ms in ms_dirs]

        # ----------------------------------------------------------------
        # 4. Move weblog_restore
        # ----------------------------------------------------------------
        weblog_dirs = [d for d in tmpdir.rglob("weblog_restore") if d.is_dir()]
        print(f"[download] Moving {len(weblog_dirs)} weblog director{'y' if len(weblog_dirs) == 1 else 'ies'}...")
        weblog_mous_dir = weblog_dir / mous_dir_name
        weblog_mous_dir.mkdir(parents=True, exist_ok=True)
        for wb in weblog_dirs:
            dest = weblog_mous_dir / wb.name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.move(str(wb), str(dest))

        # ----------------------------------------------------------------
        # 5. Compute total download size and clean up temp dir
        # ----------------------------------------------------------------
        print(f"[download] Computing total size...")
        size_bytes = sum(
            f.stat().st_size
            for f in mous_dir.rglob("*")
            if f.is_file()
        )
        shutil.rmtree(tmpdir, ignore_errors=True)

        return DownloadResult(
            mous_id=mous_id,
            success=True,
            download_path=mous_dir,
            size_bytes=size_bytes,
            sha1_verified=sha1_ok,
            duration_s=time.monotonic() - start,
        )

    except Exception as e:
        shutil.rmtree(tmpdir, ignore_errors=True)
        return DownloadResult(
            mous_id=mous_id,
            success=False,
            download_path=None,
            size_bytes=None,
            sha1_verified=False,
            duration_s=time.monotonic() - start,
            error=str(e),
        )
