"""
run_listobs_all_ms.py
----------------
Run listobs() on all Measurement Sets (.ms directories) in a given base directory.
"""

import sys
from pathlib import Path

from casatasks import listobs


def find_ms_dirs(base_dir):
    return sorted(
        [p for p in base_dir.iterdir() if p.is_dir() and p.name.endswith(".ms")]
    )


def confirm(prompt="Proceed? [y/N]: "):
    return input(prompt).strip().lower() in ("y", "yes")


def main(base_path):
    base_dir = Path(base_path).expanduser().resolve()

    if not base_dir.is_dir():
        print(f"ERROR: Not a directory: {base_dir}")
        return

    ms_list = find_ms_dirs(base_dir)

    if not ms_list:
        print("No .ms directories found.")
        return

    print("\nFound the following Measurement Sets:\n")
    for ms in ms_list:
        print(f"  {ms.name}")

    print("\nThe following listobs() commands will be run:\n")
    for ms in ms_list:
        outfile = f"{ms}.listobs.txt"
        print(f"  listobs(vis='{ms}', listfile='{outfile}', overwrite=True)")

    if not confirm():
        print("\nAborted.")
        return

    print("\nRunning listobs...\n")
    for ms in ms_list:
        outfile = f"{ms}.listobs.txt"
        print(f"  → {ms.name}")
        listobs(vis=str(ms), listfile=outfile, overwrite=True)

    print("\nDone.")


def _cli():
    if len(sys.argv) != 2:
        print("Usage: python run_listobs_all_ms.py <base_directory>")
        return
    main(sys.argv[1])


if __name__ == "__main__":
    _cli()
