"""
fix_split_products.py
----------------
Interactive script to concatenate selected Measurement Sets (MS) in a directory.
"""

import sys
from pathlib import Path

from casatasks import concat, listobs


def find_ms_dirs(base_dir):
    return sorted(
        [p for p in base_dir.iterdir() if p.is_dir() and p.name.endswith(".ms")]
    )


def print_ms_list(ms_list):
    print("\nAvailable Measurement Sets:")
    for i, ms in enumerate(ms_list):
        print(f"  [{i}] {ms.name}")


def parse_indices(s, max_index):
    try:
        indices = [int(x) for x in s.split()]
        if not indices:
            return None
        if any(i < 0 or i > max_index for i in indices):
            return None
        return indices
    except ValueError:
        return None


def confirm(prompt="Proceed? [y/N]: "):
    return input(prompt).strip().lower() in ("y", "yes")


def main(base_path):
    base_dir = Path(base_path).expanduser().resolve()

    if not base_dir.is_dir():
        print(f"ERROR: Not a directory: {base_dir}")
        sys.exit(1)

    remaining = find_ms_dirs(base_dir)
    produced = []

    while remaining:
        print_ms_list(remaining)

        sel = input(
            "\nEnter indices to concatenate (space-separated), or 'q' to quit: "
        ).strip()

        if sel.lower() == "q":
            break

        indices = parse_indices(sel, len(remaining) - 1)
        if indices is None or len(indices) < 2:
            print("ERROR: Please select at least two valid indices.")
            continue

        inputs = [remaining[i] for i in indices]

        print("\nSelected MSs:")
        for ms in inputs:
            print(f"  {ms.name}")

        out_name = input("\nEnter output MS name (must end in .ms): ").strip()
        if not out_name.endswith(".ms"):
            print("ERROR: Output name must end with .ms")
            continue

        out_path = base_dir / out_name

        print("\nConcat command:")
        print(f"  concat(vis={[str(p) for p in inputs]}, concatvis='{out_path}')")

        if not confirm():
            print("Skipping.")
            continue

        concat(
            vis=[str(p) for p in inputs],
            concatvis=str(out_path),
        )

        produced.append(out_path)

        # Remove consumed MSs
        remaining = [ms for i, ms in enumerate(remaining) if i not in indices]

        print(f"\nCreated: {out_path.name}")
        print(f"Remaining MSs: {len(remaining)}")

    # Post-processing
    if produced:
        print("\nConcatenated MSs produced:")
        for ms in produced:
            print(f"  {ms.name}")

        if confirm("\nRun listobs() on all concatenated MSs? [y/N]: "):
            for ms in produced:
                listobs(vis=str(ms), listfile=f"{ms}.listobs", overwrite=True)
                print(f"  listobs written: {ms}.listobs")

    print("\nDone.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python interactive_concat.py <base_directory>")
        sys.exit(1)

    main(sys.argv[1])
