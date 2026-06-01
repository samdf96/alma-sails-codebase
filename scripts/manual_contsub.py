"""
manual_contsub.py
-----------------
Run continuum subtraction on a selfcal-complete MOUS using a pre-placed cont.dat.

Use this when autoselfcal completed (corrected.ms exists) but findcont failed,
leaving selfcal_products_sub empty. A properly formatted cont.dat from the
pipeline weblog must be placed in the MOUS auto_selfcal/ directory before
running this script.

What it does:
  1. Load selfcal_library.pickle to get SPW metadata and target info
  2. Parse cont.dat for continuum frequency ranges
  3. Convert freq ranges → channel numbers using the corrected.ms SPW table
     (corrected.ms has identical spectral structure to the original MS)
  4. Run uvcontsub on corrected.ms with datacolumn='data'
  5. Record the contsub product path in the database

Usage (inside a CASA pipeline container):
    casa --nologger -c alma-sails-codebase/scripts/manual_contsub.py --mous-id uid://A001/X.../X...
"""

from __future__ import annotations

import os
import pickle
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
from datetime import datetime

from alma_ops import db
from alma_ops.config import DB_PATH

sys.path.insert(0, '/arc/projects/ALMA-SAILS/auto_selfcal')
from auto_selfcal.selfcal_helpers import (
    get_fitspw_dict,
    get_spwnum_refvis,
    get_telescope,
    parse_contdotdat,
)


def _parse_listobs_spws(listobs_path: Path) -> dict[int, str]:
    """Parse a listobs file and return {spw_id: spw_name} for all SPWs listed."""
    spws: dict[int, str] = {}
    in_spw_block = False
    for line in listobs_path.read_text().splitlines():
        if "SpwID" in line and "Name" in line:
            in_spw_block = True
            continue
        if in_spw_block:
            # SPW rows start with whitespace then an integer ID
            m = re.match(r'^\s+(\d+)\s+(\S+)', line)
            if m:
                spws[int(m.group(1))] = m.group(2)
            elif line.strip() == "" or line.strip().startswith("Sources"):
                break
    return spws


def _find_listobs_pair(mous_dir: Path) -> tuple[Path, Path] | None:
    """Find a matched (raw, targets) listobs pair in mous_dir.

    Looks only in the immediate mous_dir — not subdirectories. Matches pairs
    where <base>.ms.listobs.txt and <base>_targets.ms.listobs.txt both exist.
    If multiple pairs are found (multiple ASDMs), returns the first one — all
    ASDMs in the same MOUS share the same spectral windows.

    Returns (raw_listobs, targets_listobs) or None if no pair is found.
    """
    candidates = sorted(mous_dir.glob("*.ms.listobs.txt"))
    for raw in candidates:
        if "_targets.ms.listobs.txt" in raw.name:
            continue
        base = raw.name.replace(".ms.listobs.txt", "")
        targets = mous_dir / f"{base}_targets.ms.listobs.txt"
        if targets.exists():
            return raw, targets
    return None


def _remap_cont_dat_if_needed(cont_dat: Path, mous_dir: Path) -> bool:
    """Remap cont.dat SPW IDs to match the split MS if they use raw calibrated IDs.

    Uses the listobs files in mous_dir to build an exact name-based SPW mapping
    (raw SPW ID → split SPW ID). The cont.dat SPW IDs are compared against the
    split MS listobs; if they don't match, they are remapped.

    The original cont.dat is backed up as cont.dat.original before rewriting.
    Returns True if remapping was performed, False if no change was needed.
    Exits with an error if the required listobs files are not found in mous_dir.
    """
    pair = _find_listobs_pair(mous_dir)
    if pair is None:
        print(f"[contsub] ERROR: Could not find a matching listobs pair in {mous_dir}")
        print(f"          Expected files named <asdm>.ms.listobs.txt and")
        print(f"          <asdm>_targets.ms.listobs.txt in the MOUS directory.")
        print(f"          Please place both listobs files there and re-run.")
        sys.exit(1)

    raw_listobs, targets_listobs = pair
    print(f"[contsub] Using listobs pair:")
    print(f"          Raw     : {raw_listobs.name}")
    print(f"          Targets : {targets_listobs.name}")

    raw_spws     = _parse_listobs_spws(raw_listobs)      # {id: name}
    targets_spws = _parse_listobs_spws(targets_listobs)  # {id: name}

    # Invert targets: name → split_spw_id
    targets_by_name = {name: spw_id for spw_id, name in targets_spws.items()}

    # Parse cont.dat SPW IDs
    cont_spw_ids = set()
    for line in cont_dat.read_text().splitlines():
        m = re.match(r'^\s*SpectralWindow:\s*(\d+)', line)
        if m:
            cont_spw_ids.add(int(m.group(1)))

    if not cont_spw_ids:
        print("[contsub] WARNING: No SpectralWindow entries found in cont.dat")
        return False

    # If all cont.dat SPW IDs are already in the targets listobs, no remap needed
    if cont_spw_ids.issubset(set(targets_spws.keys())):
        print(f"[contsub] cont.dat SPW IDs {sorted(cont_spw_ids)} already match split MS — no remapping needed")
        return False

    # Build remapping via SPW name matching
    spw_map: dict[int, int] = {}
    for raw_spw_id in cont_spw_ids:
        name = raw_spws.get(raw_spw_id)
        if name is None:
            print(f"[contsub] WARNING: cont.dat SPW {raw_spw_id} not found in raw listobs — skipping")
            continue
        split_spw_id = targets_by_name.get(name)
        if split_spw_id is None:
            print(f"[contsub] WARNING: SPW name '{name}' not found in targets listobs — skipping")
            continue
        print(f"[contsub] Remapping cont.dat SPW {raw_spw_id} → {split_spw_id}  ({name})")
        spw_map[raw_spw_id] = split_spw_id

    if not spw_map:
        print("[contsub] ERROR: Could not build any SPW remapping from listobs files")
        sys.exit(1)

    # Back up original
    backup = cont_dat.with_name("cont.dat.original")
    if not backup.exists():
        shutil.copy2(cont_dat, backup)
        print(f"[contsub] Backed up original → cont.dat.original")
    else:
        print(f"[contsub] cont.dat.original already exists — not overwriting backup")

    # Rewrite cont.dat with remapped SPW IDs
    new_lines = []
    for line in cont_dat.read_text().splitlines():
        m = re.match(r'^(\s*SpectralWindow:\s*)(\d+)(.*)', line)
        if m:
            old_spw = int(m.group(2))
            new_spw = spw_map.get(old_spw, old_spw)
            new_lines.append(f"{m.group(1)}{new_spw}{m.group(3)}")
        else:
            new_lines.append(line)
    cont_dat.write_text("\n".join(new_lines) + "\n")
    print(f"[contsub] Rewrote cont.dat with remapped SPW IDs")
    return True


def main(mous_id: str, db_path: Path) -> None:
    conn = db.get_connection(db_path)

    # ------------------------------------------------------------------
    # Pre-flight
    # ------------------------------------------------------------------
    row = db.get_pipeline_state(conn, mous_id)
    if row is None:
        print(f"[ERROR] MOUS {mous_id} not found in database")
        sys.exit(1)

    current_state = row["current_state"]
    if current_state != "selfcal_complete":
        print(f"[ERROR] Expected state 'selfcal_complete', got '{current_state}' — aborting")
        sys.exit(1)

    mous_dir = db.get_mous_directory(conn, mous_id)
    if mous_dir is None:
        print(f"[ERROR] mous_directory not set for {mous_id}")
        sys.exit(1)

    autoselfcal_dir = mous_dir / "auto_selfcal"

    cont_dat = autoselfcal_dir / "cont.dat"
    if not cont_dat.exists():
        print(f"[ERROR] cont.dat not found at {cont_dat}")
        print(f"        Place the pipeline cont.dat in auto_selfcal/ before running this script.")
        sys.exit(1)

    pickle_path = autoselfcal_dir / "selfcal_library.pickle"
    if not pickle_path.exists():
        print(f"[ERROR] selfcal_library.pickle not found at {pickle_path}")
        sys.exit(1)

    corrected_mses = sorted(autoselfcal_dir.glob("*_targets.corrected.ms"))
    if not corrected_mses:
        print(f"[ERROR] No *_targets.corrected.ms found in {autoselfcal_dir}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    log_path = mous_dir / f"casa-{datetime.now().strftime('%Y%m%d-%H%M%S')}-contsub.log"
    casalog.setlogfile(str(log_path))  # noqa: F821 — casalog is a CASA global
    print(f"[contsub] CASA log : {log_path}")
    print(f"[contsub] MOUS     : {mous_id}")
    print(f"[contsub] cont.dat : {cont_dat}")

    with open(pickle_path, "rb") as f:
        selfcal_library = pickle.load(f)

    # auto_selfcal uses relative paths internally — run from the working dir
    os.chdir(str(autoselfcal_dir))

    # ------------------------------------------------------------------
    # Remap cont.dat SPW IDs if they use raw calibrated MS numbering
    # ------------------------------------------------------------------
    _remap_cont_dat_if_needed(cont_dat, mous_dir)

    # ------------------------------------------------------------------
    # Process each corrected MS
    # ------------------------------------------------------------------
    contsub_products = []

    for corrected_ms in corrected_mses:
        out_name = corrected_ms.name.replace("_targets.corrected.ms", "_targets.contsub.ms")
        out_ms   = autoselfcal_dir / out_name

        print(f"[contsub] Input  : {corrected_ms.name}")
        print(f"[contsub] Output : {out_name}")

        if out_ms.exists():
            print(f"[contsub] Already exists — skipping uvcontsub")
            contsub_products.append(out_ms)
            continue

        telescope = get_telescope(str(corrected_ms))

        # Build fitspec dict: {str(field_num): {str(spw): {'chan': '...', 'fitorder': 1}}}
        # corrected.ms is used for all SPW lookups — it has the same spectral
        # structure as the original MS that the pickle was built against.
        fitspec = {}

        for target in selfcal_library:
            for band in selfcal_library[target]:
                selfcal_vis = selfcal_library[target][band]["vislist"][0]
                spwsarray   = selfcal_library[target][band][selfcal_vis]["spwsarray"]

                contdotdat = parse_contdotdat("cont.dat", target)
                if len(contdotdat["ranges"]) == 0:
                    print(f"[contsub] WARNING: No entry for target '{target}' in cont.dat — skipping")
                    continue

                spwvisref = get_spwnum_refvis(
                    [str(corrected_ms)],
                    target,
                    contdotdat,
                    {str(corrected_ms): spwsarray},
                    use_names=telescope in ["ALMA", "ACA"],
                )

                fitspw_dict = get_fitspw_dict(
                    str(corrected_ms),
                    target,
                    spwsarray,
                    [str(corrected_ms)],
                    str(corrected_ms),  # spwvisref — only one MS, so same
                    contdotdat,
                    telescope,
                )

                msmd.open(str(corrected_ms))  # noqa: F821 — msmd is a CASA global
                field_nums = msmd.fieldsforname(target)
                msmd.close()

                for fieldnum in field_nums:
                    fitspec[str(fieldnum)] = fitspw_dict

        if not fitspec:
            print(f"[ERROR] Could not build fitspec — check that cont.dat targets match the MS field names")
            sys.exit(1)

        print(f"[contsub] fitspec : {fitspec}")

        spw_string = ",".join(
            str(s) for s in selfcal_library[target][band][selfcal_vis]["spwsarray"]
        )

        uvcontsub(  # noqa: F821 — uvcontsub is a CASA global
            vis=str(corrected_ms),
            spw=spw_string,
            fitspec=fitspec,
            outputvis=str(out_ms),
            datacolumn="data",
        )

        if not out_ms.exists():
            print(f"[ERROR] uvcontsub did not produce {out_ms}")
            sys.exit(1)

        print(f"[contsub] uvcontsub complete: {out_name}")
        contsub_products.append(out_ms)

    if not contsub_products:
        print(f"[ERROR] No contsub products produced")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Update database
    # ------------------------------------------------------------------
    db.update_pipeline_paths(conn, mous_id, selfcal_products_sub=contsub_products)
    db.record_transition(
        conn,
        mous_id=mous_id,
        from_state="selfcal_complete",
        to_state="selfcal_complete",
        note="contsub products added via manual_contsub.py using pre-placed cont.dat",
        payload={
            "contsub_products": [str(p) for p in contsub_products],
            "cont_dat":         str(cont_dat),
        },
        step="contsub",
    )
    conn.close()

    print(f"[contsub] Products recorded ({len(contsub_products)}):")
    for p in contsub_products:
        print(f"    {p}")
    print(f"[OK]   selfcal_products_sub updated in database")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run uvcontsub on a selfcal-complete MOUS using a pre-placed cont.dat."
    )
    parser.add_argument("--mous-id", required=True, help="MOUS ID (uid://A001/...)")
    parser.add_argument("--db-path", default=DB_PATH, help="Path to the SQLite database")
    args = parser.parse_args()

    main(mous_id=args.mous_id, db_path=Path(args.db_path))
