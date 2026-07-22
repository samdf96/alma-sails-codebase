#!/usr/bin/env python3
"""
audit_mous_disk_state.py
--------------------------
Full MOUS-level audit: cross-checks every MOUS's pipeline_state against what
actually exists on disk, and produces a final punch list of what still needs
attention.

split_products is expected to go missing for any MOUS that has progressed
past split -- the selfcal workflow consumes/replaces the original split MS
with a corrected version under auto_selfcal/, and split_products is never
cleared afterward. That is NOT a problem on its own, so it is reported
separately as informational only.

The real signal is whether selfcal_products / selfcal_products_sub (the
actual imaging input) is missing while current_state claims
'selfcal_complete' -- this is the lingering symptom of the post_selfcal.py
"phantom complete" bug (recording original _targets.ms as selfcal_products
before a findcont crash was caught) and any other manual disk cleanup that
wasn't reflected back into the DB.

TP (total power) datasets skip selfcal entirely (see manual_split.py's
tp_ready shortcut) and use inconsistent naming (.ms, .ms.cal.jy, .ms_bl,
.selfcal.ms have all been observed) -- rather than checking exact recorded
filenames, every TP MOUS directory gets a loose, name-agnostic listing
(depth-bounded, no '**' recursive glob -- see generate_mosaic_regions.py's
find_listobs for why that hangs) so the actual contents can be eyeballed.

Run from project root:
    python3 audit_mous_disk_state.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import DATASETS_DIR, DB_PATH
from alma_ops.db import to_local

SEP = "=" * 78


def mous_dir_name(mous_id: str) -> str:
    return mous_id.replace("uid://", "uid___").replace("/", "_")


def check_path_list(json_str: str | None) -> tuple[list[str], list[str]]:
    if not json_str:
        return [], []
    try:
        paths = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return [], []
    if isinstance(paths, str):
        paths = [paths]
    present, missing = [], []
    for p in paths:
        local = to_local(p)
        (present if Path(local).exists() else missing).append(str(local))
    return present, missing


def loose_tp_scan(mous_dir: Path) -> list[str]:
    """Depth-bounded, name-agnostic scan for anything MS/ASDM-like under mous_dir."""
    if not mous_dir.is_dir():
        return []
    hits = []
    try:
        top_entries = sorted(mous_dir.iterdir())
    except OSError:
        return []
    for entry in top_entries:
        name_lower = entry.name.lower()
        if entry.is_dir() and (".ms" in name_lower or "asdm" in name_lower or "uid" in name_lower):
            hits.append(str(entry))
        if entry.is_dir():
            try:
                for sub in sorted(entry.iterdir())[:50]:
                    sub_lower = sub.name.lower()
                    if ".ms" in sub_lower or "asdm" in sub_lower:
                        hits.append(str(sub))
            except OSError:
                continue
    return hits


def feeding_imaging_units(conn, mous_id: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT iu.target_name, iu.band, iu.config_type, ips.current_state
        FROM mous_imaging_unit miu
        JOIN imaging_unit iu ON iu.imaging_unit_id = miu.imaging_unit_id
        JOIN imaging_pipeline_state ips ON ips.imaging_unit_id = miu.imaging_unit_id
        WHERE miu.mous_id=?
        ORDER BY iu.target_name
        """,
        (mous_id,),
    ).fetchall()
    return [f"{r['target_name']} B{r['band']} {r['config_type']} [{r['current_state']}]" for r in rows]


def main() -> None:
    conn = db.get_connection(str(DB_PATH))

    rows = conn.execute(
        """
        SELECT p.mous_id, p.current_state, p.mous_directory,
               p.split_products, p.selfcal_products, p.selfcal_products_sub,
               m.project_code, m.band, m.array_config
        FROM pipeline_state p
        JOIN mous m ON m.mous_id = p.mous_id
        ORDER BY m.array_config, p.mous_id
        """
    ).fetchall()

    print(f"{len(rows)} MOUS total\n")

    failed: list[dict] = []
    real_problems: list[dict] = []
    benign_split_only: list[dict] = []
    tp_rows: list[dict] = []
    ok_count = 0

    for row in rows:
        mous_id = row["mous_id"]
        array_config = row["array_config"]
        state = row["current_state"]

        mous_dir_recorded = row["mous_directory"]
        mous_dir_local = Path(to_local(mous_dir_recorded)) if mous_dir_recorded else \
            DATASETS_DIR / mous_dir_name(mous_id)
        dir_exists = mous_dir_local.is_dir()

        _, split_missing = check_path_list(row["split_products"])
        sc_present, sc_missing = check_path_list(row["selfcal_products"])
        scsub_present, scsub_missing = check_path_list(row["selfcal_products_sub"])

        record = {
            "mous_id": mous_id, "project_code": row["project_code"],
            "band": row["band"], "array_config": array_config, "state": state,
            "mous_dir_local": str(mous_dir_local), "dir_exists": dir_exists,
            "split_missing": split_missing, "sc_missing": sc_missing,
            "scsub_missing": scsub_missing,
        }

        if state == "failed":
            failed.append(record)
        elif state == "selfcal_complete" and (sc_missing or scsub_missing):
            real_problems.append(record)
        elif split_missing:
            benign_split_only.append(record)
        else:
            ok_count += 1

        if array_config == "tp":
            hits = loose_tp_scan(mous_dir_local)
            tp_rows.append({**record, "loose_hits": hits})

    print(SEP)
    print(f"OK (state matches disk, nothing stale):     {ok_count}")
    print(f"FAILED (pipeline_state):                    {len(failed)}")
    print(f"REAL PROBLEM (corrected MS actually gone):  {len(real_problems)}")
    print(f"benign (only the consumed split MS is gone): {len(benign_split_only)}  -- informational, not an action item")
    print(f"TP datasets (separate loose review):        {len(tp_rows)}")
    print(SEP)

    if failed:
        print("\n--- FAILED MOUS ---")
        for r in failed:
            note = conn.execute(
                "SELECT note FROM state_transitions WHERE mous_id=? AND to_state='failed' ORDER BY at DESC LIMIT 1",
                (r["mous_id"],),
            ).fetchone()
            print(f"  {r['mous_id']}  ({r['project_code']}, B{r['band']} {r['array_config']})")
            if note and note[0]:
                print(f"    reason: {note[0]}")
            for u in feeding_imaging_units(conn, r["mous_id"]):
                print(f"    feeds: {u}")

    if real_problems:
        print("\n--- REAL PROBLEM: selfcal_complete but corrected MS missing from disk ---")
        for r in real_problems:
            print(f"  {r['mous_id']}  ({r['project_code']}, B{r['band']} {r['array_config']})")
            if r["sc_missing"]:
                print(f"    selfcal_products MISSING ({len(r['sc_missing'])}):")
                for p in r["sc_missing"]:
                    print(f"      - {p}")
            if r["scsub_missing"]:
                print(f"    selfcal_products_sub MISSING ({len(r['scsub_missing'])}):")
                for p in r["scsub_missing"]:
                    print(f"      - {p}")
            for u in feeding_imaging_units(conn, r["mous_id"]):
                print(f"    feeds: {u}")

    if tp_rows:
        print("\n--- TP datasets (loose disk review, selfcal not applicable) ---")
        for r in tp_rows:
            tag = "ok" if r["loose_hits"] else "EMPTY/NOTHING FOUND"
            print(f"  {r['mous_id']}  ({r['project_code']}, B{r['band']}, state={r['state']})  [{tag}]")
            if not r["loose_hits"]:
                print(f"    dir: {r['mous_dir_local']}  (exists: {r['dir_exists']})")
                print("    nothing MS/ASDM-like found at top level or one level deep")

    if benign_split_only:
        print(f"\n--- benign: split_products stale only ({len(benign_split_only)} MOUS, not an action item) ---")
        print("  (original split MS consumed by selfcal and replaced under auto_selfcal/, as expected)")
        print("  " + ", ".join(r["mous_id"] for r in benign_split_only))

    print(f"\n{SEP}")
    print("PUNCH LIST")
    print(SEP)
    if not failed and not real_problems:
        print("Nothing outstanding at the MOUS level.")
    else:
        if failed:
            print(f"- Resolve {len(failed)} failed MOUS: " + ", ".join(r["mous_id"] for r in failed))
        if real_problems:
            print(f"- Recover {len(real_problems)} MOUS with genuinely missing corrected MS: "
                  + ", ".join(r["mous_id"] for r in real_problems))

    conn.close()


if __name__ == "__main__":
    main()
