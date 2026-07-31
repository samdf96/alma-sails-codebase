#!/usr/bin/env python3
"""
audit_continuum_products.py
============================
Cross-checks every imaging unit in the DB against files on disk for
both robust 0.5 and robust 2.0 continuum products.

Run from project root (VM shell):
    python3 alma-sails-codebase/scripts/audit_continuum_products.py

Optional flags:
    --ready-only      Only show units that need action (suppress complete rows)
    --launch-cmds     Print tclean launch commands for all units needing work
    --db-path PATH    Path to serpens_v2.db (default: alma_ops.config.DB_PATH)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import CONTINUUM_IMAGING_DIR, DB_PATH
from alma_ops.db import to_local


def _imaging_dir(target: str, band: str, config: str) -> Path:
    return CONTINUUM_IMAGING_DIR / target / f"B{band}_{config}"


def _pbcor_fits(target: str, band: str, config: str, robust: float) -> Path:
    rob_str = f"rob{robust}"
    fname = f"{target}_B{band}_{config}_{rob_str}_cont.image.pbcor.fits"
    return _imaging_dir(target, band, config) / fname


def _build_fits_index(root: Path) -> set:
    """Single directory walk to collect all FITS paths under root."""
    existing = set()
    if not root.is_dir():
        return existing
    for dirpath, _, fnames in os.walk(root):
        for fname in fnames:
            if fname.endswith(".fits"):
                existing.add(Path(dirpath) / fname)
    return existing


def _check_on_disk(target: str, band: str, config: str, robust: float,
                   existing: set) -> bool:
    return _pbcor_fits(target, band, config, robust) in existing


def _robustness_from_products(products_json: str | None) -> set:
    if not products_json:
        return set()
    try:
        entries = json.loads(products_json)
        return {e["robust"] for e in entries if "robust" in e}
    except (json.JSONDecodeError, TypeError):
        return set()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--ready-only", action="store_true",
                        help="Suppress rows where both rob0.5 and rob2.0 are done")
    parser.add_argument("--launch-cmds", action="store_true",
                        help="Print tclean launch commands for units needing work")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to serpens_v2.db")
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)
    rows = conn.execute("""
        SELECT iu.target_name, iu.band, iu.config_type,
               ips.current_state, ips.imaging_products
        FROM imaging_unit iu
        JOIN imaging_pipeline_state ips USING (imaging_unit_id)
        WHERE iu.config_type != 'tp'
        ORDER BY iu.target_name, iu.band, iu.config_type
    """).fetchall()
    conn.close()

    existing = _build_fits_index(CONTINUUM_IMAGING_DIR)

    results = []
    for r in rows:
        target = r["target_name"]
        band   = r["band"]
        config = r["config_type"]
        state  = r["current_state"]

        db_robustness = _robustness_from_products(r["imaging_products"])
        disk_05 = _check_on_disk(target, band, config, 0.5, existing)
        disk_20 = _check_on_disk(target, band, config, 2.0, existing)

        results.append({
            "target": target, "band": band, "config": config, "state": state,
            "db_05":  0.5 in db_robustness, "db_20":  2.0 in db_robustness,
            "disk_05": disk_05, "disk_20": disk_20,
        })

    total = len(results)
    db_state_counts: dict[str, int] = {}
    for r in results:
        db_state_counts[r["state"]] = db_state_counts.get(r["state"], 0) + 1

    done_05   = sum(1 for r in results if r["disk_05"])
    done_20   = sum(1 for r in results if r["disk_20"])
    done_both = sum(1 for r in results if r["disk_05"] and r["disk_20"])

    need_05          = [r for r in results if not r["disk_05"] and r["state"] in ("ready", "complete")]
    need_20          = [r for r in results if r["disk_05"] and not r["disk_20"]]
    db_ok_disk_miss  = [r for r in results if r["db_05"] and not r["disk_05"]]
    failed           = [r for r in results if r["state"] == "failed"]
    pending          = [r for r in results if r["state"] == "pending"]
    ready            = [r for r in results if r["state"] == "ready"]

    W = 90
    print("=" * W)
    print("  ALMA-SAILS  —  Continuum Imaging Audit")
    print("=" * W)
    print(f"\n  Total imaging units : {total}")
    print("  DB state breakdown  :")
    for s, n in sorted(db_state_counts.items()):
        print(f"    {s:<12}  {n}")
    print(f"\n  Disk product counts :")
    print(f"    rob 0.5  on disk  : {done_05:>4}  / {total}")
    print(f"    rob 2.0  on disk  : {done_20:>4}  / {total}")
    print(f"    both complete     : {done_both:>4}  / {total}")

    if need_05:
        print(f'\n{"─"*W}')
        print(f"  ROB 0.5 STILL NEEDED  ({len(need_05)} units — DB says ready/complete but no FITS on disk)")
        print(f'{"─"*W}')
        _print_table(need_05, show_disk_05=True, show_disk_20=False)

    if need_20:
        print(f'\n{"─"*W}')
        print(f"  ROB 2.0 STILL NEEDED  ({len(need_20)} units — rob0.5 done, rob2.0 not yet run)")
        print(f'{"─"*W}')
        _print_table(need_20, show_disk_05=False, show_disk_20=True)

    if db_ok_disk_miss:
        print(f'\n{"─"*W}')
        print(f"  DB/DISK MISMATCH  ({len(db_ok_disk_miss)} units — DB records rob0.5 but FITS not found)")
        print(f'{"─"*W}')
        _print_table(db_ok_disk_miss)

    if failed:
        print(f'\n{"─"*W}')
        print(f"  FAILED  ({len(failed)} units)")
        print(f'{"─"*W}')
        _print_table(failed)

    first_run_ready = [r for r in ready if not r["disk_05"]]
    if first_run_ready:
        print(f'\n{"─"*W}')
        print(f"  READY TO IMAGE  ({len(first_run_ready)} units — DB=ready, no rob0.5 yet)")
        print(f'{"─"*W}')
        _print_table(first_run_ready)

    if pending:
        print(f'\n{"─"*W}')
        print(f"  PENDING  ({len(pending)} units — waiting on selfcal or mark_imaging_ready)")
        print(f'{"─"*W}')
        _print_table(pending)

    if not args.ready_only:
        print(f'\n{"─"*W}')
        print("  FULL TABLE")
        print(f'{"─"*W}')
        _print_table(results, show_disk_05=True, show_disk_20=True, show_state=True)

    if args.launch_cmds:
        print(f'\n{"─"*W}')
        print("  LAUNCH COMMANDS  (c-casa container)")
        print(f'{"─"*W}')
        if first_run_ready:
            print("\n  # --- Rob 0.5 (first run) ---")
            for r in first_run_ready:
                print(f"  casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_continuum_imaging.py "
                      f"--source {r['target']} --band {r['band']} --config {r['config']} --robust 0.5")
        if need_20:
            print("\n  # --- Rob 2.0 (second run) ---")
            for r in need_20:
                print(f"  casa --nologger --nologfile -c alma-sails-codebase/scripts/manual_continuum_imaging.py "
                      f"--source {r['target']} --band {r['band']} --config {r['config']} --robust 2.0")

    print(f'\n{"="*W}\n')


def _print_table(
    rows: list[dict],
    show_disk_05: bool = True,
    show_disk_20: bool = True,
    show_state: bool = False,
) -> None:
    Y, N = "✓", "✗"
    header_parts = [f'{"SOURCE":<35}  {"BAND":>4}  {"CONFIG":<8}']
    if show_state:
        header_parts.append(f'{"DB STATE":<12}')
    if show_disk_05:
        header_parts.append(f'{"ROB0.5":>8}')
    if show_disk_20:
        header_parts.append(f'{"ROB2.0":>8}')
    print("  " + "  ".join(header_parts))
    print("  " + "  ".join("─" * len(h) for h in header_parts))
    for r in rows:
        parts = [f"{r['target']:<35}  {r['band']:>4}  {r['config']:<8}"]
        if show_state:
            parts.append(f"{r['state']:<12}")
        if show_disk_05:
            parts.append(f"{'  ' + Y if r['disk_05'] else '  ' + N:>8}")
        if show_disk_20:
            parts.append(f"{'  ' + Y if r['disk_20'] else '  ' + N:>8}")
        print("  " + "  ".join(parts))


if __name__ == "__main__":
    main()
