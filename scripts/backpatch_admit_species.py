#!/usr/bin/env python3
"""
backpatch_admit_species.py
--------------------------
Cross-match Unknown line_admit_detections against the survey's local
tier1_lines.list and update species_name / formula / uid for any
confident matches.

Run from the VM shell (no CASA needed):

    python3 alma-sails-codebase/scripts/backpatch_admit_species.py --dry-run
    python3 alma-sails-codebase/scripts/backpatch_admit_species.py --dry-run --species SO2 CCS
    python3 alma-sails-codebase/scripts/backpatch_admit_species.py

Background
----------
ADMIT's line identification uses two paths:
  1. A hardcoded Tier 1 SQLite database (~20 common envelope species) in
     admit/etc/transitions.db -- these are always identified.
  2. CASA's slsearch (bundled Splatalogue snapshot) with include_only_nrao=True
     for all other lines.  This filter is highly conservative: many
     well-characterised transitions are in Splatalogue but lack the NRAO
     "recommended" tag in the bundled catalog version, and are therefore
     returned as Unknown.

This script re-matches existing Unknown detections (species_name='Unknown')
against all species shorthands present in admit/etc/tier1_lines.list, using
the same ±40 km/s frequency window that ADMIT's own LineID_AT.identify()
applies for sources with |vlsr| < 150 km/s (LineID_AT.py:1517-1520).

Matching methodology
--------------------
  - frequency_ghz in line_admit_detection is already in the source rest
    frame: ADMIT converts via the stored VLSR before writing to the lltable.
  - Catalog entries (from tier1_lines.list) are at laboratory rest
    frequencies.  A detection at f_det matches a catalog entry at f_cat when
    |f_det - f_cat| / f_cat * c < tolerance_kms.
  - Default tolerance: 40.0 km/s (ADMIT default for |vlsr| < 150 km/s).
  - If multiple catalog lines fall within the window, the nearest is taken,
    mirroring ADMIT's noskip/minval selection (LineID_AT.py:1538-1542).

When a match is found, three columns are updated:
  - formula      <- shorthand from tier1_lines.list (e.g. "SO2")
  - species_name <- shorthand
  - uid          <- "{shorthand}_{catalog_freq:.5f}"  (ADMIT's naming
                    convention for identified lines; replaces "U_{freq:.4f}")

By default all species shorthands present in tier1_lines.list are used as
the catalog.  Use --species to restrict to a subset.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alma_ops import db
from alma_ops.config import ADMIT_ROOT, DB_PATH

TIER1_LIST = ADMIT_ROOT / "etc" / "tier1_lines.list"

C_KMS        = 299_792.458  # km/s
TOLERANCE_KMS = 40.0        # ADMIT default for |vlsr| < 150 km/s

SEP = "-" * 120


# ── Catalog ───────────────────────────────────────────────────────────────────

def load_catalog(species_filter: set[str] | None = None) -> dict[str, list[float]]:
    """Return {shorthand: [freq_ghz, ...]} from tier1_lines.list.

    If species_filter is given, only those shorthands are loaded.  Otherwise
    all shorthands found in the file are returned.
    """
    catalog: dict[str, list[float]] = {}

    with open(TIER1_LIST) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                freq = float(parts[0])
            except ValueError:
                continue
            shorthand = parts[1]
            if species_filter is not None and shorthand not in species_filter:
                continue
            catalog.setdefault(shorthand, []).append(freq)

    return catalog


# ── Matching ──────────────────────────────────────────────────────────────────

def best_match(
    detected_freq: float,
    catalog: dict[str, list[float]],
    tolerance_kms: float = TOLERANCE_KMS,
) -> tuple[str, float, float] | None:
    """Return (shorthand, catalog_freq_ghz, velocity_offset_kms) or None.

    velocity_offset sign convention matches ADMIT's freqtovel:
    positive = detected frequency redshifted relative to catalog.
    """
    best: tuple[str, float, float] | None = None
    best_abs_dv: float | None = None
    fwidth = detected_freq * (tolerance_kms / C_KMS)

    for shorthand, freqs in catalog.items():
        for cfreq in freqs:
            if abs(detected_freq - cfreq) < fwidth:
                dv = (detected_freq - cfreq) / cfreq * C_KMS
                abs_dv = abs(dv)
                if best is None or abs_dv < best_abs_dv:
                    best = (shorthand, cfreq, dv)
                    best_abs_dv = abs_dv

    return best


# ── Database ──────────────────────────────────────────────────────────────────

def get_unknowns(conn) -> list:
    return db.fetch_all(
        conn,
        """
        SELECT lad.line_admit_detection_id,
               lad.frequency_ghz,
               lad.peakrms_sigma,
               laps.mous_id,
               laps.target_name,
               laps.spw_idx
        FROM   line_admit_detection lad
        JOIN   line_admit_pipeline_state laps
               ON lad.line_admit_unit_id = laps.line_admit_unit_id
        WHERE  lad.species_name = 'Unknown'
        ORDER  BY lad.frequency_ghz
        """,
    )


def apply_update(conn, detection_id: int, shorthand: str, catalog_freq: float) -> None:
    uid = f"{shorthand}_{catalog_freq:.5f}"
    conn.execute(
        """
        UPDATE line_admit_detection
        SET    formula      = ?,
               species_name = ?,
               uid          = ?
        WHERE  line_admit_detection_id = ?
        """,
        (shorthand, shorthand, uid, detection_id),
    )


# ── Output ────────────────────────────────────────────────────────────────────

def print_matches(matches: list[tuple], dry_run: bool) -> None:
    tag = "[DRY RUN] " if dry_run else ""
    if not matches:
        print(f"{tag}No matches found.")
        return
    hdr = (f"  {'id':>6}  {'detected(GHz)':>14}  {'catalog(GHz)':>14}  "
           f"{'Δv(km/s)':>9}  {'species':12}  {'σ':>6}  "
           f"{'mous_id':22}  {'source':20}  spw")
    print(hdr)
    print("  " + SEP)
    for det_id, det_freq, cat_freq, dv, shorthand, snr, mous_id, source, spw in matches:
        print(f"  {det_id:>6}  {det_freq:>14.6f}  {cat_freq:>14.6f}  "
              f"{dv:>+9.3f}  {shorthand:12}  {snr:>6.2f}  "
              f"{mous_id:22.22}  {source:20.20}  {spw}")


def print_species_summary(matches: list[tuple]) -> None:
    from collections import Counter
    counts = Counter(m[4] for m in matches)
    print("\n  Matches per species:")
    for sp, n in sorted(counts.items()):
        print(f"    {sp:14} {n:4d}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Back-patch Unknown ADMIT detections against tier1_lines.list."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show proposed changes without writing to the database",
    )
    parser.add_argument(
        "--species", nargs="+", metavar="SHORTHAND",
        help="Limit matching to specific shorthands (default: all in tier1_lines.list)",
    )
    parser.add_argument(
        "--tolerance-kms", type=float, default=TOLERANCE_KMS,
        help=f"Matching window in km/s (default: {TOLERANCE_KMS}, ADMIT default for |vlsr|<150)",
    )
    parser.add_argument("--db-path", default=str(DB_PATH))
    args = parser.parse_args()

    tag = "[DRY RUN] " if args.dry_run else ""

    species_filter = set(args.species) if args.species else None

    if not TIER1_LIST.exists():
        print(f"[ERROR] tier1_lines.list not found at {TIER1_LIST}")
        sys.exit(1)

    catalog = load_catalog(species_filter)
    if not catalog:
        if species_filter:
            print(f"[ERROR] None of the requested species found in tier1_lines.list: "
                  f"{', '.join(sorted(species_filter))}")
        else:
            print("[ERROR] tier1_lines.list appears empty or has no parseable lines")
        sys.exit(1)

    total_entries = sum(len(v) for v in catalog.values())
    print(f"[backpatch-admit] {tag}catalog: {total_entries} lines across "
          f"{len(catalog)} species from {TIER1_LIST.name}")
    if args.species:
        for sp in sorted(catalog):
            print(f"  {sp:14} {len(catalog[sp]):5d} lines")
    print(f"[backpatch-admit] tolerance: ±{args.tolerance_kms:.1f} km/s")
    print()

    conn = db.get_connection(args.db_path)
    rows = get_unknowns(conn)
    print(f"[backpatch-admit] {len(rows)} Unknown detections in line_admit_detection")
    print()

    matches: list[tuple] = []
    for row in rows:
        result = best_match(row["frequency_ghz"], catalog, args.tolerance_kms)
        if result is None:
            continue
        shorthand, cat_freq, dv = result
        matches.append((
            row["line_admit_detection_id"],
            row["frequency_ghz"],
            cat_freq,
            dv,
            shorthand,
            row["peakrms_sigma"],
            row["mous_id"],
            row["target_name"],
            row["spw_idx"],
        ))

    print(f"{tag}{len(matches)} of {len(rows)} Unknown detections match a catalog species:")
    print()
    print_matches(matches, args.dry_run)
    if matches:
        print_species_summary(matches)

    if not args.dry_run and matches:
        print(f"\n[backpatch-admit] applying {len(matches)} update(s) …")
        for m in matches:
            det_id, _, cat_freq, _, shorthand, *_ = m
            apply_update(conn, det_id, shorthand, cat_freq)
        conn.commit()
        print(f"[OK] {len(matches)} detection(s) updated.")
    elif args.dry_run:
        print(f"\n[DRY RUN] No changes written.  Re-run without --dry-run to apply.")

    conn.close()


if __name__ == "__main__":
    main()
