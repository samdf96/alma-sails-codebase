"""
imaging_payload.py
-------------------
Build the JSON payload consumed by headless_continuum_imaging.py.

Lives in alma_ops rather than in flows/ because two callers need it: the
Prefect flow (imaging_flow.py) and batch_continuum_reimage.py, which launches
CANFAR headless sessions directly without a Prefect server. Duplicating the
MOUS/cont.dat/mosaic resolution in both would be a correctness hazard -- the
is_mosaic determination in particular is subtle (see the comment inline).
"""

from __future__ import annotations

import json
from typing import Optional

from alma_ops import db
from alma_ops.config import CONTINUUM_IMAGING_DIR


def build_imaging_payload(
    imaging_unit_id: int,
    source: str,
    band: str,
    config: str,
    robust: float,
    run_id: str,
    ncores: Optional[int],
    db_path: str,
) -> dict:
    """Query the DB and assemble the JSON payload for headless_continuum_imaging.py."""
    conn = db.get_connection(db_path)

    mous_rows = db.get_mous_for_imaging_unit(conn, imaging_unit_id)
    mous_list = []
    skipped = []
    is_mosaic = False

    for row in mous_rows:
        mous_id  = row["mous_id"]
        ms_paths = db.get_selfcal_products(conn, mous_id, continuum_subtracted=False)
        mous_dir = db.get_mous_directory(conn, mous_id)

        if not ms_paths:
            skipped.append(f"{mous_id} (no selfcal products)")
            continue

        cont_dat = mous_dir / "auto_selfcal" / "cont.dat" if mous_dir else None
        if cont_dat is None or not cont_dat.exists():
            skipped.append(f"{mous_id} (cont.dat missing)")
            continue

        # source_field_name/field_ids are usually NULL -- only set for imaging
        # units that select a subset of fields out of a shared-name mosaic
        # MOUS (see migrate_add_field_selection_columns.py).
        source_field_name = row["source_field_name"]
        field_ids = json.loads(row["field_ids"]) if row["field_ids"] else None

        mous_list.append({
            "mous_id":           mous_id,
            "ms_paths":          [db.to_canonical(str(p)) for p in ms_paths],
            "cont_dat_path":     db.to_canonical(str(cont_dat)),
            "source_field_name": source_field_name,
            "field_ids":         field_ids,
        })

        # Always look up is_mosaic via the imaging unit's own target_name.
        # source_field_name is the parent MOUS's MS field name (e.g.
        # "Serpens_Main_and_Serpens_South") — inheriting its mosaic flag would
        # incorrectly mark a single extracted field as a mosaic and cause
        # compute_mosaic_imaging_params to span the full parent footprint.
        target_row = next(
            (
                r for r in db.get_mous_targets(conn, mous_id)
                if r["alma_source_name"] == source
            ),
            None,
        )
        if target_row is not None and target_row["is_mosaic"] == "T":
            is_mosaic = True

    conn.close()

    if skipped:
        for s in skipped:
            print(f"  [WARNING] [{source} B{band} {config}] Skipped MOUS: {s}", flush=True)

    if not mous_list:
        raise ValueError(
            f"No usable MOUS entries for {source} B{band} {config} — "
            "check selfcal_products and cont.dat in DB."
        )

    imaging_dir = db.to_canonical(
        str(CONTINUUM_IMAGING_DIR / source / f"B{band}_{config}")
    )

    parallel = ncores is not None and ncores > 1
    payload = {
        "imaging_unit_id": imaging_unit_id,
        "field_name":      source,
        "imaging_dir":     imaging_dir,
        "db_path":         db.to_canonical(db_path),
        "robust":          robust,
        "run_id":          run_id,
        "mous_list":       mous_list,
        "ncores":          ncores or 1,
        "parallel":        parallel,
        "is_mosaic":       is_mosaic,
    }

    print(f"  [payload] {source} B{band} {config}: {len(mous_list)} MOUS(es), "
          f"is_mosaic={is_mosaic}", flush=True)
    return payload


# tclean writes far more than the two FITS files recorded in imaging_products.
# Every one of these carries a coordinate system and will abort a re-run whose
# parameters differ, so all of them must go -- deleting only the recorded
# products leaves .pb/.psf/.residual behind and the next run dies on those
# instead.
_CONT_CASA_SUFFIXES = (
    ".image", ".image.pbcor", ".image.fits", ".image.pbcor.fits",
    ".mask", ".model", ".pb", ".psf", ".residual", ".sumwt", ".weight",
    ".alpha", ".beta", ".tt0", ".tt1",
)


def continuum_img_base(imaging_dir, field_name: str, band, config_type: str,
                       robust: float) -> str:
    """Image basename tclean writes to -- must match imaging_utils exactly."""
    import os

    return os.path.join(str(imaging_dir),
                        f"{field_name}_B{band}_{config_type}_rob{robust:.1f}_cont")


def clear_existing_products(conn, imaging_unit_id: int, robust: float,
                            label: str = "",
                            field_name: str = None, band=None,
                            config_type: str = None, imaging_dir=None) -> int:
    """Delete recorded products for one (imaging unit, robust) and drop the DB entry.

    tclean refuses to write over existing CASA-format images whose coordinate
    system differs from the current parameters:

        There is a coordinate system mismatch between existing images on disk
        and current parameters (The SpectralCoordinates have differing
        reference values)

    Re-imaging after the TOPO/LSRK frame fix selects different channels, which
    moves the mfs reference frequency, so every pre-fix product trips this. The
    old products must go before a re-run, not merely be overwritten.

    Shared by imaging_flow.py's validate task and batch_continuum_reimage.py --
    the latter launches CANFAR sessions directly and does not go through the
    flow's validation, so without this it submits jobs that fail on arrival.
    """
    from pathlib import Path

    import shutil

    from alma_ops import db as _db

    import json as _json

    row = conn.execute(
        "SELECT imaging_products FROM imaging_pipeline_state WHERE imaging_unit_id=?",
        (imaging_unit_id,),
    ).fetchone()
    entries = _json.loads((row["imaging_products"] if row else None) or "[]")
    if isinstance(entries, dict):
        entries = [entries]
    matching = [e for e in entries if e.get("robust") == robust]
    removed = 0
    for entry in matching:
        for key in ("image_pbcor_fits", "image_fits", "image_pbcor", "image"):
            path = entry.get(key)
            if not path:
                continue
            p = Path(_db.to_local(path))
            if p.exists():
                shutil.rmtree(str(p)) if p.is_dir() else p.unlink()
                removed += 1
    # Sweep the full tclean product set off the image base, not just what was
    # recorded. .pb/.psf/.residual are never recorded but still block a re-run.
    if field_name is not None and imaging_dir is not None:
        base = continuum_img_base(Path(_db.to_local(str(imaging_dir))),
                                  field_name, band, config_type, robust)
        for suffix in _CONT_CASA_SUFFIXES:
            p = Path(base + suffix)
            if p.exists():
                shutil.rmtree(str(p)) if p.is_dir() else p.unlink()
                removed += 1

    if matching:
        _db.remove_imaging_products_entry(conn, imaging_unit_id, robust)
    if removed and label:
        print(f"  [force] {label}: removed {removed} existing product(s) "
              f"for robust={robust}", flush=True)
    return removed
