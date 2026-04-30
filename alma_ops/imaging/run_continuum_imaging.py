"""
run_continuum_imaging.py
------------------------
CASA driver script for ALMA-SAILS continuum imaging.

Called from run_imaging.sh, which launches it inside a headless CASA session:

    casa --nologger --nogui -c run_continuum_imaging.py <payload.json>

The script reads a JSON payload file passed as the first command-line argument
(sys.argv[1] when launched via 'casa -c <script> <args>'), performs continuum
imaging of the provided measurement sets using tclean, runs impbcor, and
exports a FITS file.  The imaging_state DB record is updated to 'complete' or
'error' at the end.

JSON payload schema
-------------------
{
    "source_name":  "EC_53",
    "band":         "6",
    "config_type":  "12m",          // "12m", "7m", or "tp"
    "field_name":   "EC_53",        // field name as it appears in cont.dat
    "imaging_dir":  "/arc/.../imaging/Band6/EC_53_12m/",
    "db_path":      "/arc/.../db/serpens_main.db",
    "robust":       0.5,            // Briggs robust parameter
    "mous_list": [
        {
            "mous_id":       "uid://A001/...",
            "ms_path":       "/arc/.../<eb>_targets.ms",
            "cont_dat_path": "/arc/.../auto_selfcal/cont.dat"
        },
        ...
    ]
}

Output (all written inside imaging_dir)
-------
<source_name>_B<band>_<config_type>_cont.image         — tclean image cube
<source_name>_B<band>_<config_type>_cont.pb             — primary beam
<source_name>_B<band>_<config_type>_cont.image.pbcor    — PB-corrected image
<source_name>_B<band>_<config_type>_cont.fits           — exported FITS
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_db(db_path: str, source_name: str, band: str, config_type: str,
               status: str, error_msg: str | None = None) -> None:
    """Update the imaging_state row for this job."""
    try:
        conn = sqlite3.connect(db_path)
        fields = {"status": status, "updated_at": _now_iso()}
        if error_msg is not None:
            fields["last_error_message"] = error_msg[:4096]  # guard against giant tracebacks
        set_clause = ", ".join(f"{k} = :{k}" for k in fields)
        fields["source_name"] = source_name
        fields["band"] = band
        fields["config_type"] = config_type
        conn.execute(
            f"UPDATE imaging_state SET {set_clause} "
            "WHERE source_name = :source_name AND band = :band AND config_type = :config_type",
            fields,
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"[WARNING] Could not update DB: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: casa -c run_continuum_imaging.py <payload.json>", flush=True)
        sys.exit(1)

    payload_path = sys.argv[1]
    with open(payload_path) as fh:
        payload = json.load(fh)

    source_name:  str        = payload["source_name"]
    band:         str        = payload["band"]
    config_type:  str        = payload["config_type"]
    field_name:   str        = payload["field_name"]
    imaging_dir:  str        = payload["imaging_dir"]
    db_path:      str        = payload["db_path"]
    robust:       float      = float(payload.get("robust", 0.5))
    mous_list:    list[dict] = payload["mous_list"]

    print(
        f"[imaging] {source_name}  Band{band}  {config_type}  "
        f"robust={robust}  n_mous={len(mous_list)}",
        flush=True,
    )

    # Mark as running
    _update_db(db_path, source_name, band, config_type, "running")

    # Create imaging directory
    os.makedirs(imaging_dir, exist_ok=True)

    try:
        _run_imaging(
            source_name=source_name,
            band=band,
            config_type=config_type,
            field_name=field_name,
            imaging_dir=imaging_dir,
            robust=robust,
            mous_list=mous_list,
        )
    except Exception:
        err = traceback.format_exc()
        print(f"[ERROR]\n{err}", flush=True)
        _update_db(db_path, source_name, band, config_type, "error", err)
        sys.exit(1)

    _update_db(db_path, source_name, band, config_type, "complete")
    print("[imaging] done.", flush=True)


def _run_imaging(
    source_name: str,
    band: str,
    config_type: str,
    field_name: str,
    imaging_dir: str,
    robust: float,
    mous_list: list[dict],
) -> None:
    """Core imaging logic — runs inside CASA."""

    # These imports only work inside a CASA session
    from casatasks import tclean, impbcor, exportfits  # type: ignore[import]

    from alma_ops.imaging.cont_dat_utils import parse_cont_dat, cont_dat_to_spw_string
    from alma_ops.imaging.imaging_utils import compute_imaging_params

    # ------------------------------------------------------------------
    # 1. Build per-MS SPW selection strings from cont.dat
    # ------------------------------------------------------------------
    vis_list: list[str] = []
    spw_list: list[str] = []

    for entry in mous_list:
        ms_path       = entry["ms_path"]
        cont_dat_path = entry["cont_dat_path"]

        print(f"  [cont.dat] {Path(cont_dat_path).parent.name}", flush=True)
        cont_ranges = parse_cont_dat(cont_dat_path, field_name)

        if not cont_ranges:
            print(
                f"  [WARNING] No cont.dat entries for field '{field_name}' in "
                f"{cont_dat_path} — skipping this MS.",
                flush=True,
            )
            continue

        spw_str = cont_dat_to_spw_string(ms_path, cont_ranges)
        if not spw_str:
            print(
                f"  [WARNING] cont_dat_to_spw_string returned empty string for "
                f"{ms_path} — skipping.",
                flush=True,
            )
            continue

        vis_list.append(ms_path)
        spw_list.append(spw_str)
        print(f"    spw selection: {spw_str}", flush=True)

    if not vis_list:
        raise ValueError(
            f"No usable MS/cont.dat pairs found for {source_name} Band{band} {config_type}."
        )

    # ------------------------------------------------------------------
    # 2. Compute cell size and image size from metadata
    # ------------------------------------------------------------------
    print("  [params] computing cell size and image size …", flush=True)
    cell_str, imsize = compute_imaging_params(vis_list, robust=robust)
    print(f"    cell={cell_str}  imsize={imsize}", flush=True)

    # ------------------------------------------------------------------
    # 3. Image name base (all tclean products share this prefix)
    # ------------------------------------------------------------------
    img_base = os.path.join(
        imaging_dir,
        f"{source_name}_B{band}_{config_type}_cont",
    )

    # Multiscale clean scales: 0 (point), 1× beam, 3× beam, 9× beam (in pixels)
    # pixels_per_beam is 5 by construction; these give sensible scale progression.
    multiscale_scales = [0, 5, 15, 45]

    # ------------------------------------------------------------------
    # 4. tclean — dirty image first (niter=0) to assess noise, then deep
    # ------------------------------------------------------------------
    print("  [tclean] starting …", flush=True)

    # tclean accepts vis and spw as parallel lists when vis is a list
    tclean(
        vis=vis_list,
        spw=spw_list,
        imagename=img_base,
        field=field_name,
        specmode="mfs",
        deconvolver="multiscale",
        scales=multiscale_scales,
        nterms=1,
        gridder="standard",
        imsize=[imsize, imsize],
        cell=cell_str,
        weighting="briggs",
        robust=robust,
        pbcor=False,             # we apply impbcor separately for the FITS export
        phasecenter="",          # use the MS phase centre
        niter=50000,
        gain=0.1,
        cyclefactor=2.0,
        threshold="0.0mJy",      # run to niter; auto-masking handles stopping
        usemask="auto-multithresh",
        # auto-multithresh defaults are appropriate for ALMA continuum
        sidelobethreshold=2.0,
        noisethreshold=4.25,
        lownoisethreshold=1.5,
        minbeamfrac=0.3,
        negativethreshold=0.0,
        interactive=False,
        savemodel="none",
    )

    print("  [tclean] finished.", flush=True)

    # ------------------------------------------------------------------
    # 5. Primary beam correction
    # ------------------------------------------------------------------
    pbcor_image = img_base + ".image.pbcor"
    print("  [impbcor] applying primary beam correction …", flush=True)
    impbcor(
        imagename=img_base + ".image",
        pbimage=img_base + ".pb",
        outfile=pbcor_image,
        overwrite=True,
    )

    # ------------------------------------------------------------------
    # 6. Export FITS
    # ------------------------------------------------------------------
    fits_path = img_base + ".fits"
    print(f"  [exportfits] → {fits_path}", flush=True)
    exportfits(
        imagename=pbcor_image,
        fitsimage=fits_path,
        dropdeg=True,
        dropstokes=True,
        overwrite=True,
    )

    print(f"  [done] products in {imaging_dir}", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main()
