"""
manual_acorns_run.py
---------------------
Run ACORNS (Agglomerative Clustering for ORganising Nested Structures) on
the completed SCOUSE stage-4 output.  Headless — no display required.

    /arc/projects/ALMA-SAILS/structure_id_env/.venv/bin/python3 \\
        alma-sails-codebase/scripts/manual_acorns_run.py \\
        --source Per-emb-2 --line-label "HC3N(10-9)"

Prerequisites: unit must be in state 'stage4_complete'.

Workflow:
  1. Exports the stage-4 SCOUSE fits to acorns_input.dat (via output_ascii_indiv).
  2. Runs Acorns.process() in PPV mode using the stored per-unit parameters.
  3. Saves the cluster tree to <output_dir>/<stem>/acorns_output/acorns_clusters.acorn.
  4. Records n_fitted_components, n_acorns_clusters, and the .acorn path in DB.
  5. Advances state to complete.

ACORNS parameters (stored in parameters_json by manual_scouse_prep.py):
  pixel_size_arcsec  — arcsec per pixel (CDELT2)
  min_radius_arcsec  — spatial linking radius (~half geometric-mean beam)
  velo_link_kms      — velocity linking tolerance (~2 native channels)
  acorns_relax       — relax factors [spatial, velocity]  (default [2.0, 1.0])
  acorns_stop        — stop criterion  (default 3.0)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_casa_prefix = "/opt/casa"
sys.path = [p for p in sys.path if not p.startswith(_casa_prefix)]

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("TQDM_DISABLE", "1")

from alma_ops import db
from alma_ops.config import DB_PATH
from alma_ops.imaging.scouse_utils import (
    advance_scouse_state,
    cube_stem,
    scousepy_config_path,
    update_scouse_payload,
)

import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run ACORNS on SCOUSE stage-4 output."
    )
    parser.add_argument("--source",     required=True)
    parser.add_argument("--line-label", required=True)
    parser.add_argument("--force",      action="store_true",
                        help="Re-run even if acorns_input.dat already exists")
    parser.add_argument("--db-path",    default=str(DB_PATH))
    args = parser.parse_args()

    conn = db.get_connection(args.db_path)
    state_row = db.fetch_one(
        conn,
        "SELECT sps.*, su.cube_fits_path, su.output_dir FROM scouse_pipeline_state sps "
        "JOIN scouse_unit su USING (scouse_unit_id) "
        "WHERE sps.source_name=? AND sps.line_label=?",
        (args.source, args.line_label),
    )
    if state_row is None:
        print(f"[ERROR] No SCOUSE unit for source='{args.source}' line='{args.line_label}'.")
        sys.exit(1)
    if state_row["current_state"] != "stage4_complete":
        print(f"[ERROR] Expected state 'stage4_complete', got '{state_row['current_state']}'.")
        sys.exit(1)

    params          = json.loads(state_row["parameters_json"])
    cube_fits_local = Path(db.to_local(state_row["cube_fits_path"]))
    output_dir      = Path(db.to_local(state_row["output_dir"]))
    stem            = cube_stem(str(cube_fits_local))
    unit_id         = state_row["scouse_unit_id"]
    config          = str(scousepy_config_path(output_dir, stem))

    if not Path(config).exists():
        print(f"[ERROR] scousepy.config not found: {config}")
        sys.exit(1)

    # ACORNS parameters
    pixel_size   = float(params["pixel_size_arcsec"])
    min_radius   = float(params["min_radius_arcsec"])
    velo_link    = float(params["velo_link_kms"])
    relax        = np.array(params.get("acorns_relax", [2.0, 1.0]))
    stop         = float(params.get("acorns_stop", 3.0))
    mask_below   = float(params["mask_below"])
    njobs        = int(params.get("njobs", 4))

    from scousepy import scouse
    from scousepy.io import output_ascii_indiv
    from acorns import Acorns

    stem_dir     = output_dir / stem
    cat_path     = stem_dir / "acorns_input.dat"
    acorns_dir   = stem_dir / "acorns_output"

    if not cat_path.exists() or args.force:
        print(f"[acorns] loading SCOUSE stage-4 object …", flush=True)
        s4file = stem_dir / "stage_4" / "s4.scousepy"
        if not s4file.exists():
            print(f"[ERROR] stage_4/s4.scousepy not found: {s4file}")
            sys.exit(1)
        s = scouse.stage_4(config=config, nocheck=True)
        output_ascii_indiv(s, str(stem_dir) + "/", filename="acorns_input.dat")
        print(f"[acorns] catalogue written → {cat_path}", flush=True)
    else:
        print(f"[acorns] using existing catalogue: {cat_path}", flush=True)

    data = np.loadtxt(str(cat_path), skiprows=1)
    # Columns 1,2,3,4,5,7 = x, y, v_centroid, amplitude, FWHM, component_id
    dataarr = data[:, [1, 2, 3, 4, 5, 7]].T
    n_components = int(dataarr.shape[1])
    print(f"[acorns] {n_components} fitted Gaussian components loaded.", flush=True)

    cluster_criteria = np.array([min_radius, velo_link])
    print(
        f"[acorns] running PPV clustering  "
        f"min_radius={min_radius}\"  velo_link={velo_link} km/s  "
        f"relax={list(relax)}  stop={stop}  n_jobs={njobs}",
        flush=True,
    )

    A = Acorns.process(
        dataarr, cluster_criteria,
        method="PPV", min_height=mask_below,
        pixel_size=pixel_size, relax=relax, stop=stop,
        verbose=True, n_jobs=njobs,
    )

    acorns_dir.mkdir(exist_ok=True)
    acorn_file = acorns_dir / "acorns_clusters.acorn"
    A.save_to(str(acorn_file))
    n_clusters = len(A.clusters)
    print(f"[acorns] done — {n_clusters} clusters → {acorn_file}", flush=True)

    acorn_canonical = db.to_canonical(str(acorn_file))
    update_scouse_payload(
        conn, unit_id,
        n_fitted_components=n_components,
        n_acorns_clusters=n_clusters,
        acorns_output_path=acorn_canonical,
    )

    advance_scouse_state(
        conn, unit_id, args.source, args.line_label,
        from_state="stage4_complete", to_state="complete",
        note=f"n_components={n_components} n_clusters={n_clusters}",
    )
    print(f"[OK] {args.source} / {args.line_label} → complete  "
          f"({n_components} components → {n_clusters} clusters)")
    conn.close()


if __name__ == "__main__":
    main()
