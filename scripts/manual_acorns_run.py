"""
manual_acorns_run.py
---------------------
Run ACORNS (Agglomerative Clustering for ORganising Nested Structures) on
the completed SCOUSE stage-4 output.  Headless — no display required.

Launch it with scouse_run.sh from the project root (it sets LD_LIBRARY_PATH and
uses the structure_id_env venv python):

    alma-sails-codebase/scripts/scouse_run.sh --stage acorns \
        --source S68Nc --line-label "HC3N(10-9)"   # or --unit-id 2

batch_scouse.py runs it as a headless session once stage 4 is complete.

Prerequisites: unit must be in state 'stage4_complete'.

Workflow:
  1. Exports the stage-4 SCOUSE fits to acorns_input.dat (via output_ascii_indiv).
  2. Splits the components at acorns_sigma_split_kms into 'narrow' (quiescent gas) and
     'broad' (mostly outflow-broadened) sets and runs Acorns.process() in PPV mode on
     each set separately. A set with fewer components than ACORNS's minimum cluster
     size (the pixels within min_radius) is skipped and gets no tree.
  3. Saves each tree to <output_dir>/<stem>/acorns_output/acorns_clusters_<set>.acorn.
  4. Replaces the unit's scouse_cluster rows (old rows are deleted first; each row is
     tagged with its component_set, and carries the shape and velocity-gradient columns
     from scouse_utils.cluster_geometry), records the resolved ACORNS parameters in
     parameters_json, and n_fitted_components / n_acorns_clusters (both sets) /
     acorns_output_path (the narrow tree, else the broad one) in scouse_pipeline_state.
  5. Advances state to complete.
  6. Writes the summary PDF (alma_ops.imaging.scouse_summary) to
     <output_dir>/<source>_<line>_scouse_summary.pdf, unless --no-pdf. A PDF failure
     is reported but does not undo the ACORNS results; regenerate it with
     scouse_run.sh --stage pdf.

ACORNS parameters (parameters_json; defaults in alma_ops.imaging.scouse_utils):
  pixel_size_arcsec       — arcsec per pixel (CDELT2)
  min_radius_arcsec       — always recomputed: radius of a circle of one beam area
  velo_link_kms           — velocity linking tolerance (~2 native channels)
  acorns_min_height       — default 3 x mean fitted-amplitude error of the catalogue's real
                            fits (no-fit rows and fits with error > amplitude left out); a
                            resolved value is stored, so clear it to re-derive
  acorns_sigma_split_kms  — narrow/broad split on component sigma (default 0.6 km/s)
  acorns_relax            — relax factors [spatial, velocity]  (default [2.0, 1.0])
  acorns_stop             — stop criterion  (default 3.0)

scousepy's component "width" is the Gaussian sigma, not the FWHM.
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
from alma_ops.imaging.scouse_utils import (
    ACORNS_SIGMA_SPLIT_KMS,
    acorns_min_radius_arcsec,
    add_stage_args,
    advance_scouse_state,
    component_sets,
    cube_stem,
    default_min_height,
    cube_x_east_sign,
    import_scouse,
    insert_scouse_clusters,
    load_stage_unit,
    scouse_connection,
    scousepy_config_path,
    stage_job,
    update_scouse_parameters,
    update_scouse_payload,
)

import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run ACORNS on SCOUSE stage-4 output."
    )
    add_stage_args(parser)
    parser.add_argument("--force",      action="store_true",
                        help="Re-export acorns_input.dat even if it already exists")
    parser.add_argument("--no-pdf",     action="store_true",
                        help="Skip the summary PDF")
    args = parser.parse_args()

    state_row = load_stage_unit(args, "acorns")
    with stage_job(args, state_row, "acorns"):
        run(args, state_row)
    if not args.no_pdf:
        write_pdf(args.db_path, state_row["scouse_unit_id"])


def write_pdf(db_path, unit_id) -> None:
    try:
        from alma_ops.imaging.scouse_summary import write_summary_pdf
        with scouse_connection(db_path) as conn:
            out = write_summary_pdf(conn, unit_id)
        print(f"[acorns] summary PDF → {out}", flush=True)
    except Exception as exc:
        print(f"[acorns] WARNING: summary PDF failed ({type(exc).__name__}: {exc}); the ACORNS "
              f"results are saved. Retry with scouse_run.sh --stage pdf --unit-id {unit_id}",
              flush=True)


def run(args, state_row) -> None:
    source, label   = state_row["source_name"], state_row["line_label"]
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
    min_radius   = acorns_min_radius_arcsec(params["_bmaj_arcsec"], params["_bmin_arcsec"])
    velo_link    = float(params["velo_link_kms"])
    relax        = np.array(params.get("acorns_relax", [2.0, 1.0]))
    stop         = float(params.get("acorns_stop", 3.0))
    sigma_split  = float(params.get("acorns_sigma_split_kms", ACORNS_SIGMA_SPLIT_KMS))
    njobs        = int(params.get("njobs", 4))
    x_east_sign  = cube_x_east_sign(str(cube_fits_local))

    scouse = import_scouse(headless=True)
    from scousepy.io import output_ascii_indiv
    from acorns import Acorns
    from acorns.acorns import latticepoints

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
    # scousepy writes: 0=ncomps 1=x 2=y 3=amplitude 4=err_amplitude 5=shift(velocity)
    #                  6=err_shift 7=width (Gaussian sigma, not FWHM) ...
    # ACORNS requires exactly this row order for PPV (see acorns.py "For PPV data"):
    #   x, y, intensity, error intensity, velocity, ++ any other variables
    # Rows 2 and 4 are intensity and velocity -- do not swap them.
    dataarr = data[:, [1, 2, 3, 4, 5, 7]].T
    n_components = int(dataarr.shape[1])
    min_height = float(params.get("acorns_min_height")
                       or default_min_height(data))
    print(f"[acorns] {n_components} fitted Gaussian components loaded.", flush=True)

    # ACORNS takes min_radius and pixel_size in the units of the data positions, which
    # acorns_input.dat gives in pixels: its linking length and minimum cluster separation
    # are derived from them. Runs before 2026-11-06 passed arcsec, ~15x too short a link.
    min_radius_pix = min_radius / pixel_size
    cluster_criteria = np.array([min_radius_pix, velo_link])
    print(
        f"[acorns] PPV clustering  min_radius={min_radius:.3f}\" ({min_radius_pix:.2f} px)  velo_link={velo_link} km/s  "
        f"min_height={min_height:.5f}  relax={list(relax)}  stop={stop}  n_jobs={njobs}  "
        f"narrow/broad split at sigma={sigma_split} km/s",
        flush=True,
    )

    acorns_dir.mkdir(exist_ok=True)
    # ACORNS's own minimum cluster size: the pixels inside min_radius. A set with fewer
    # components can never form a cluster (and ACORNS crashes on an empty forest).
    min_members = latticepoints(min_radius_pix, 1.0)
    runs = {}
    for name, keep in component_sets(dataarr, sigma_split).items():
        sub = dataarr[:, keep]
        if sub.shape[1] < max(2, min_members):
            print(f"[acorns] {name}: {sub.shape[1]} components, fewer than the {min_members} "
                  f"one cluster needs; not clustered", flush=True)
            continue
        print(f"[acorns] {name}: {sub.shape[1]} components", flush=True)
        A = Acorns.process(
            sub, cluster_criteria,
            method="PPV", min_height=min_height,
            pixel_size=1.0, relax=relax, stop=stop,
            verbose=True, n_jobs=njobs,
        )
        acorn_file = acorns_dir / f"acorns_clusters_{name}.acorn"
        A.save_to(str(acorn_file))
        print(f"[acorns] {name}: {len(A.clusters)} clusters → {acorn_file}", flush=True)
        runs[name] = (A, sub, acorn_file)

    n_clusters = sum(len(A.clusters) for A, _, _ in runs.values())
    params.update(
        min_radius_arcsec=round(min_radius, 4),
        acorns_min_height=round(min_height, 6),
        acorns_sigma_split_kms=sigma_split,
        _acorns_spatial_units="pixels",
        _acorns_outputs={name: db.to_canonical(str(f)) for name, (_, _, f) in runs.items()},
    )
    with scouse_connection(args.db_path) as conn:
        # Clusters from an earlier run would otherwise linger: their ids need not recur.
        conn.execute("DELETE FROM scouse_cluster WHERE scouse_unit_id=?", (unit_id,))
        for name, (A, sub, _) in runs.items():
            n_inserted = insert_scouse_clusters(
                conn, unit_id, source, label,
                acorns_obj=A, dataarr=sub, pixel_size_arcsec=pixel_size,
                x_east_sign=x_east_sign, component_set=name,
            )
            print(f"[acorns] {name}: {n_inserted} cluster rows written to DB.", flush=True)

        update_scouse_parameters(conn, unit_id, params)
        update_scouse_payload(
            conn, unit_id,
            n_fitted_components=n_components,
            n_acorns_clusters=n_clusters,
            # the narrow tree, or the broad one when the narrow set was too small to cluster
            acorns_output_path=(params["_acorns_outputs"].get("narrow")
                                or params["_acorns_outputs"].get("broad")),
        )

        advance_scouse_state(
            conn, unit_id, source, label,
            from_state="stage4_complete", to_state="complete",
            note=(f"n_components={n_components} n_clusters={n_clusters} ("
                  + ", ".join(f"{k} {len(v[0].clusters)}" for k, v in runs.items())
                  + f"; split sigma={sigma_split} km/s, min_height={min_height:.5f})"),
        )
    print(f"[OK] {source} / {label} → complete  "
          f"({n_components} components → {n_clusters} clusters)")


if __name__ == "__main__":
    main()
