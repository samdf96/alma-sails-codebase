"""
line_admit_utils.py
---------------------
Utilities for running ADMIT (ALMA Data Mining Toolkit) line identification
on a completed line-triage cube. Must be called from inside a CASA session
(imports the admit package, which needs casatools/casatasks/casaviewer).

The flow (run_admit_lineid) mirrors admit/recipes/Archive_SpecLine.py, built
by hand rather than via admit.recipe("Archive_SpecLine", ...) -- that
convenience call looks for admit/scripts/Archive_SpecLine.py, which doesn't
exist in this checkout (the recipe lives in admit/recipes/ instead). See
admit_local_patches.diff at the project root for the local Python-3
compatibility fixes this ADMIT checkout needs -- without them, nothing here
will run.

Plot generation is disabled (PlotControl.NOPLOT): casaviewer's imview() task
can't launch its background render process in a headless container (no
display to sync with), and the LineList/LineCube/Moment BDPs are what this
pipeline actually needs, not the PNG plots.

VLSR must be passed explicitly (from targets.vlsr_kms) rather than left to
ADMIT's own auto-detection: Ingest_AT's fallback (avt.vlsr(srcname)) is
ADMIT's internal table of its own demo/test source names, not a real
velocity catalog, and produced a ~20 km/s error on this survey's targets.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import sys
from pathlib import Path

from alma_ops import db
from alma_ops.config import ADMIT_ROOT

TCLEAN_LINECUBE_PAD = 50  # native channels of baseline context kept on each side of a cut line cube
LINEID_NUMSIGMA = 6.0
LINEID_MINCHAN = 5


def bootstrap_admit_path() -> None:
    """Put ADMIT_ROOT on sys.path and set the ADMIT env var, idempotent."""
    admit_root_str = str(ADMIT_ROOT)
    if admit_root_str not in sys.path:
        sys.path.insert(0, admit_root_str)
    os.environ.setdefault("ADMIT", admit_root_str)


def run_admit_lineid(cube_fits, vlsr_kms: float, projdir=None) -> dict:
    """Run Ingest -> CubeStats -> CubeSum -> CubeSpectrum -> LineID -> LineCube
    -> Moment on one triage cube.

    Parameters
    ----------
    cube_fits:
        Path to the triage cube FITS file (local, not canonical).
    vlsr_kms:
        Source VLSR in km/s, from targets.vlsr_kms. Passed to both Ingest_AT
        (lands in the cube's own header) and LineID_AT (used directly for
        identification's velocity math).
    projdir:
        ADMIT project directory. Defaults to <cube_fits without extension>.admit,
        matching admit.Project's own default naming.

    Returns
    -------
    dict
        {"projdir": str, "lltable_path": str}
    """
    bootstrap_admit_path()
    import admit
    import admit.util.PlotControl as PlotControl

    cube_fits = str(cube_fits)
    projdir = str(projdir) if projdir is not None else os.path.splitext(cube_fits)[0] + ".admit"

    # Never resume an existing project directory -- admit.Project() restores
    # from admit.xml if one is found, and that restore path needed multiple
    # bug fixes during initial development and remains fragile (most
    # recently: "required nodes missing from the xml file"). A fresh
    # Ingest_AT run is cheap, so always start clean rather than risk hitting
    # that path again. This also protects against leftover project
    # directories from manual/exploratory runs colliding with this same
    # default path.
    if os.path.isdir(projdir):
        shutil.rmtree(projdir)

    p = admit.Project(projdir, commit=False)
    p.plotparams(PlotControl.NOPLOT)

    p.addtask(admit.Ingest_AT(alias="incube", file=cube_fits, vlsr=vlsr_kms))
    p.addtask(admit.CubeStats_AT(alias="instats"), ["incube"])
    p.addtask(admit.CubeSum_AT(sigma=1, numsigma=3.0, alias="insum"), ["incube", "instats"])
    p.addtask(admit.CubeSpectrum_AT(alias="spec1"), ["incube", "insum"])
    p.addtask(
        admit.LineID_AT(
            csub=[1, None], minchan=LINEID_MINCHAN, numsigma=LINEID_NUMSIGMA,
            vlsr=vlsr_kms, alias="lines",
        ),
        ["instats", "spec1"],
    )
    p.addtask(admit.LineCube_AT(alias="cutcubes", pad=TCLEAN_LINECUBE_PAD), ["incube", "lines"])
    p.addtask(
        admit.Moment_AT(alias="linemom", mom0clip=3.0, numsigma=[1.5, 3.0, 3.0], moments=[0, 1, 2]),
        ["cutcubes", "instats"],
    )

    p.run()

    matches = glob.glob(os.path.join(projdir, "lltable.*.json"))
    if not matches:
        raise RuntimeError(f"No lltable.*.json found in {projdir} after ADMIT run")
    if len(matches) > 1:
        raise RuntimeError(f"Expected exactly one lltable.*.json in {projdir}, found {matches}")

    return {"projdir": projdir, "lltable_path": matches[0]}


def parse_lltable(lltable_path) -> list:
    """Parse an ADMIT lltable.N.json into a list of dicts matching
    db.insert_line_admit_detections' expected keys."""
    with open(lltable_path) as f:
        data = json.load(f)

    detections = []
    for line in data["linetable"]["lines"]:
        detections.append({
            "frequency": float(line["frequency"]),
            "uid": line["uid"],
            "formula": line.get("formula"),
            "name": line.get("name"),
            "transition": line.get("transition"),
            "velocity": float(line["velocity"]),
            "peakrms": float(line["peakrms"]),
            "peakintensity": float(line["peakintensity"]),
            "startchan": int(line["startchan"]),
            "endchan": int(line["endchan"]),
            "blend": int(line["blend"]),
        })
    return detections


def purge_incube_copy(projdir) -> bool:
    """Delete Ingest_AT's duplicate copy of the input cube (*-incube.im) from
    an ADMIT project directory. Safe to run unconditionally once a run has
    completed: measured at ~99% of the original triage cube's size in every
    case tested, and the source triage cube is untouched by ADMIT, so this
    copy is pure duplication. Leaves the small *-incube.im.bdp XML sidecar
    in place (needed for the project's own restore bookkeeping).

    Returns True if a copy was found and deleted.
    """
    projdir = Path(projdir)
    deleted = False
    for match in projdir.glob("*-incube.im"):
        shutil.rmtree(match, ignore_errors=True)
        deleted = True
    return deleted


def purge_triage_cube_if_reviewed(conn, line_admit_unit_id: int) -> bool:
    """Delete the original triage FITS cube once every detection tied to
    this line_admit_unit has a non-'pending' review_status. Unlike
    purge_incube_copy, this is NOT called automatically by the main run --
    it touches the actual source data (not a duplicate), so it's invoked
    explicitly, e.g. from scripts/review_line_detections.py after review.

    Returns True if the cube was deleted.
    """
    detections = db.get_line_admit_detections(conn, line_admit_unit_id)
    if not detections or any(d["review_status"] == "pending" for d in detections):
        return False

    admit_state = db.get_line_admit_pipeline_state(conn, line_admit_unit_id)
    triage_state = db.get_line_triage_pipeline_state(conn, admit_state["line_triage_unit_id"])
    triage_products = json.loads(triage_state["triage_products"])[-1]
    cube_fits = Path(db.to_local(triage_products["cube_fits"]))

    if cube_fits.exists():
        cube_fits.unlink()
        return True
    return False
