"""
scouse_summary.py
-----------------
Multi-page PDF summarising one completed SCOUSE + ACORNS unit:

  1. SCOUSE fit maps: number of components, centroid velocity and dispersion of the
     brightest component per pixel, reduced chi-squared; moment-0 contours from stage 1
  2. ACORNS structures: components on the sky coloured by top-level cluster, and the
     two position-velocity projections
  3. Table of the largest top-level clusters: shape and velocity gradient, read from
     scouse_cluster
     (pages 2-3 repeat for the narrow and broad component sets)
  4. Example spectra with the SCOUSE Gaussian components over-plotted

The PDF is written to the unit's output dir and copied into config.SCOUSE_SUMMARY_DIR
(imaging/lines/scouse/summary_pdfs/), the one folder that holds every unit's summary.

Positions are arcsec offsets from the continuum peak inside the cube's footprint. The
continuum image is the unit's own target's 12m robust-0.5 image (B6, then B3, then B7),
else any complete 12m continuum image covering the cube centre.

Needs matplotlib, astropy and acorns, so it runs in the structure_id_env venv (called
at the end of manual_acorns_run.py, or via `scouse_run.sh --stage pdf`).
scousepy's component "width" is the Gaussian sigma, not the FWHM (FWHM = 2.355 sigma).
"""

from __future__ import annotations

import json
import re
import shutil
import warnings
from pathlib import Path

import numpy as np

from alma_ops import db
from alma_ops.config import SCOUSE_SUMMARY_DIR, SURVEY_DISTANCE_PC
from alma_ops.imaging.scouse_utils import cube_stem, fetch_scouse_unit

N_COLOURED = 12
N_TABLE_ROWS = 20
BAND_PREFERENCE = ("6", "3", "7")
_RCPARAMS = {
    "font.family": "serif",
    "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
    "mathtext.fontset": "dejavuserif",
    "font.size": 10,
    "axes.linewidth": 0.5,
    "xtick.direction": "in", "ytick.direction": "in",
    "xtick.top": True, "ytick.right": True,
    "xtick.minor.visible": True, "ytick.minor.visible": True,
    "xtick.major.size": 3, "ytick.major.size": 3,
    "xtick.minor.size": 1.5, "ytick.minor.size": 1.5,
    "figure.autolayout": False,
}


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def offsets(wcs, x, y, centre):
    """Arcsec offsets (east positive) of cube pixels from centre."""
    import astropy.units as u
    sky = wcs.pixel_to_world(x, y)
    dra = (sky.ra - centre.ra).to_value(u.arcsec) * np.cos(centre.dec.radian)
    ddec = (sky.dec - centre.dec).to_value(u.arcsec)
    return dra, ddec


def map_extent(wcs, shape, centre):
    ny, nx = shape
    x0, y0 = offsets(wcs, np.array([-0.5]), np.array([-0.5]), centre)
    x1, y1 = offsets(wcs, np.array([nx - 0.5]), np.array([ny - 0.5]), centre)
    return [float(x0[0]), float(x1[0]), float(y0[0]), float(y1[0])]


def zoom(ax, dra, ddec, pad=3.0):
    """Limit an axis to the region holding fitted components (east to the left)."""
    half = max(np.ptp(dra), np.ptp(ddec)) / 2 + pad
    cx, cy = (dra.max() + dra.min()) / 2, (ddec.max() + ddec.min()) / 2
    ax.set_xlim(cx + half, cx - half)
    ax.set_ylim(cy - half, cy + half)


# ---------------------------------------------------------------------------
# Continuum centre
# ---------------------------------------------------------------------------

def _continuum_candidates(conn, source):
    """(rank, path) for every complete 12m continuum product, own target first."""
    rows = db.fetch_all(
        conn,
        "SELECT iu.target_name, iu.band, ips.imaging_products FROM imaging_unit iu "
        "JOIN imaging_pipeline_state ips USING (imaging_unit_id) "
        "WHERE ips.current_state='complete' AND iu.config_type='12m'",
    )
    out = []
    for r in rows:
        prods = json.loads(r["imaging_products"] or "[]")
        for p in (prods if isinstance(prods, list) else [prods]):
            path = p.get("image_fits")
            if not path:
                continue
            band = str(r["band"])
            rank = (r["target_name"] != source,
                    BAND_PREFERENCE.index(band) if band in BAND_PREFERENCE else len(BAND_PREFERENCE),
                    float(p.get("robust", 0.5)) != 0.5)
            out.append((rank, r["target_name"], Path(db.to_local(path))))
    return sorted(out, key=lambda t: t[0])


def find_continuum(conn, source, cube_centre):
    """Continuum image for the unit: its own target's, else one covering the cube centre."""
    from astropy.io import fits
    from astropy.wcs import WCS

    cands = [c for c in _continuum_candidates(conn, source) if c[2].exists()]
    own = [c for c in cands if c[1] == source]
    if own:
        return own[0][2]
    for _, _, path in cands:
        w = WCS(fits.getheader(path)).celestial
        if w.footprint_contains(cube_centre):
            return path
    return None


def continuum_peak(cont_path, cube_wcs, cube_shape):
    """Sky position of the brightest continuum pixel inside the cube's footprint."""
    from astropy.io import fits
    from astropy.wcs import WCS

    with fits.open(cont_path) as hd:
        d = np.squeeze(hd[0].data).astype(float)
        w = WCS(hd[0].header).celestial
    ny, nx = cube_shape
    corners = cube_wcs.pixel_to_world(np.array([0, nx - 1, 0, nx - 1]), np.array([0, 0, ny - 1, ny - 1]))
    cx, cy = w.world_to_pixel(corners)
    x0, x1 = int(max(np.floor(cx.min()), 0)), int(min(np.ceil(cx.max()), d.shape[1] - 1))
    y0, y1 = int(max(np.floor(cy.min()), 0)), int(min(np.ceil(cy.max()), d.shape[0] - 1))
    box = d[y0:y1 + 1, x0:x1 + 1]
    py, px = np.unravel_index(np.nanargmax(box), box.shape)
    return w.pixel_to_world(px + x0, py + y0)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

class _Pages:
    """PdfPages wrapper that also writes each page as a PNG when png_dir is set."""

    def __init__(self, pdf, png_dir):
        self.pdf, self.png_dir, self.n = pdf, png_dir, 0

    def save(self, fig):
        import matplotlib.pyplot as plt
        self.pdf.savefig(fig)
        self.n += 1
        if self.png_dir is not None:
            fig.savefig(self.png_dir / f"page{self.n:02d}.png", dpi=110)
        plt.close(fig)


def page_fit_maps(pages, title, wcs, centre, comp, stem_dir, extent, mom0):
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from astropy.io import fits

    x, y = comp["x"].astype(int), comp["y"].astype(int)
    ny, nx = mom0.shape
    order = np.lexsort((-comp["amp"], y, x))           # brightest component first per pixel
    first = np.ones(len(order), bool)
    first[1:] = (x[order][1:] != x[order][:-1]) | (y[order][1:] != y[order][:-1])
    b = order[first]
    vmap = np.full((ny, nx), np.nan)
    fmap = np.full((ny, nx), np.nan)
    vmap[y[b], x[b]] = comp["vel"][b]
    fmap[y[b], x[b]] = comp["sig"][b]
    ncomp = np.squeeze(fits.getdata(stem_dir / "stage_4" / "stage_4_ncomps.fits")).astype(float)
    rchi = np.squeeze(fits.getdata(stem_dir / "stage_4" / "stage_4_redchisq.fits")).astype(float)
    ncomp[ncomp <= 0] = np.nan
    rchi[~np.isfinite(ncomp)] = np.nan

    vlo, vhi = np.nanpercentile(comp["vel"], [3, 97])
    nmax = int(min(np.nanmax(ncomp), 7))
    panels = [
        (ncomp, "number of Gaussian components", mpl.colors.ListedColormap(
            ["#0C5DA5", "#00B945", "#FF9500", "#FF2C00", "#845B97", "#474747", "#000000"][:nmax]),
         dict(vmin=0.5, vmax=nmax + 0.5), ""),
        (vmap, "centroid velocity, brightest component", "RdBu_r", dict(vmin=vlo, vmax=vhi), "km/s"),
        (fmap, "velocity dispersion σ, brightest component", "viridis",
         dict(vmin=0, vmax=np.nanpercentile(comp["sig"], 97)), "km/s"),
        (rchi, "reduced χ²", "magma", dict(vmin=0.5, vmax=np.nanpercentile(rchi, 97)), ""),
    ]
    fig, axs = plt.subplots(2, 2, figsize=(10, 9))
    dra, ddec = offsets(wcs, comp["x"], comp["y"], centre)
    for ax, (img, ttl, cmap, kw, unit) in zip(axs.flat, panels):
        im = ax.imshow(img, origin="lower", extent=extent, cmap=cmap, **kw)
        ax.contour(mom0, levels=np.nanmax(mom0) * np.array([0.1, 0.3, 0.6]), colors="k",
                   linewidths=0.4, extent=extent, origin="lower")
        ax.plot(0, 0, "k+", ms=8, mew=1)
        zoom(ax, dra, ddec)
        cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.02)
        if unit:
            cb.set_label(unit, fontsize=8)
        if ttl.startswith("number"):
            cb.set_ticks(list(range(1, nmax + 1)))
            if np.nanmax(ncomp) > nmax:
                cb.set_ticklabels([str(i) for i in range(1, nmax)] + [f"≥{nmax}"])
        ax.set_title(ttl, fontsize=9)
        ax.set_xlabel("ΔRA (arcsec)", fontsize=8)
        ax.set_ylabel("ΔDec (arcsec)", fontsize=8)
        ax.tick_params(labelsize=7)
    n = np.isfinite(ncomp).sum()
    multi = (ncomp >= 2).sum() / max(n, 1)
    fig.suptitle(f"{title}: SCOUSE decomposition, {len(comp['x'])} Gaussian components in {n} spectra "
                 f"({100 * multi:.0f}% need ≥2)\n+ = continuum peak, contours = moment 0 at 10/30/60% of peak",
                 fontsize=10)
    fig.subplots_adjust(left=0.07, right=0.95, bottom=0.06, top=0.92, wspace=0.3, hspace=0.25)
    pages.save(fig)


def cluster_assignment(A, n):
    """Top-level cluster id per component (-1 = not in any cluster), and the top-level ids
    largest first."""
    lab = np.full(n, -1)
    top = [cid for cid, c in A.clusters.items() if c.antecedent is None]
    top.sort(key=lambda cid: -A.clusters[cid].number_of_members)
    for cid in top:
        lab[np.asarray(A.clusters[cid].cluster_members, int)] = cid
    return lab, top


def page_acorns(pages, title, wcs, centre, comp, lab, top, extent, mom0, distance_pc):
    import matplotlib.pyplot as plt

    dra, ddec = offsets(wcs, comp["x"], comp["y"], centre)
    colours = plt.get_cmap("tab20")(np.linspace(0, 1, 20))[:N_COLOURED]
    fig = plt.figure(figsize=(11, 8.2))
    gs = fig.add_gridspec(2, 2, width_ratios=[1.15, 1], left=0.07, right=0.97, bottom=0.07, top=0.89,
                          wspace=0.25, hspace=0.3)
    ax_sky = fig.add_subplot(gs[:, 0])
    ax_rv = fig.add_subplot(gs[0, 1])
    ax_dv = fig.add_subplot(gs[1, 1])
    views = ((ax_sky, dra, ddec), (ax_rv, dra, comp["vel"]), (ax_dv, ddec, comp["vel"]))
    grey = lab < 0
    others = np.isin(lab, top[N_COLOURED:])
    for ax, xs, ys in views:
        ax.scatter(xs[grey], ys[grey], s=1, c="0.85", lw=0, rasterized=True)
        ax.scatter(xs[others], ys[others], s=1, c="0.55", lw=0, rasterized=True)
    for k, cid in enumerate(top[:N_COLOURED]):
        m = lab == cid
        for ax, xs, ys in views:
            ax.scatter(xs[m], ys[m], s=2, color=colours[k], lw=0, rasterized=True)
        ax_sky.text(np.median(dra[m]), np.median(ddec[m]), str(cid), fontsize=7, fontweight="bold",
                    ha="center", va="center", color="k",
                    bbox=dict(boxstyle="round,pad=0.15", fc=colours[k], ec="none", alpha=0.85))
    ax_sky.contour(mom0, levels=np.nanmax(mom0) * np.array([0.1, 0.3, 0.6]), colors="k", linewidths=0.3,
                   extent=extent, origin="lower")
    ax_sky.plot(0, 0, "k+", ms=9, mew=1.2)
    zoom(ax_sky, dra, ddec)
    ax_sky.set_xlabel("ΔRA (arcsec)")
    ax_sky.set_ylabel("ΔDec (arcsec)")
    L = 1000 / distance_pc
    xl, yl = ax_sky.get_xlim(), ax_sky.get_ylim()
    x0 = xl[1] + 0.08 * (xl[0] - xl[1])
    y0 = yl[0] + 0.05 * (yl[1] - yl[0])
    ax_sky.plot([x0 + L, x0], [y0, y0], "k-", lw=1.5)
    ax_sky.text(x0 + L / 2, y0, "1000 au\n", ha="center", va="bottom", fontsize=7)
    ax_sky.set_title(f"ACORNS: {len(top)} top-level structures (largest {N_COLOURED} coloured, numbered)",
                     fontsize=9)
    ax_rv.set_xlim(*ax_sky.get_xlim())
    ax_dv.set_xlim(*ax_sky.get_ylim()[::-1])
    ax_dv.invert_xaxis()
    ax_rv.set_xlabel("ΔRA (arcsec)")
    ax_dv.set_xlabel("ΔDec (arcsec)")
    for ax in (ax_rv, ax_dv):
        ax.set_ylabel(r"$v_{\rm LSRK}$ (km/s)")
        ax.set_ylim(*np.nanpercentile(comp["vel"], [0.5, 99.5]))
        ax.tick_params(labelsize=7)
    ax_rv.set_title("position-velocity (RA)", fontsize=9)
    ax_dv.set_title("position-velocity (Dec)", fontsize=9)
    fig.suptitle(f"{title}\nACORNS hierarchical clustering of the SCOUSE components "
                 f"(grey = not in any structure)", fontsize=10)
    pages.save(fig)


def _fmt(x, spec):
    return "" if x is None else format(x, spec)


def page_table(pages, title, wcs, centre, comp, lab, top, clusters, distance_pc):
    import matplotlib.pyplot as plt

    arcsec_pc = distance_pc * 4.8481e-6
    rows_db = {c["acorns_cluster_id"]: c for c in clusters}
    dra, ddec = offsets(wcs, comp["x"], comp["y"], centre)
    header = ["ID", "N comp", "⟨v⟩", "v range", "σ(v)", "⟨σ⟩", "peak\n(Jy/beam)", "length\n(\")",
              "width\n(\")", "axis\nratio", "PA\n(°)", "∇v\n(km/s/pc)", "∇v PA\n(°)", "∇v vs\naxis (°)",
              "∇v\nR²", "offset\n(\")", "sub-\nstructures"]
    cells = []
    for cid in top[:N_TABLE_ROWS]:
        m = lab == cid
        c = rows_db[cid]
        g = c["vgrad_kms_per_arcsec"]
        nsub = sum(1 for cc in clusters if cc["parent_acorns_cluster_id"] == cid)
        cells.append([cid, c["n_components"], f"{c['mean_vel_kms']:.2f}",
                      f"{c['vel_range_min_kms']:.1f}–{c['vel_range_max_kms']:.1f}", f"{c['vel_disp_kms']:.2f}",
                      f"{c['mean_sigma_kms']:.2f}", f"{c['peak_amplitude_jybeam']:.3f}",
                      _fmt(c["major_length_arcsec"], ".1f"), _fmt(c["minor_width_arcsec"], ".1f"),
                      _fmt(c["aspect_ratio"], ".1f"), _fmt(c["position_angle_deg"], ".0f"),
                      _fmt(None if g is None else g / arcsec_pc, ".1f"), _fmt(c["vgrad_pa_deg"], ".0f"),
                      _fmt(c["vgrad_misalign_deg"], ".0f"), _fmt(c["vgrad_r2"], ".2f"),
                      f"{np.hypot(np.median(dra[m]), np.median(ddec[m])):.1f}", nsub])
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis("off")
    if cells:
        tab = ax.table(cellText=cells, colLabels=header, loc="upper center", cellLoc="center")
        tab.auto_set_font_size(False)
        tab.set_fontsize(7)
        tab.scale(1, 1.5)
        for (r, _), cell in tab.get_celld().items():
            cell.set_linewidth(0.3)
            if r == 0:
                cell.set_facecolor("#E8E8E8")
    ax.set_title(f"{title}: largest top-level ACORNS structures (velocities in km/s)", fontsize=11, pad=10)
    fig.text(0.05, 0.035,
             "All statistics are over the structure's Gaussian components, unweighted (a pixel fitted with n "
             "Gaussians counts n times). σ(v): spread of component centroid velocities; ⟨σ⟩: mean fitted "
             "dispersion (FWHM = 2.355σ). Length/width: 5th–95th percentile range of component positions along "
             "the major/minor axis (≈0.9× the full length of a uniformly filled filament). Axis ratio: "
             "σ_major/σ_minor. PA: major axis, east of north (0–180°). ∇v: least-squares plane "
             "v = v0 + a·ΔRA + b·ΔDec; ∇v PA (east of north) points toward increasing velocity; ∇v vs axis: "
             "angle between the gradient and the major axis (0 = along it, 90 = across it); R²: fraction of the "
             "velocity variance the plane explains. Offset: median component position from the continuum peak. "
             f"Distance {distance_pc:g} pc. Values from scouse_cluster "
             "(alma_ops.imaging.scouse_utils.cluster_geometry).",
             fontsize=6.5, wrap=True)
    pages.save(fig)


def page_spectra(pages, title, cube_path, comp, wcs, centre, ncomp_map):
    import matplotlib.pyplot as plt
    from astropy.io import fits

    with fits.open(cube_path, memmap=True) as hd:
        h = hd[0].header
        d = hd[0].data
        n = h["NAXIS3"]
        rf = h.get("RESTFRQ", h.get("RESTFREQ"))
        f = h["CRVAL3"] + (np.arange(n) + 1 - h["CRPIX3"]) * h["CDELT3"]
        v = 299792.458 * (rf - f) / rf
        # positions: continuum peak, brightest component, and the most complex spectra
        cx, cy = (int(round(float(t))) for t in wcs.world_to_pixel(centre))
        picks = [(cx, cy, "continuum peak")]
        kb = int(np.argmax(comp["amp"]))
        picks.append((int(comp["x"][kb]), int(comp["y"][kb]), "brightest component"))
        rng = np.random.default_rng(1)
        ys, xs = np.where(np.nan_to_num(ncomp_map) >= np.nanmax(ncomp_map))
        for i in rng.choice(len(xs), size=min(2, len(xs)), replace=False):
            picks.append((int(xs[i]), int(ys[i]), f"{int(np.nanmax(ncomp_map))} components"))
        ys, xs = np.where(np.nan_to_num(ncomp_map) == 2)
        for i in rng.choice(len(xs), size=min(2, len(xs)), replace=False):
            picks.append((int(xs[i]), int(ys[i]), "2 components"))
        fig, axs = plt.subplots(2, 3, figsize=(11, 6.5))
        for ax, (px, py, why) in zip(axs.flat, picks):
            spec = np.asarray(d[0, :, py, px] if d.ndim == 4 else d[:, py, px], dtype=float)
            ax.step(v, spec, where="mid", color="k", lw=0.6)
            m = (comp["x"].astype(int) == px) & (comp["y"].astype(int) == py)
            model = np.zeros_like(v)
            for a, c0, sg in zip(comp["amp"][m], comp["vel"][m], comp["sig"][m]):
                g = a * np.exp(-0.5 * (v - c0) ** 2 / sg ** 2)
                ax.plot(v, g, lw=0.8)
                model += g
            ax.plot(v, model, color="#FF2C00", lw=1.0, ls="--")
            vv = comp["vel"][m]
            lo, hi = (vv.min() - 6, vv.max() + 6) if m.any() else (np.nanmedian(comp["vel"]) - 8,
                                                                   np.nanmedian(comp["vel"]) + 8)
            ax.set_xlim(lo, hi)
            o = offsets(wcs, np.array([px]), np.array([py]), centre)
            ax.set_title(f"{why}: ({o[0][0]:+.1f}\", {o[1][0]:+.1f}\"), {int(m.sum())} comp.", fontsize=8)
            ax.set_xlabel(r"$v_{\rm LSRK}$ (km/s)", fontsize=8)
            ax.set_ylabel("Jy/beam", fontsize=8)
            ax.tick_params(labelsize=7)
        for ax in list(axs.flat)[len(picks):]:
            ax.axis("off")
    fig.suptitle(f"{title}: example spectra with SCOUSE Gaussian components (dashed = total model)",
                 fontsize=10)
    fig.subplots_adjust(left=0.07, right=0.98, bottom=0.09, top=0.9, wspace=0.3, hspace=0.45)
    pages.save(fig)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def summary_pdf_path(output_dir: Path, source: str, line_label: str) -> Path:
    """Default location: <output_dir>/<source>_<line>_scouse_summary.pdf."""
    safe = re.sub(r"[^A-Za-z0-9+.\-]+", "_", f"{source}_{line_label}").strip("_")
    return output_dir / f"{safe}_scouse_summary.pdf"


def write_summary_pdf(conn, scouse_unit_id: int, out_path: Path | None = None,
                      cont_path: Path | None = None, png: bool = False,
                      distance_pc: float = SURVEY_DISTANCE_PC) -> Path:
    """Write the summary PDF for a complete unit and return its path.

    Parameters
    ----------
    conn:
        Open DB connection (read only; the caller may close it as soon as this returns).
    scouse_unit_id:
        The unit; it must have finished ACORNS.
    out_path:
        PDF path (default: summary_pdf_path in the unit's output dir, and then also
        copied into config.SCOUSE_SUMMARY_DIR; an explicit path is not copied).
    cont_path:
        Continuum FITS whose peak defines (0, 0) (default: find_continuum).
    png:
        Also write each page as a PNG into <pdf stem>_png/.
    distance_pc:
        Distance for the au scale bar and km/s/pc gradients.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib as mpl
    from astropy.io import fits
    from astropy.wcs import WCS
    from matplotlib.backends.backend_pdf import PdfPages
    from acorns import Acorns

    warnings.filterwarnings("ignore")
    mpl.rcParams.update(_RCPARAMS)

    unit = fetch_scouse_unit(conn, unit_id=scouse_unit_id)
    if unit is None:
        raise ValueError(f"no SCOUSE unit {scouse_unit_id}")
    if unit["current_state"] != "complete":
        raise ValueError(f"unit {scouse_unit_id} is '{unit['current_state']}', not complete")
    source, label = unit["source_name"], unit["line_label"]
    params = json.loads(unit["parameters_json"])
    clusters = db.fetch_all(conn, "SELECT * FROM scouse_cluster WHERE scouse_unit_id=? "
                                  "ORDER BY n_components DESC", (scouse_unit_id,))
    cube = Path(db.to_local(unit["cube_fits_path"]))
    output_dir = Path(db.to_local(unit["output_dir"]))
    stem = cube_stem(str(cube))
    stem_dir = output_dir / stem

    wcs = WCS(fits.getheader(cube)).celestial
    mom0 = np.squeeze(fits.getdata(stem_dir / "stage_1" / f"{stem}_mom0.fits")).astype(float)
    ny, nx = mom0.shape
    if cont_path is None:
        cont_path = find_continuum(conn, source, wcs.pixel_to_world((nx - 1) / 2, (ny - 1) / 2))
    if cont_path is None:
        raise FileNotFoundError(f"no continuum image covers {source}; pass a continuum FITS")
    centre = continuum_peak(cont_path, wcs, mom0.shape)

    raw = np.loadtxt(stem_dir / "acorns_input.dat", skiprows=1)
    comp = {"x": raw[:, 1], "y": raw[:, 2], "amp": raw[:, 3], "vel": raw[:, 5], "sig": raw[:, 7]}
    extent = map_extent(wcs, mom0.shape, centre)
    ncomp_map = np.squeeze(fits.getdata(stem_dir / "stage_4" / "stage_4_ncomps.fits")).astype(float)

    split = float(params["acorns_sigma_split_kms"])
    sets = {"narrow": comp["sig"] <= split, "broad": comp["sig"] > split}
    trees = {k: v for k, v in params["_acorns_outputs"].items() if k in sets}
    runs = []
    for name, path in trees.items():
        A = Acorns.load_from(db.to_local(path))
        sub = {k: v[sets[name]] for k, v in comp.items()}
        lab, top = cluster_assignment(A, len(sub["x"]))
        rows = [c for c in clusters if c["component_set"] == name]
        set_label = {"narrow": f"narrow components (σ ≤ {split} km/s)",
                     "broad": f"broad components (σ > {split} km/s)"}[name]
        runs.append((set_label, sub, lab, top, rows))

    share = out_path is None
    out_path = Path(out_path) if out_path else summary_pdf_path(output_dir, source, label)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    png_dir = None
    if png:
        png_dir = out_path.with_name(out_path.stem + "_png")
        png_dir.mkdir(exist_ok=True)
    title = f"{source} {label}"
    with PdfPages(out_path) as pdf:
        pages = _Pages(pdf, png_dir)
        page_fit_maps(pages, title, wcs, centre, comp, stem_dir, extent, mom0)
        for set_label, sub, lab, top, rows in runs:
            page_acorns(pages, f"{title}, {set_label}", wcs, centre, sub, lab, top, extent, mom0, distance_pc)
            page_table(pages, f"{title}, {set_label}", wcs, centre, sub, lab, top, rows, distance_pc)
        page_spectra(pages, title, cube, comp, wcs, centre, ncomp_map)
    print(f"[scouse-summary] {len(raw)} components; "
          + "; ".join(f"{s}: {len(top)} top-level" for s, _, _, top, _ in runs)
          + f"; centre = continuum peak of {Path(cont_path).name}", flush=True)
    if share:
        SCOUSE_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(out_path, SCOUSE_SUMMARY_DIR / out_path.name)
        print(f"[scouse-summary] copied to {SCOUSE_SUMMARY_DIR / out_path.name}", flush=True)
    return out_path
