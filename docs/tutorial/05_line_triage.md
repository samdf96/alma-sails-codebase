# Step 5: Line Triage

**Start state:** MOUS `selfcal_complete`  
**End state (per SPW):** line triage unit `complete`

---

## What this step does

Line triage produces a set of coarse, dirty (undeconvolved) spectral cubes — one per candidate line SPW — that serve as input for automated line identification in the next step.

Before imaging, the script classifies each SPW as a line candidate or continuum-only using two criteria:

1. **`cont.dat`** — the continuum channel flags written by auto\_selfcal. An SPW flagged entirely as continuum is skipped.
2. **Native channel width** — coarse correlator modes (wide-channel SPWs used for continuum sensitivity) are skipped.

SPWs that pass both tests are imaged as dirty cubes using the continuum-subtracted MS (`selfcal_products_sub`). The channel width is binned to a practical velocity resolution, with a floor set to avoid smearing broad emission lines.

Each cube gets its own row in the `line_triage_unit` table, which tracks its pipeline state independently.

---

## Running line triage

This step requires CASA.

=== "Manual"

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_line_triage_imaging.py \
        --mous-id <mous-id> \
        --source <source>
    ```

    To image a specific SPW only:

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_line_triage_imaging.py \
        --mous-id <mous-id> \
        --source <source> \
        --spw 3
    ```

    Other options:

    | Argument | Default | Description |
    |---|---|---|
    | `--linewidth-floor-kms` | `0.3` | Narrowest line FWHM (km/s) protected by channel binning |
    | `--taper-arcsec` | off | Image at this uv-tapered resolution instead of full resolution (see [Very large images](#very-large-images)) |
    | `--allow-large` | off | Image above `TRIAGE_MAX_IMSIZE` at full resolution anyway |
    | `--force` | off | Re-image even if the unit is already `complete` |
    | `--dry-run` | off | Print the plan, image size and estimated memory/FITS size, without running tclean or touching the database |

    For a large cube, run CASA with a memory-capping rc file:
    `CASARCFILES=<rc file> casa ...` (see [CASA rc files](../reference/casa_rc.md)).

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run line-triage-imaging \
        --param mous_id=<mous-id> \
        --param source=<source>
    ```

    Optional parameters: `linewidth_floor_kms`, `spw`, `force`, `ram_gb`,
    `taper_arcsec`, `allow_large`, and `casarc` (a CASA rc file set as
    `CASARCFILES` in the session; see [CASA rc files](../reference/casa_rc.md)).

    !!! note
        Unlike most flows, `line_triage_flow` writes nothing to the database before
        submitting the session. The `line_triage_unit` rows for this MOUS only exist
        once the headless session has classified the SPWs from `cont.dat`. All
        line-candidate SPWs get their rows (`ready`) at that point, before any
        imaging, so a session killed partway through leaves the remaining SPWs
        visible as `ready` rather than missing.

---

## Very large images

The triage cube's cell and image size follow the data's resolution and primary
beam. Long-baseline data can make that image enormous: the Serpens 0.015–0.03″
data gave 8640–10000 px images, about 2–3 TB of `tclean` memory and 0.5–0.8 TB
of FITS per SPW. A run refuses any image larger than `TRIAGE_MAX_IMSIZE`
(4096 px, in `alma_ops/imaging/line_triage_config.py`) and suggests a taper.

`--taper-arcsec T` images at a uv-tapered resolution of about `T` arcsec. The
cell grows to match (`T / PIXELS_PER_BEAM`), while the image still covers the
primary beam, so the image shrinks by roughly the square of the resolution
ratio. ADMIT only needs the spectra to identify lines, and deep line imaging
(Step 7) recomputes its own full-resolution geometry, so nothing downstream
loses resolution. The taper, cell and image size are recorded in the unit's
`triage_products` and in its completion note.

Check the plan first with `--dry-run`: it prints the tapered and untapered
geometry and an estimate of memory and FITS size per SPW.

`--allow-large` images at full resolution anyway; pair it with a large pinned
session and a high-memory rc file.

---

## What's on disk afterward

```
imaging/
└── lines/
    └── triage/
        └── <source>/
            └── B<band>_<config>/
                ├── <source>_B<band>_spw<N>_dirty.fits
                └── ...
```

One FITS cube per line-candidate SPW.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT ltu.spw_idx, ltu.spw_freq_ghz, ltps.current_state
   FROM line_triage_unit ltu
   JOIN line_triage_pipeline_state ltps USING (line_triage_unit_id)
   WHERE ltu.mous_id = '<mous-id>'
   AND ltu.target_name = '<source>';"
```

---

## Troubleshooting

Line triage is the most likely stage to fail silently due to out-of-memory kills during `tclean` on large cubes. The symptom is a unit stuck in `running` with no `line_triage_run_log` entries.

Reset a specific SPW:

```bash
python alma-sails-codebase/scripts/reset_line_triage.py \
    --mous-id <mous-id> \
    --source <source> \
    --spw <spw-index>
```

This deletes partial CASA products (`.pb`, `.psf`, `.sumwt`, `.weight`) and resets the unit to `ready`. Already-complete SPWs for the same MOUS/source are left untouched.
