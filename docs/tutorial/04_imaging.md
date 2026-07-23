# Step 4: Continuum Imaging

**Start state (imaging unit):** `ready`  
**End state (imaging unit):** `complete`

---

## What this step does

Continuum imaging deconvolves the calibrated visibilities to produce a primary-beam-corrected FITS image of the dust continuum emission. The pipeline uses CASA's `tclean` with auto-multithresh masking, followed by `impbcor` for primary beam correction and `exportfits` to produce science-ready FITS files. For a breakdown of auto-multithresh parameters by dataset type, see the [CASA Automasking Guide](https://casaguides.nrao.edu/index.php/Automasking_Guide_CASA_6.5.4).

One imaging unit groups all observations of the same source in the same band and configuration type — so if two separate MOUSes both observed `<source>` in Band 6 with 12m arrays, they are imaged together in one `tclean` call.

---

## Before imaging: promote units to `ready`

Imaging units are created when the database is seeded, but they start in `pending` state. A unit becomes eligible for imaging only once **all** of its contributing MOUSes have reached `selfcal_complete`. Run the promotion script to find and advance eligible units:

```bash
python alma-sails-codebase/scripts/mark_imaging_ready.py
```

Check which units are now ready:

```bash
sqlite3 db/<survey>.db \
  "SELECT target_name, band, config_type
   FROM imaging_pipeline_state
   WHERE current_state = 'ready';"
```

---

## Imaging a single unit

=== "Manual"

    Run inside a CASA 6 environment:

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_continuum_imaging.py \
        --source <source> \
        --band <band> \
        --config <config>
    ```

    **Serpens example:**
    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_continuum_imaging.py \
        --source EC_53 --band 6 --config 12m
    ```

    Optional arguments:

    | Argument | Default | Description |
    |---|---|---|
    | `--robust` | `0.5` | Briggs robust weighting |
    | `--dry-run` | off | Print the imaging plan without running tclean |
    | `--keep-casa-products` | off | Keep `.image`, `.pb`, `.psf` etc. after FITS export |

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run continuum-imaging \
        --param mous_id=<mous-id> \
        --param source=<source> \
        --param band=<band> \
        --param config=<config>
    ```

    Optional: add `--param ncores=4` to request a parallel `tclean` run via `mpicasa`.

---

## Batch imaging

To image multiple units in one run, use the batch script. It selects units at random from those in `ready` state:

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/batch_continuum_imaging.py \
    --count 5
```

Exclude specific targets:

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/batch_continuum_imaging.py \
    --count 10 \
    --exclude <source1> <source2>
```

The `--dry-run` flag prints the selected units without running anything.

---

## What's on disk afterward

```
imaging/
└── continuum/
    └── <source>/
        └── B<band>_<config>/
            ├── <source>_B<band>_<config>_robust0.5.image.fits
            └── <source>_B<band>_<config>_robust0.5.image.pbcor.fits
```

The primary-beam-corrected image (`.pbcor.fits`) is the science product. The uncorrected image is kept for reference (useful for source detection, which should be done on a flat-noise image).

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT target_name, band, config_type, current_state
   FROM imaging_pipeline_state
   WHERE target_name = '<source>';"
```

---

## Re-imaging at a different robust value

A unit in `complete` state can be re-imaged with a different robust weighting. The batch script detects this automatically: it includes `complete` units in its candidate pool if they have not yet been imaged at the requested `--robust` value. Each imaging run appends a new entry to `imaging_products` rather than overwriting.
