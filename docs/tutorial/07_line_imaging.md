# Step 7: Deep Line Imaging

**Start state:** detection `review_status = confirmed`  
**End state:** line imaging unit `complete`

---

## What this step does

Deep line imaging produces a fully-cleaned spectral cube of a specific confirmed line transition. Unlike the dirty triage cubes used for line identification, this step runs `tclean` with `niter > 0` and auto-multithresh masking to deconvolve the emission properly.

The frequency window for the cube is derived automatically from the confirmed detection: the pipeline inverts the ADMIT frequency transforms to find the correct sky-frequency range, then pads by a multiple of the detection's empirical linewidth.

Each confirmed detection gets exactly one `line_imaging_unit`. If you want to re-image with different parameters (robust, padding), use `--force` to re-run.

---

## Confirm a detection first

Line imaging requires the detection to be confirmed. If you have not done so yet:

```bash
python review_line_detections.py --confirm <detection-id>
```

Find the detection ID:

```bash
sqlite3 db/<survey>.db \
  "SELECT detection_id, species_name, frequency_ghz, peakrms_sigma, review_status
   FROM line_admit_detection
   WHERE review_status IN ('pending', 'confirmed');"
```

---

## Running deep line imaging

This step requires CASA.

=== "Manual"

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_line_imaging.py \
        --detection-id <detection-id>
    ```

    Options:

    | Argument | Default | Description |
    |---|---|---|
    | `--robust` | `0.5` | Briggs robust weighting |
    | `--ncores` | `1` | Number of MPI processes for parallel tclean |
    | `--force` | off | Re-image even if already complete |
    | `--dry-run` | off | Print the plan without running tclean |
    | `--keep-casa-products` | off | Keep `.image`, `.pb`, `.psf` after FITS export |

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run line-imaging \
        --param detection_id=<detection-id>
    ```

    Optional: `--param ncores=4` for a parallel `tclean` run.

---

## What's on disk afterward

```
imaging/
└── lines/
    └── final/
        └── <source>/
            └── B<band>_<config>/
                ├── <source>_<species>_<freq>GHz_robust0.5.image.fits
                └── <source>_<species>_<freq>GHz_robust0.5.image.pbcor.fits
```

The primary-beam-corrected cube (`.pbcor.fits`) is the science product.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT liu.line_imaging_unit_id, d.species_name, lips.current_state,
          liu.imaging_products
   FROM line_imaging_unit liu
   JOIN line_admit_detection d USING (line_admit_detection_id)
   JOIN line_imaging_pipeline_state lips USING (line_imaging_unit_id)
   WHERE d.detection_id = <detection-id>;"
```

---

## Troubleshooting

If `tclean` is killed (OOM or timeout), partial CASA products will be left on disk. Reset the unit:

```bash
python alma-sails-codebase/scripts/reset_line_imaging.py \
    --detection-id <detection-id>
```

This removes the partial products and resets the unit to `ready`.

---

## What comes next

With fully-cleaned continuum and line images in hand, the data are ready for science analysis. Common next steps include:

- **Spectral decomposition** — fitting line profiles with SCOUSE to identify kinematic structures
- **Moment maps** — zeroth, first, and second moment images of the detected lines
- **Multi-configuration combination** — if the source was observed in both 12m and 7m configurations, combining them improves sensitivity to extended emission
