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
    | `--linewidth-floor-kms` | `1.0` | Minimum velocity resolution after binning |
    | `--force` | off | Re-image even if the unit is already `complete` |
    | `--dry-run` | off | Print the plan without running tclean |

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run line-triage-imaging \
        --param mous_id=<mous-id> \
        --param source=<source>
    ```

    !!! note
        Unlike most flows, `line_triage_flow` writes nothing to the database before
        submitting the session. The `line_triage_unit` rows for this MOUS only exist
        once the headless session has classified the SPWs from `cont.dat`.

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
