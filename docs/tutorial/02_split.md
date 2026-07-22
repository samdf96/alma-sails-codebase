# Step 2: Split

**Start state:** `downloaded`  
**End state:** `split_complete`

---

## What this step does

The calibrated MS downloaded from SRDP contains all fields observed during the scheduling block — calibrators, check sources, and science targets — and all spectral windows in their native channel widths. The split step produces a compact, target-only MS containing only the science fields and the spectral windows needed for imaging.

This reduces the data volume significantly (calibrator fields alone can double the file size) and is a prerequisite for self-calibration.

---

## Running the split

This step requires CASA.

=== "Manual"

    Run inside a CASA 6 environment:

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_split.py \
        --mous-id <mous-id>
    ```

    The script will:

    1. Confirm the MOUS is in `downloaded` state
    2. Locate the calibrated MS from `mous_directory` in the database
    3. Run `mstransform` to extract target-only spectral windows
    4. Run `listobs` on the output and write a summary to the run log
    5. Record the output paths in `split_products` and transition to `split_complete`

    Output measurement sets are written to `datasets/<mous-id>/splits/`.

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run mous-split \
        --param mous_id=<mous-id>
    ```

    The flow submits a CANFAR headless session running CASA. The session executes `manual_split.py` and writes results to the database.

---

## What's on disk afterward

```
datasets/
└── uid___A001_X1234_X5678/
    └── splits/
        ├── <source>_B<band>_12m_targets.ms
        └── <source>_B<band>_7m_targets.ms
```

One `*_targets.ms` per source and per array configuration. The `split_products` column in the database stores the canonical paths to all of them as a JSON array.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT mous_id, current_state, split_products
   FROM pipeline_state
   WHERE mous_id = '<mous-id>';"
```

Expected: `current_state = split_complete`, `split_products` is a non-empty JSON array of paths.

---

## Troubleshooting

If CASA crashes or the session is killed mid-split, the MOUS may be left with partial `*_targets.ms` files. Remove them and reset:

```bash
# Remove partial splits (check paths in the database first)
rm -rf datasets/uid___A001_X1234_X5678/splits/

# Reset state to downloaded
python alma-sails-codebase/scripts/reset_download.py --mous-id <mous-id>
# (reset_download resets to 'pending'; manually update to 'downloaded' if needed,
#  or re-download and re-split from scratch)
```

!!! note
    If you need to re-split without re-downloading, update the database state manually:
    ```bash
    sqlite3 db/<survey>.db \
      "UPDATE pipeline_state SET current_state='downloaded' WHERE mous_id='<mous-id>';"
    # Also insert a recovery note into state_transitions
    ```
    Then re-run the split command.
