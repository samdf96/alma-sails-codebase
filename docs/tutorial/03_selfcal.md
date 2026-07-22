# Step 3: Self-calibration

**Start state:** `split_complete`  
**End state:** `selfcal_complete`

---

## What this step does

Self-calibration uses the science data itself to derive and apply additional phase (and optionally amplitude) corrections beyond what the observatory's pipeline calibration provided. It typically improves image dynamic range and reduces artefacts near bright compact sources — both common in protostellar continuum data.

The pipeline uses **auto\_selfcal**, a PHANGS-derived script that automates the decision logic: it iterates over solution intervals, applies successive phase correction rounds, and stops when the improvement plateaus.

Self-calibration is the most compute-intensive step. Plan for 30 minutes to several hours depending on the MS size and number of fields.

---

## Running selfcal

This step requires CASA (with MPI support for parallel execution).

=== "Manual"

    Run the selfcal script from the project root:

    ```bash
    bash alma-sails-codebase/scripts/manual_selfcal.sh --mous-id <mous-id>
    ```

    The script handles the entire workflow in one invocation:

    1. Copies the split MSes into `datasets/<mous-id>/auto_selfcal/`
    2. Runs `mpicasa` for `findcont` (identifies continuum channels)
    3. Runs `mpicasa` for the selfcal loop (phase → amplitude rounds as warranted)
    4. Extracts `CORRECTED_DATA` from each MS into new, compact MSes
    5. Removes intermediate working data to free disk space
    6. Records selfcal products and transitions to `selfcal_complete`

    !!! tip "MPI parallelism"
        `manual_selfcal.sh` calls `mpicasa` internally. To control the number of MPI
        processes, set `MPICASA_NPROC` before running:
        ```bash
        MPICASA_NPROC=8 bash alma-sails-codebase/scripts/manual_selfcal.sh --mous-id <mous-id>
        ```

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run mous-selfcal \
        --param mous_id=<mous-id> \
        --param ncores=8 \
        --param ram_gb=32
    ```

    The flow writes `selfcal_running` to the database **before** submitting the headless session. This acts as a guard: if the session is killed (OOM, timeout), the MOUS is left in `selfcal_running` rather than an ambiguous state, and you know a recovery step is needed.

    `ncores` and `ram_gb` pin the CANFAR session size. Omit them for flexible scheduling.

---

## What's on disk afterward

```
datasets/
└── uid___A001_X1234_X5678/
    ├── splits/                     # Original split MSes (preserved)
    └── auto_selfcal/
        ├── cont.dat                # Continuum channel ranges (used later by imaging)
        ├── <source>_B<band>_12m_targets_selfcal.ms   # Selfcal-corrected MS
        └── <source>_B<band>_12m_targets_selfcal_sub.ms  # Continuum-subtracted MS
```

The `cont.dat` file is required by all downstream imaging steps — it records which channels contain continuum emission, so that continuum imaging and line subtraction use the same channel selections.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT mous_id, current_state, selfcal_products
   FROM pipeline_state
   WHERE mous_id = '<mous-id>';"
```

Expected: `current_state = selfcal_complete`, `selfcal_products` is a non-empty JSON array.

---

## Troubleshooting

If selfcal is killed or fails, the MOUS may be stuck in `selfcal_running` or `failed`. Use the reset script to restore it to `split_complete`:

```bash
casa --nologger \
    -c alma-sails-codebase/scripts/reset_selfcal.py \
    --mous-id <mous-id>
```

This script:

- Removes `CORRECTED_DATA` from any partially-processed MSes
- Cleans up the `auto_selfcal/` working directory
- Moves MSes back to `splits/`
- Resets state to `split_complete`

After reset, you can re-run selfcal from scratch.
