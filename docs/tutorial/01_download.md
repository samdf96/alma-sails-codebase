# Step 1: Download

**Start state:** `pending`  
**End state:** `downloaded`

---

## What this step does

The pipeline downloads the calibrated Measurement Set from the [ALMA Science Ready Data Products (SRDP)](https://science.nrao.edu/srdp/) service. SRDP provides a URL from which the calibrated MS and associated products (weblogs, QA2 reports) are retrieved and recorded in the database.

After this step, the MOUS directory exists under `datasets/<mous-id>/` and contains the raw calibrated data ready for splitting.

---

## What you need

- The MOUS UID (e.g., `uid://A001/X1234/X5678`)
- The SRDP download URL for that MOUS

The SRDP URL is retrieved from your submitted jobs on the SRDP portal.

---

## Running the download

=== "Manual"

    The download script runs in a standard Python environment (no CASA required):

    ```bash
    python alma-sails-codebase/scripts/manual_download.py \
        <mous-id> \
        <srdp-url>
    ```

    **Serpens example:**
    ```bash
    python alma-sails-codebase/scripts/manual_download.py \
        "uid://A001/X1234/X5678" \
        "https://almascience.nrao.edu/dataPortal/..."
    ```

    The script will:

    1. Confirm the MOUS is in `pending` state
    2. Retrieve the data from the SRDP URL
    3. Write the `mous_directory` path to the database
    4. Transition the MOUS to `downloaded`

=== "Prefect (CANFAR)"

    Trigger the `mous-download` deployment from the Prefect UI or CLI:

    ```bash
    prefect deployment run mous-download \
        --param mous_id=<mous-id> \
        --param download_url=<srdp-url>
    ```

    The flow validates the MOUS state, then submits a CANFAR headless session that runs `manual_download.py` and reports back to the database.

---

## What's on disk afterward

```
datasets/
└── uid___A001_X1234_X5678/
    ├── <calibrated-ms>/        # The raw calibrated MS
    ├── weblog/                 # CASA pipeline weblog
    └── ...
```

!!! note
    The MOUS directory name uses underscores in place of colons and slashes (CASA convention). The database stores this path in canonical form; the pipeline translates it to your local filesystem automatically.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT mous_id, current_state, mous_directory
   FROM pipeline_state
   WHERE mous_id = '<mous-id>';"
```

Expected output: `current_state = downloaded` and a non-null `mous_directory`.

---

## Troubleshooting

If the download fails partway, the MOUS may be left in `pending` with a partial directory. Delete the partial directory and re-run.

```bash
python alma-sails-codebase/scripts/reset_download.py --mous-id <mous-id>
```

This resets state to `pending` without touching the database path, so you can re-run the download command.
