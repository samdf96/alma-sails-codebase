# Step 6: Line Identification

**Start state:** triage unit `complete`  
**End state:** admit unit `complete`

---

## What this step does

Line identification runs [ADMIT](https://admit.astro.umd.edu) (ALMA Data Mining Toolkit) on each dirty triage cube to identify spectral lines automatically. ADMIT fits the cube statistics, sums the emission, and runs its `LineID_AT` algorithm to match peaks against a spectral line catalog.

Each detected line is written to the `line_admit_detection` table with:

- Species name and rest frequency
- Peak signal-to-noise ratio
- Channel range
- A blend flag if multiple lines overlap

Detections start with `review_status = 'pending'` and must be reviewed visually before they can be promoted to deep line imaging. This is intentional — automated line finders produce false positives, especially near bright continuum artefacts.

!!! warning "VLSR is required"
    ADMIT's built-in source velocity lookup only covers its own demo sources. For real
    survey targets, the systemic velocity (`vlsr_kms`) must be set in the `targets` table
    before running ADMIT — otherwise line identification will be off by the full source
    recession velocity.

    ```bash
    sqlite3 db/<survey>.db \
      "UPDATE targets SET vlsr_kms = 8.5
       WHERE mous_id = '<mous-id>' AND alma_source_name = '<source>';"
    ```

---

## Running ADMIT line identification

This step runs in a CASA environment (ADMIT is imported via a path inserted onto `sys.path`).

=== "Manual"

    ```bash
    casa --nologger --nologfile \
        -c alma-sails-codebase/scripts/manual_admit_lineid.py \
        --mous-id <mous-id> \
        --source <source> \
        --spw <spw-index>
    ```

    Options:

    | Argument | Default | Description |
    |---|---|---|
    | `--force` | off | Re-run even if a completed admit unit exists |
    | `--dry-run` | off | Print the plan without running ADMIT |

=== "Prefect (CANFAR)"

    ```bash
    prefect deployment run admit-lineid \
        --param mous_id=<mous-id> \
        --param source=<source> \
        --param spw=<spw-index>
    ```

---

## Reviewing detections

After ADMIT runs, review its detections before proceeding to deep imaging:

```bash
python alma-sails-codebase/scripts/review_line_detections.py
```

This interactive tool lists all pending detections, shows the peak S/N and frequency, and lets you confirm or reject each one:

```bash
# Confirm a detection by its ID
python alma-sails-codebase/scripts/review_line_detections.py --confirm <detection-id>

# Reject a false positive
python alma-sails-codebase/scripts/review_line_detections.py --reject <detection-id>

# List all pending detections
python alma-sails-codebase/scripts/review_line_detections.py --list
```

Only `confirmed` detections are eligible for deep line imaging. Rejected detections remain in the database for audit purposes.

---

## What's on disk afterward

ADMIT writes a project directory next to the triage cube. Once ADMIT completes successfully, the redundant input cube copy that ADMIT creates is deleted automatically (it is identical to the triage FITS file). The permanent records are the `line_admit_detection` rows in the database.

---

## Verify

```bash
sqlite3 db/<survey>.db \
  "SELECT d.detection_id, d.species_name, d.frequency_ghz,
          d.peakrms_sigma, d.review_status
   FROM line_admit_detection d
   JOIN line_admit_unit u USING (line_admit_unit_id)
   JOIN line_triage_unit t USING (line_triage_unit_id)
   WHERE t.mous_id = '<mous-id>'
   AND t.target_name = '<source>';"
```

---

## Troubleshooting

If ADMIT crashes, reset the unit and re-run from scratch:

```bash
python alma-sails-codebase/scripts/reset_admit.py \
    --mous-id <mous-id> \
    --source <source> \
    --spw <spw-index>
```

This deletes the ADMIT project directory and resets the unit to `ready`. Any existing `line_admit_detection` rows for this unit are left in the database.
