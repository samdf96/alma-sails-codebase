# Recovery Procedures

When a pipeline stage fails or is interrupted, the database state may not match the on-disk state. Each stage has a dedicated reset script that brings both back into agreement.

!!! warning "Always check state before resetting"
    Query the database to confirm what state the MOUS or unit is actually in before
    running a reset. Running a reset on a unit that is genuinely running will corrupt state.

    ```bash
    sqlite3 db/<survey>.db \
      "SELECT mous_id, current_state FROM pipeline_state
       WHERE mous_id = '<mous-id>';"
    ```

---

## Download failed or incomplete

**Symptom:** MOUS stuck in `pending` with a partial download directory, or download directory missing.

```bash
python alma-sails-codebase/scripts/reset_download.py --mous-id <mous-id>
```

Resets to `pending`. Re-run `manual_download.py` with the original SRDP URL.

---

## Selfcal killed or OOM

**Symptom:** MOUS stuck in `selfcal_running` or `failed`, partial files in `auto_selfcal/`.

```bash
casa --nologger \
    -c alma-sails-codebase/scripts/reset_selfcal.py \
    --mous-id <mous-id>
```

This script must run inside CASA (it calls CASA table tools to strip `CORRECTED_DATA`). It:

- Removes `CORRECTED_DATA` from any partially-corrected MSes
- Cleans up `datasets/<mous-id>/auto_selfcal/`
- Moves MSes back to `splits/`
- Resets state to `split_complete`

---

## Continuum imaging failed

**Symptom:** Imaging unit stuck in `running` or `failed`.

The imaging scripts do not have a dedicated reset command — they use the shared database update pattern. Reset manually:

```bash
sqlite3 db/<survey>.db "
  UPDATE imaging_pipeline_state
  SET current_state = 'ready'
  WHERE target_name = '<source>'
  AND band = '<band>'
  AND config_type = '<config>';

  INSERT INTO imaging_state_transitions
    (imaging_unit_id, from_state, to_state, at, note)
  SELECT imaging_unit_id, 'running', 'ready', datetime('now'), 'manual reset after failure'
  FROM imaging_pipeline_state
  WHERE target_name = '<source>'
  AND band = '<band>'
  AND config_type = '<config>';
"
```

Remove any partial CASA products in `imaging/continuum/<source>/B<band>_<config>/` before re-running.

---

## Line triage imaging failed (OOM)

**Symptom:** A `line_triage_unit` stuck in `running`, no run log entries, partial `.pb`/`.psf` files on disk.

```bash
python alma-sails-codebase/scripts/reset_line_triage.py \
    --mous-id <mous-id> \
    --source <source> \
    --spw <spw-index>
```

Deletes partial CASA products and resets that SPW's unit to `ready`. Other SPWs for the same MOUS/source are unaffected.

---

## ADMIT line identification failed

**Symptom:** A `line_admit_unit` stuck in `running` or `failed`, partial ADMIT project directory.

```bash
python alma-sails-codebase/scripts/reset_admit.py \
    --mous-id <mous-id> \
    --source <source> \
    --spw <spw-index>
```

Deletes the entire ADMIT project directory and resets to `ready`. Existing `line_admit_detection` rows are left in the database.

---

## Deep line imaging failed

**Symptom:** A `line_imaging_unit` stuck in `running`, partial CASA products in `imaging/lines/final/`.

```bash
python alma-sails-codebase/scripts/reset_line_imaging.py \
    --detection-id <detection-id>
```

Removes partial products and resets to `ready`.

---

## Checking what state a unit is in

When something goes wrong, these queries help diagnose the situation:

```sql
-- Last 10 transitions for a MOUS
SELECT from_state, to_state, at, note
FROM state_transitions
WHERE mous_id = '<mous-id>'
ORDER BY at DESC LIMIT 10;

-- What step did imaging fail at?
SELECT step, level, payload, at
FROM imaging_run_log
WHERE imaging_unit_id = (
    SELECT imaging_unit_id FROM imaging_pipeline_state
    WHERE target_name = '<source>'
);

-- Failure count for a MOUS
SELECT mous_id, failure_count, current_state
FROM pipeline_state
WHERE mous_id = '<mous-id>';
```
