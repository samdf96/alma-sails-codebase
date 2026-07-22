# Check Pipeline Status

---

## Quick summary

Query the database directly for an overview of pipeline state:

```bash
sqlite3 db/<survey>.db \
  "SELECT current_state, COUNT(*) AS n FROM pipeline_state
   GROUP BY current_state ORDER BY n DESC;"
```

---

## Database queries

For more detail, query the database directly.

**MOUSes at each state:**
```sql
SELECT current_state, COUNT(*) AS n
FROM pipeline_state
GROUP BY current_state
ORDER BY n DESC;
```

**Imaging unit breakdown:**
```sql
SELECT config_type, band, current_state, COUNT(*) AS n
FROM imaging_pipeline_state
GROUP BY config_type, band, current_state
ORDER BY config_type, band;
```

**Which sources have complete continuum images:**
```sql
SELECT target_name, band, config_type
FROM imaging_pipeline_state
WHERE current_state = 'complete'
ORDER BY target_name, band;
```

**Line detections awaiting review:**
```sql
SELECT d.detection_id, t.target_name, t.spw_idx,
       d.species_name, d.frequency_ghz, d.peakrms_sigma
FROM line_admit_detection d
JOIN line_admit_unit u USING (line_admit_unit_id)
JOIN line_triage_unit t USING (line_triage_unit_id)
WHERE d.review_status = 'pending'
ORDER BY d.peakrms_sigma DESC;
```

**Recent state transitions (audit log):**
```sql
SELECT mous_id, from_state, to_state, at, note
FROM state_transitions
ORDER BY at DESC
LIMIT 20;
```

---

## Disk usage

Check how much space each MOUS's data is consuming at the OS level:

```bash
du -sh datasets/uid___A001_*/
```

For a per-state summary, query which MOUSes are at each state and cross-reference with `du` on their directories. The `audit_mous_disk_state.py` script does this automatically and highlights any DB/disk mismatches.

```bash
python alma-sails-codebase/scripts/audit_mous_disk_state.py
```

---

## Run log inspection

The `run_log` table stores structured JSON payloads from each script run — durations, product paths, errors. Query it for details on a specific MOUS:

```sql
SELECT step, at, level, payload
FROM run_log
WHERE mous_id = '<mous-id>'
ORDER BY at;
```

For imaging:
```sql
SELECT step, at, payload
FROM imaging_run_log
WHERE imaging_unit_id = (
    SELECT imaging_unit_id FROM imaging_pipeline_state
    WHERE target_name = '<source>'
    AND band = '<band>'
    AND config_type = '<config>'
);
```
