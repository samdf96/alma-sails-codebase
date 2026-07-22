# Database Schema Reference

All pipeline state is stored in a single SQLite database at `db/<survey>.db`.

**Never write raw `UPDATE` statements on state tables.** Use the `alma_ops.db` helper functions (`record_transition`, `record_failure`, etc.), which keep the `*_pipeline_state` and `*_state_transitions` tables in sync. Raw updates to `pipeline_state` will leave `state_transitions` without the corresponding audit entry.

---

## Core survey tables (static after seeding)

### `mous`

One row per Measurement Set of Unique Spectrometers (MOUS).

| Column | Type | Description |
|---|---|---|
| `mous_id` | TEXT PK | ALMA UID (`uid://A001/...`) |
| `project_code` | TEXT | ALMA project code (e.g., `2019.1.00591.S`) |
| `band` | TEXT | ALMA frequency band (`3`, `6`, `7`, ...) |
| `array_config` | TEXT | Array configuration (`12m`, `7m`, `tp`) |

### `targets`

One or more rows per MOUS — all science fields observed.

| Column | Type | Description |
|---|---|---|
| `mous_id` | TEXT FK | Parent MOUS |
| `alma_source_name` | TEXT | Field name as used in the MS |
| `is_mosaic` | TEXT | `'T'` if this field is a mosaic pointing |
| `vlsr_kms` | REAL | Systemic velocity (km/s); required for ADMIT |

---

## MOUS pipeline tables

### `pipeline_state`

One row per MOUS. See [Pipeline States](states.md) for the FSM.

| Column | Type | Description |
|---|---|---|
| `mous_id` | TEXT PK | |
| `current_state` | TEXT | Current FSM state |
| `updated_at` | TEXT | ISO timestamp of last update |
| `failure_count` | INTEGER | Number of times entered `failed` |
| `mous_directory` | TEXT | Canonical path to downloaded data |
| `split_products` | TEXT | JSON array of canonical paths to `*_targets.ms` |
| `selfcal_products` | TEXT | JSON array of canonical paths to selfcal MSes |
| `selfcal_products_sub` | TEXT | JSON array of canonical paths to cont-subtracted MSes |

### `state_transitions`

Append-only audit log.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `mous_id` | TEXT FK | |
| `from_state` | TEXT | Previous FSM state |
| `to_state` | TEXT | New FSM state |
| `at` | TEXT | ISO timestamp |
| `run_id` | TEXT | UUID of the run that caused this transition |
| `note` | TEXT | Human-readable reason |

### `run_log`

Structured per-run events.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | |
| `mous_id` | TEXT FK | |
| `run_id` | TEXT | UUID of the run |
| `step` | TEXT | Step name within the run |
| `at` | TEXT | ISO timestamp |
| `payload` | TEXT | JSON payload (products, durations, errors) |
| `level` | TEXT | `info`, `warning`, `error` |

---

## Imaging tables

### `imaging_unit`

Static grouping: all MOUSes contributing to the same `(source, band, config_type)` image.

| Column | Type | Description |
|---|---|---|
| `imaging_unit_id` | INTEGER PK | |
| `target_name` | TEXT | Canonical source name |
| `band` | TEXT | ALMA band |
| `config_type` | TEXT | `12m`, `7m`, or `tp` |

### `mous_imaging_unit`

Junction table mapping MOUSes to imaging units.

| Column | Type |
|---|---|
| `mous_id` | TEXT FK |
| `imaging_unit_id` | INTEGER FK |
| `source_field_name` | TEXT | MS field name (if different from `target_name`) |
| `field_ids` | TEXT | JSON array of field IDs (mosaic subsets) |

### `imaging_pipeline_state`

One row per imaging unit.

| Column | Type | Description |
|---|---|---|
| `imaging_unit_id` | INTEGER PK FK | |
| `target_name` | TEXT | Denormalised from `imaging_unit` |
| `band` | TEXT | |
| `config_type` | TEXT | |
| `current_state` | TEXT | FSM state |
| `imaging_products` | TEXT | JSON array of product entries (one per imaging run) |

### `imaging_state_transitions` / `imaging_run_log`

Same structure as the MOUS equivalents, keyed on `imaging_unit_id`.

---

## Line triage tables

### `line_triage_unit`

One row per `(mous_id, target_name, spw_idx)`.

| Column | Type | Description |
|---|---|---|
| `line_triage_unit_id` | INTEGER PK | |
| `mous_id` | TEXT FK | |
| `target_name` | TEXT | |
| `spw_idx` | INTEGER | SPW index in the MS |
| `spw_freq_ghz` | REAL | Central frequency |
| `imaging_dir` | TEXT | Canonical path to triage cube directory |

### `line_triage_pipeline_state` / `line_triage_state_transitions` / `line_triage_run_log`

Standard FSM state + audit + run log tables keyed on `line_triage_unit_id`.

---

## Line admit tables

### `line_admit_unit`

One row per `line_triage_unit_id` (1:1).

| Column | Type | Description |
|---|---|---|
| `line_admit_unit_id` | INTEGER PK | |
| `line_triage_unit_id` | INTEGER FK UNIQUE | |
| `admit_products` | TEXT | JSON: path to ADMIT project directory |

### `line_admit_detection`

One row per line identified by ADMIT.

| Column | Type | Description |
|---|---|---|
| `detection_id` | INTEGER PK | |
| `line_admit_unit_id` | INTEGER FK | |
| `species_name` | TEXT | Line species (e.g., `HC3N`) |
| `frequency_ghz` | REAL | Rest frequency |
| `peakrms_sigma` | REAL | Peak signal-to-noise |
| `channel_min` | INTEGER | First channel of detection |
| `channel_max` | INTEGER | Last channel of detection |
| `blend_id` | INTEGER | Non-null if blended with another line |
| `review_status` | TEXT | `pending`, `confirmed`, or `rejected` |

### `line_admit_pipeline_state` / `line_admit_state_transitions` / `line_admit_run_log`

Standard tables keyed on `line_admit_unit_id`.

---

## Line imaging tables

### `line_imaging_unit`

One row per confirmed `line_admit_detection_id` (1:1).

| Column | Type | Description |
|---|---|---|
| `line_imaging_unit_id` | INTEGER PK | |
| `line_admit_detection_id` | INTEGER FK UNIQUE | |
| `mous_id` | TEXT | Denormalised |
| `target_name` | TEXT | Denormalised |
| `spw_idx` | INTEGER | Denormalised |
| `species_name` | TEXT | Denormalised from detection |
| `frequency_ghz` | REAL | Denormalised from detection |
| `imaging_products` | TEXT | JSON array (one entry per run) |

### `line_imaging_pipeline_state` / `line_imaging_state_transitions` / `line_imaging_run_log`

Standard tables keyed on `line_imaging_unit_id`.

---

## Path conventions

All paths stored in the database use the **canonical prefix** (e.g., `/arc/projects/ALMA-SAILS/`), not the local filesystem path. The `alma_ops.db` helper functions translate paths automatically when reading from or writing to the database. If you are writing raw SQL, apply `to_local()` after reading and `to_canonical()` before writing.
