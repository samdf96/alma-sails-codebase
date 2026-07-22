# Pipeline States Reference

The pipeline uses several independent finite-state machines (FSMs), one per processing track. Each FSM is stored as a `current_state` column in the relevant database table.

---

## MOUS FSM (`pipeline_state`)

Tracks each MOUS from archive download through self-calibration.

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> downloaded : manual_download.py
    downloaded --> split_complete : manual_split.py
    split_complete --> selfcal_running : selfcal_flow (Prefect only)
    split_complete --> selfcal_complete : manual_selfcal.sh (direct)
    selfcal_running --> selfcal_complete : headless session completes
    selfcal_running --> failed : OOM / timeout
    selfcal_complete --> failed : (rare — post-selfcal step fails)
    failed --> split_complete : reset_selfcal.py
```

| State | Description |
|---|---|
| `pending` | Initial state; no data downloaded yet |
| `downloaded` | Calibrated MS retrieved; `mous_directory` set in DB |
| `split_complete` | Target-only MSes extracted; `split_products` set in DB |
| `selfcal_running` | Selfcal session submitted (Prefect path only) |
| `selfcal_complete` | Selfcal finished; `selfcal_products` and `selfcal_products_sub` set |
| `failed` | Terminal failure; `failure_count` incremented |

---

## Imaging FSM (`imaging_pipeline_state`)

One row per `(source, band, config_type)` group.

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> ready : mark_imaging_ready.py
    ready --> running : imaging script starts
    running --> complete : tclean + impbcor + exportfits done
    running --> failed : exception during imaging
    complete --> running : re-image at different robust
    failed --> ready : manual DB reset
```

| State | Description |
|---|---|
| `pending` | Unit exists but contributing MOUSes are not all selfcal-complete |
| `ready` | All contributing MOUSes at `selfcal_complete`; imaging can start |
| `running` | tclean in progress |
| `complete` | At least one imaging product written; `imaging_products` JSON array populated |
| `failed` | Exception during imaging |

---

## Line Triage FSM (`line_triage_pipeline_state`)

One row per `(mous_id, source, spw_index)`. Rows are created dynamically when the headless session (or manual script) classifies SPWs from `cont.dat`.

| State | Description |
|---|---|
| `ready` | SPW classified as a line candidate; dirty cube not yet made |
| `running` | tclean in progress |
| `complete` | Dirty FITS cube written |
| `failed` | tclean failed (commonly OOM) |

---

## Line Admit FSM (`line_admit_pipeline_state`)

One row per `line_triage_unit_id` (1:1 mapping). Rows are created on first run.

| State | Description |
|---|---|
| `ready` | Parent triage unit is `complete`; ADMIT not yet run |
| `running` | ADMIT in progress |
| `complete` | ADMIT finished; detections written to `line_admit_detection` |
| `failed` | ADMIT raised an exception |

Line detections additionally carry their own `review_status`:

| `review_status` | Description |
|---|---|
| `pending` | Awaiting human review |
| `confirmed` | Confirmed as real; eligible for deep line imaging |
| `rejected` | Marked as false positive; excluded from deep imaging |

---

## Line Imaging FSM (`line_imaging_pipeline_state`)

One row per confirmed `line_admit_detection_id` (1:1 mapping).

| State | Description |
|---|---|
| `ready` | Detection confirmed; deep cube not yet made |
| `running` | tclean in progress |
| `complete` | Cleaned FITS cube written; `imaging_products` populated |
| `failed` | tclean failed |
