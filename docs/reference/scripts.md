# Script Reference

All scripts live in `alma-sails-codebase/scripts/`. Run them from `<project-root>`.

Scripts marked **CASA** must be run with `casa --nologger --nologfile -c script.py [args]` inside a CASA 6 environment. Scripts marked **Python** run with standard Python (no CASA required).

---

## MOUS pipeline scripts

### `manual_download.py` — Python

Download a MOUS from the SRDP archive.

```bash
python alma-sails-codebase/scripts/manual_download.py <mous-id> <srdp-url>
```

Advances: `pending → downloaded`

---

### `manual_split.py` — CASA

Split calibrated MS to target-only spectral windows.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/manual_split.py \
    --mous-id <mous-id>
```

Advances: `downloaded → split_complete`

---

### `manual_selfcal.sh` — CASA (MPI)

Run the full auto_selfcal workflow: prep → findcont → selfcal loops → extract → cleanup.

```bash
bash alma-sails-codebase/scripts/manual_selfcal.sh --mous-id <mous-id>
```

Optional: `MPICASA_NPROC=8` environment variable to set MPI process count.

Advances: `split_complete → selfcal_complete`

---

### `reset_download.py` — Python

Reset a failed download back to `pending`.

```bash
python alma-sails-codebase/scripts/reset_download.py --mous-id <mous-id>
```

---

### `reset_selfcal.py` — CASA

Reset a killed or failed selfcal back to `split_complete`.

```bash
casa --nologger \
    -c alma-sails-codebase/scripts/reset_selfcal.py \
    --mous-id <mous-id>
```

Removes `CORRECTED_DATA`, cleans `auto_selfcal/`, moves MSes back to `splits/`.

---

## Imaging scripts

### `manual_continuum_imaging.py` — CASA

Image a single `(source, band, config)` imaging unit.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/manual_continuum_imaging.py \
    --source <source> --band <band> --config <config> \
    [--robust 0.5] [--dry-run] [--keep-casa-products]
```

Advances: imaging `ready → complete`

---

### `batch_continuum_imaging.py` — CASA

Randomly select and image N ready units sequentially.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/batch_continuum_imaging.py \
    --count <N> \
    [--robust 0.5] \
    [--exclude <source1> <source2> ...] \
    [--dry-run] [--keep-casa-products]
```

Includes `complete` units not yet imaged at the requested robust value.

---

### `mark_imaging_ready.py` — Python

Promote eligible imaging units from `pending` to `ready`.

```bash
python alma-sails-codebase/scripts/mark_imaging_ready.py
```

A unit becomes `ready` when all of its contributing MOUSes are `selfcal_complete`.

---

## Line processing scripts

### `manual_line_triage_imaging.py` — CASA

Classify SPWs and produce dirty line-triage cubes.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/manual_line_triage_imaging.py \
    --mous-id <mous-id> --source <source> \
    [--spw <spw-index>] \
    [--linewidth-floor-kms 1.0] \
    [--force] [--dry-run]
```

Advances: creates and completes `line_triage_unit` rows per SPW

---

### `reset_line_triage.py` — Python

Reset a failed line triage unit.

```bash
python alma-sails-codebase/scripts/reset_line_triage.py \
    --mous-id <mous-id> --source <source> --spw <spw-index>
```

---

### `manual_admit_lineid.py` — CASA

Run ADMIT line identification on a completed triage cube.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/manual_admit_lineid.py \
    --mous-id <mous-id> --source <source> --spw <spw-index> \
    [--force] [--dry-run]
```

Advances: admit `ready → complete`

---

### `reset_admit.py` — Python or CASA

Reset a failed ADMIT run.

```bash
python alma-sails-codebase/scripts/reset_admit.py \
    --mous-id <mous-id> --source <source> --spw <spw-index>
```

Deletes the ADMIT project directory and resets to `ready`.

---

### `manual_line_imaging.py` — CASA

Deep-clean a confirmed line detection.

```bash
casa --nologger --nologfile \
    -c alma-sails-codebase/scripts/manual_line_imaging.py \
    --detection-id <detection-id> \
    [--robust 0.5] [--ncores 1] [--force] [--dry-run] [--keep-casa-products]
```

Advances: line imaging `ready → complete`

---

### `reset_line_imaging.py` — Python

Reset a failed deep line imaging unit.

```bash
python alma-sails-codebase/scripts/reset_line_imaging.py --detection-id <detection-id>
```

---

## Inspection scripts (project root)

These scripts live in `<project-root>`, not inside the codebase.

| Script | Description |
|---|---|
| `pipeline_status.py` | Count of MOUSes and units at each FSM state |
| `disk_usage_summary.py` | Disk usage grouped by pipeline state |
| `review_line_detections.py` | List, confirm, or reject ADMIT line detections |
| `line_config_editor.py` | Interactive editor for `config/line_config.json` |

---

## Prefect flows (`flows/`)

Each flow corresponds to a pipeline stage and submits a CANFAR headless session. See the tutorial pages for per-stage Prefect commands.

| File | Deployment name | Stage |
|---|---|---|
| `download_flow.py` | `mous-download` | Download |
| `split_flow.py` | `mous-split` | Split |
| `selfcal_flow.py` | `mous-selfcal` | Self-calibration |
| `imaging_flow.py` | `continuum-imaging` | Continuum imaging |
| `line_triage_flow.py` | `line-triage-imaging` | Line triage |
| `line_admit_flow.py` | `admit-lineid` | Line identification |
| `line_imaging_flow.py` | `line-imaging` | Deep line imaging |

Deploy all flows at once:

```bash
cd alma-sails-codebase/flows
prefect deploy --all
```
