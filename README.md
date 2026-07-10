# alma-sails-codebase

Pipeline codebase for the ALMA-SAILS project. Handles the full data preparation workflow from raw SRDP downloads through self-calibration, continuum imaging, and spectral-line identification and imaging.

## Overview

The pipeline operates on ALMA **Member Observing Units (MOUSes)** — individual calibrated datasets retrieved from the NRAO SRDP service. Each MOUS moves through a finite state machine:

```
pending → downloaded → split_complete → selfcal_running → selfcal_complete
                                                         → failed
```

Imaging is driven by **imaging units** — one per (target, band, config_type) combination — which aggregate data from one or more selfcal-complete MOUSes:

```
pending → ready → running → complete
                          → failed
```

Once a MOUS is `selfcal_complete`, its continuum-subtracted MS can also feed a three-stage line-finding pipeline, each stage with its own FSM:

```
line_triage_unit:  ready → running → complete → failed   (dirty, niter=0 cubes per line-candidate spw, for ADMIT to scan)
line_admit_unit:   ready → running → complete → failed   (ADMIT line ID on a completed triage cube; 1:1 with its triage unit)
line_imaging_unit: ready → running → complete → failed   (real, niter>0 image of one human-confirmed detection; 1:1 with its detection)
```

Promotion from `line_admit_detection` to deep imaging is gated on manual review (`review_line_detections.py`, project root) rather than automatic — see `DEVLOG.md`.

Work is orchestrated by Prefect flows that launch CANFAR headless sessions; the sessions do the actual work and write results back to a central SQLite database. Manual scripts mirror each flow step for interactive use.

## Codebase Structure

```
alma-sails-codebase/
├── alma_ops/                   # Core library — DB access, work units, models, config
│   ├── config.py               # Paths, container image names
│   ├── db.py                   # DB connection, FSM writes, path translation
│   ├── models.py               # Result dataclasses (DownloadResult, SplitResult, …)
│   ├── download.py             # run_download() work unit
│   ├── split.py                # run_split() work unit
│   ├── selfcal.py              # run_post_selfcal() work unit
│   └── imaging/
│       ├── imaging_utils.py    # run_continuum_imaging(), compute_imaging_params(), compute_b75()
│       ├── continuum_config.py # Shared tclean parameters, automask sets, image geometry constants
│       ├── cont_dat_utils.py   # cont.dat parsing and SPW channel range conversion
│       ├── line_triage_config.py / line_triage_utils.py   # Dirty triage-cube imaging for ADMIT
│       ├── line_admit_utils.py                            # ADMIT line-identification flow
│       └── line_imaging_config.py / line_imaging_utils.py # Deep imaging of confirmed detections
├── flows/                      # Prefect flows and deployment config
│   ├── prefect.yaml            # Deployment definitions (prefect deploy --all)
│   ├── canfar_utils.py         # make_session() — shared CANFAR session helper
│   ├── download_flow.py        # Launches c-astroml headless session for download
│   ├── split_flow.py           # Launches CASA headless session for MS split
│   ├── selfcal_flow.py         # Launches CASA headless session for autoselfcal
│   ├── imaging_flow.py         # Launches CASA headless session for continuum imaging
│   ├── line_triage_flow.py     # Launches CASA headless session for triage-cube imaging
│   ├── line_admit_flow.py      # Launches CASA headless session for ADMIT line ID
│   └── line_imaging_flow.py    # Launches CASA headless session for deep line imaging
├── scripts/                    # Manual scripts and headless session entry points
│   ├── manual_download.py      # Download — run in c-astroml container
│   ├── manual_split.py         # Split — run via casa -c
│   ├── manual_selfcal.sh       # Selfcal — runs mpicasa (or plain casa -c in flexible mode) + post_selfcal
│   ├── post_selfcal.py         # Post-selfcal product extraction — run via casa -c
│   ├── manual_continuum_imaging.py  # Continuum imaging — run via casa -c
│   ├── batch_continuum_imaging.py   # Image N randomly-selected ready units sequentially
│   ├── manual_contsub.py       # Continuum subtraction — run via casa -c
│   ├── manual_line_triage_imaging.py # Dirty triage-cube imaging — run via casa -c
│   ├── manual_admit_lineid.py        # ADMIT line identification — run via casa -c
│   ├── manual_line_imaging.py        # Deep imaging of one confirmed detection — run via casa -c
│   ├── mark_imaging_ready.py   # Promote eligible imaging units from pending → ready
│   ├── reset_selfcal.py        # Recovery: reset a stuck/failed selfcal back to split_complete
│   ├── reset_download.py       # Recovery: reset a stuck/failed download back to pending
│   ├── reset_imaging.py        # Recovery: reset a stuck/failed continuum imaging run
│   ├── reset_line_triage.py    # Recovery: reset a stuck/failed triage-cube imaging run
│   ├── reset_admit.py          # Recovery: reset a stuck/failed ADMIT run
│   ├── reset_line_imaging.py   # Recovery: reset a stuck/failed deep line-imaging run
│   ├── headless_download.sh    # Headless entry point for download flow
│   ├── headless_split.sh       # Headless entry point for split flow
│   ├── headless_imaging.sh     # Headless entry point for imaging flow
│   ├── headless_continuum_imaging.py  # Payload-driven CASA imaging driver
│   ├── headless_line_triage_imaging.sh # Headless entry point for triage flow
│   ├── headless_admit_lineid.sh        # Headless entry point for ADMIT flow
│   ├── headless_line_imaging.sh        # Headless entry point for deep line-imaging flow
│   ├── seed_database.py        # One-time DB seeding from ALMiner metadata
│   ├── migrate_add_field_selection_columns.py  # One-time: mosaic field-selection columns
│   ├── migrate_add_line_triage_tables.py       # One-time: line_triage_* tables
│   ├── migrate_add_admit_tables.py             # One-time: line_admit_* tables + targets.vlsr_kms
│   └── migrate_add_line_imaging_tables.py      # One-time: line_imaging_* tables
├── DEVLOG.md                   # Architecture decisions, design notes, planned work
├── OPERATIONS.md               # Prefect server, workers, and deployment setup
└── pyproject.toml
```

Review tooling for ADMIT detections (`review_line_detections.py`) lives at the project root, not in this repo — it's project-instance-specific, not portable codebase logic.

## Database Schema

The SQLite database (`db/serpens_v2.db`) is organized in three layers:

**Reference layer** — static, seeded from ALMiner, never written to by the pipeline:
- `alminer_metadata` — raw ALMiner export, one row per (MOUS, target, SPW)
- `projects` — unique projects
- `mous` — unique MOUSes with band and array configuration
- `targets` — unique (MOUS, source) pairs

**Entity layer** — pipeline state, updated as work progresses:
- `pipeline_state` — current FSM state and product paths for each MOUS
- `imaging_unit` — one row per (target, band, config_type) combination
- `mous_imaging_unit` — junction table linking MOUSes to imaging units
- `imaging_pipeline_state` — current state for each imaging unit
- `line_triage_unit` / `line_triage_pipeline_state` — one row per (mous_id, target_name, spw_idx) triage cube
- `line_admit_unit` / `line_admit_pipeline_state` — one row per completed triage unit (1:1)
- `line_admit_detection` — every line ADMIT identified (species, frequency, `peakrms_sigma`, channel range, `review_status`) — the permanent record that survives after the triage cube itself is deleted
- `line_imaging_unit` / `line_imaging_pipeline_state` — one row per confirmed detection (1:1)

**Event layer** — append-only audit trail:
- `state_transitions` / `imaging_state_transitions` / `line_triage_state_transitions` / `line_admit_state_transitions` / `line_imaging_state_transitions` — every FSM state change
- `run_log` / `imaging_run_log` / `line_triage_run_log` / `line_admit_run_log` / `line_imaging_run_log` — step-level diagnostic payloads

Paths in the database are stored in canonical form (`/arc/projects/ALMA-SAILS/...`) and translated to the local prefix at read time. See `alma_ops/db.py` for the translation logic.

## Pipeline States and Transitions

### MOUS states

| State | Meaning |
|---|---|
| `pending` | Seeded; SRDP download has not started |
| `downloaded` | Calibrated `.ms` directories are on disk |
| `split_complete` | Target-only science-SPW MSes created in `splits/` |
| `selfcal_running` | Autoselfcal headless session submitted |
| `selfcal_complete` | `*_targets.corrected.ms` and `*_targets.contsub.ms` ready |
| `failed` | A step failed; `run_log` has the diagnostic payload |

### Imaging unit states

| State | Meaning |
|---|---|
| `pending` | Contributing MOUSes not yet selfcal-complete |
| `ready` | All contributing MOUSes are selfcal-complete; promoted by `mark_imaging_ready.py` |
| `running` | Imaging headless session submitted |
| `complete` | FITS products written; paths recorded in `imaging_pipeline_state.imaging_products` |
| `failed` | Imaging failed; `imaging_run_log` has the diagnostic payload |

Every transition is recorded in the relevant transitions table with a timestamp and optional payload.

## Running the Pipeline

### MOUS pipeline via Prefect (automated)

Deploy all flows from the `flows/` directory:

```bash
cd alma-sails-codebase/flows
prefect deploy --all
```

Then trigger runs from the Prefect UI or CLI. Each flow validates state, submits a CANFAR headless session, and exits. See `OPERATIONS.md` for server, work pool, and worker setup.

### MOUS pipeline via manual scripts (interactive)

Each step has a manual script that runs the same work unit and writes the same DB transitions as the Prefect flow. Run from inside the appropriate CANFAR container.

| Step | Container | Command |
|---|---|---|
| Download | c-astroml | `python scripts/manual_download.py --mous-id uid://... --srdp-url https://...` |
| Split | CASA pipeline | `casa --nologger --nologfile -c scripts/manual_split.py --mous-id uid://...` |
| Selfcal | CASA pipeline | `bash scripts/manual_selfcal.sh --mous-id uid://...` |

To reset a failed or stuck MOUS, use the recovery scripts:

```bash
# Reset a failed/killed selfcal back to split_complete
casa --nologger -c scripts/reset_selfcal.py --mous-id uid://A001/X.../X...

# Reset a failed download back to pending
python scripts/reset_download.py --mous-id uid://A001/X.../X...
```

### Continuum imaging

Before imaging can run, eligible imaging units must be promoted from `pending` to `ready`:

```bash
python scripts/mark_imaging_ready.py          # promote all eligible units
python scripts/mark_imaging_ready.py --dry-run  # preview without writing
```

**Via Prefect** — trigger the `continuum-imaging` deployment for one unit:

```
source, band, config    e.g. EC_53, 6, 12m
robust                  Briggs robust weighting (default 0.5)
ncores                  None = flexible mode; >1 = parallel tclean via mpicasa
```

**Via manual script** — run directly in a CASA container:

```bash
casa --nologger --nologfile -c scripts/manual_continuum_imaging.py \
    --source EC_53 --band 6 --config 12m [--robust 0.5] [--dry-run]
```

**Batch** — image N randomly-selected ready units sequentially in one CASA session:

```bash
casa --nologger --nologfile -c scripts/batch_continuum_imaging.py --count 5
```

Output for each imaging unit goes to `imaging/continuum/<source>/B<band>_<config>/`. Products are FITS files (`*.image.fits`, `*.image.pbcor.fits`); CASA intermediates are deleted after export by default.

### Line triage, ADMIT identification, and deep line imaging

Three sequential stages, each gated on the previous one completing (and, for the last stage, on manual review in between). All run via `casa -c` in a CASA container, either directly or through their matching Prefect flow (`line-triage-imaging`, `admit-lineid`, `line-imaging` deployments). Unlike continuum's `imaging_unit`, none of these units are pre-seeded — each is created lazily (`get_or_create`) the moment its precondition is actually met, since their existence depends on a pipeline result (a `cont.dat` classification, an ADMIT run, a confirmed review) rather than static survey metadata.

```bash
# 1. Triage: dirty (niter=0) cubes for every line-candidate spw of a MOUS/source
casa --nologger --nologfile -c scripts/manual_line_triage_imaging.py \
    --mous-id uid://A001/X.../X... --source EC_53

# 2. ADMIT: line identification on one completed triage cube
casa --nologger --nologfile -c scripts/manual_admit_lineid.py \
    --mous-id uid://A001/X.../X... --source EC_53 --spw 3

# Review identified lines and confirm/reject before deep imaging
python review_line_detections.py                       # project root; lists pending detections
python review_line_detections.py --confirm 2 --note "..."

# 3. Deep imaging: real, niter>0 image of one confirmed detection
casa --nologger --nologfile -c scripts/manual_line_imaging.py --detection-id 2
```

Triage output: `imaging/lines/triage/<source>/B<band>_<config>/` (dirty FITS cubes, deleted once every detection tied to them is reviewed). Deep-imaging output: `imaging/lines/final/<source>/B<band>_<config>/` (`*.image.fits`, `*.image.pbcor.fits`, one pair per confirmed detection).

## Three-Layer Architecture

Each pipeline step follows the same structure:

```
Orchestration layer   — decides when and where to run (Prefect flow or manual script)
Work unit             — does the actual work; pure callable, no DB access
State layer           — records what happened (atomic DB transition + payload)
```

Work units (`run_download`, `run_split`, `run_post_selfcal`, `run_continuum_imaging`, `run_line_triage_imaging`, `run_admit_lineid`, `run_line_imaging`) are plain Python functions. Prefect flows and manual scripts call the same work unit and write the same DB transitions. There is one implementation of each pipeline step.

## Development Notes

Architecture decisions and design rationale are in `DEVLOG.md`. Infrastructure and operational setup (Prefect server, work pools, screen sessions) is in `OPERATIONS.md`.