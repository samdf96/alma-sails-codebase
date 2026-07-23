# Getting Started

This page covers what you need before running any pipeline stage.

---

## Prerequisites

**Software**

| Tool | Version | Used for |
|---|---|---|
| Python | ≥ 3.8 | All scripts |
| [CASA](https://casa.nrao.edu) | 6.5.4.9 (pipeline) | Split, selfcal, imaging scripts |
| SQLite | any | Inspecting the database |

CASA includes its own Python interpreter. Scripts that call CASA tasks (`split`, `tclean`, etc.) must be run with `casa -c script.py`, not with your system Python. Scripts that only interact with the database and filesystem can run under any Python ≥ 3.8.

**Python packages** (for non-CASA scripts)

```bash
pip install -e alma-sails-codebase/
# or, to install docs dependencies too:
pip install -e "alma-sails-codebase/[docs]"
```

---

## Directory layout

The pipeline expects a specific layout. The codebase repository sits inside a project directory that holds all data:

```
project-root/
├── alma-sails-codebase/        # This repository (pipeline code)
├── auto_selfcal/               # auto_selfcal checkout
├── admit/                      # ADMIT line-identification toolkit
├── db/
│   └── <survey>.db             # Central SQLite database
├── datasets/                   # One subdirectory per MOUS
│   └── uid___A001_.../
│       ├── splits/             # Split measurement sets
│       └── auto_selfcal/       # Selfcal working directory
├── imaging/
│   ├── continuum/              # Continuum images per (source, band, config)
│   └── lines/
│       ├── triage/             # Dirty line-triage cubes
│       └── final/              # Deep-cleaned line images
└── config/
    └── line_config.json        # Spectral line metadata
```

Scripts are always run from the **project root**, not from inside the codebase directory.

---

## The database

All pipeline state lives in `db/<survey>.db`. This is the single source of truth — you can always query it to find out what has been processed and what hasn't.

```bash
sqlite3 db/<survey>.db
```

Useful first queries:

```sql
-- How many MOUSes at each pipeline stage?
SELECT current_state, COUNT(*) FROM pipeline_state GROUP BY current_state;

-- Which imaging units are ready to image?
SELECT target_name, band, config_type FROM imaging_pipeline_state
WHERE current_state = 'ready';

-- Recent activity log
SELECT mous_id, from_state, to_state, at FROM state_transitions
ORDER BY at DESC LIMIT 20;
```

!!! tip "Canonical paths"
    The database stores paths in canonical form (e.g. `/arc/projects/...`) because this
    pipeline was developed for the CANFAR platform, where storage is mounted at a fixed
    canonical prefix. On your local machine, translate these to your actual project root
    when reading them. The pipeline library handles this automatically — you only need to
    worry about it if you are writing raw SQL queries.

---

## Seeding the database

If you are starting a new survey, the database is initially seeded from the normalized output of the [ALMiner](https://github.com/emerge-erc/ALminer) package, which queries the ALMA archive and produces a structured table of MOUS metadata. See `scripts/seed_database.py` for the seeding workflow.

---

## Next steps

Once the directory layout is in place and the database is seeded, start the [tutorial](tutorial/index.md) to process your first MOUS from download to image.
