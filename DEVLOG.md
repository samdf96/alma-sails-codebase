# ALMA-SAILS Pipeline Devlog

## v2 Major Revision — Summary

*2026-05-30*

This revision completes the v2 architecture rewrite and merges it into `main`, replacing the v1 codebase entirely. The v1 code is no longer accessible from `main` but remains in the git history.

### What changed

**Core architecture**
- Replaced scattered status columns (`selfcal_status`, `corrected_split`, etc.) with a single FSM-driven `current_state` column per MOUS, backed by an append-only `state_transitions` audit log and a structured `run_log` event table
- Introduced a three-layer architecture (orchestration → work unit → state layer) so that Prefect flows and manual scripts call the same underlying function with no divergence
- Pushed VM/CANFAR path duality entirely into `alma_ops/db.py` via `to_local()` / `to_canonical()`; no script above the DB layer handles path translation

**Imaging pipeline (new in v2)**
- `alma_ops/imaging/` — `run_continuum_imaging()` work unit handling tclean → impbcor → exportfits, cell size and image size computed from MS metadata, auto-multithresh masking selected by B75 baseline length
- `imaging_flow.py` — fire-and-forget Prefect flow; flexible session mode (no cores/RAM pinned) by default; `ncores > 1` enables parallel tclean via mpicasa
- `manual_continuum_imaging.py` and `batch_continuum_imaging.py` for interactive use
- `mark_imaging_ready.py` to promote eligible imaging units from `pending` to `ready`
- Imaging FSM: `pending → ready → running → complete → failed`; results (FITS paths, robust, timestamp) recorded in `imaging_pipeline_state.imaging_products`

**Scripts and flows**
- All headless entry points (`headless_download.sh`, `headless_split.sh`, `headless_imaging.sh`) are thin shell wrappers that delegate to Python drivers
- `headless_imaging.sh` branches on `ncores`: `mpicasa -n N` for parallel, plain `casa` for single-threaded
- Session launch logging captures stdout/stderr from the canfar library so HTTP errors are visible in Prefect flow logs
- `reset_selfcal.py` and `reset_download.py` for safe recovery of stuck/failed MOUSes
- `manual_contsub.py` for continuum subtraction when autoselfcal findcont fails

**Documentation**
- `DEVLOG.md` — architecture decisions, design lessons, planned work
- `README.md` — full codebase overview including imaging pipeline
- `OPERATIONS.md` — Prefect server, work pool, and worker setup
- `CLAUDE.md` — codebase conventions for AI-assisted development

### What is not yet in this revision
- Line imaging (flagged as future work)
- Imaging orchestrator flow for fully automated hands-off imaging
- Disk verification in `mark_imaging_ready.py`
- End-to-end test of the mpicasa parallel tclean path on a real headless session

---

## v2 Architecture — Design Decisions

*2026-05-19*

Leading up to CASCA-MIL 2026, I needed to starting finalizing the data preparation and getting the imaging underway. The CANFAR Kueue system was getting overloaded, and none of my jobs were going through in any amount of reasonable time. This caused me to spend some time thinking about the overall problems with the codebase, especially how reliant I was in building to interact solely with the Kueue system (headless-mode). The codebase also became a mess, as I would also develop manual scripts I could use, that just mimicked the Prefect flow versions. I ended up launching these from an interactive session to make sure something was always being processed. 

The main issues that I found were:

- Pipeline state was tracked through scattered columns (`selfcal_status`, `corrected_split`, `cleanup_status`, etc.) each with their own mini-progression. The database could reach logically impossible states and there was no history of how a MOUS got where it is. Although this wasn't a large issue overall, decoupling my work from the Kueue session, made it so I would have to keep track of a manual databse myself, which wasn't the intention.
- The Prefect flows and the manual bypass scripts had diverged into parallel implementations of the same logic. Fixing a bug in one didn't fix it in the other.
- Every script that touched a file path had to know about the VM/CANFAR path duality, which was a constant source of friction. My solution for using a DualPath class implementation actually worked very well, but it meant that the DualPath infrastructure was passed in to every single script, and it had to be written explicitly into every function that needed a path.

The v2 rewrite addresses all three.

---

### Finite State Machine

Instead of scattered status columns, each MOUS now has a single `current_state` column and a dedicated `state_transitions` table that records every state change as an event:

```
mous_id | from_state      | to_state         | at                  | note
--------|-----------------|------------------|---------------------|------
X2d20   | NULL            | pending          | 2026-05-19 ...      | seeded
X2d20   | pending         | downloaded       | ...                 |
X2d20   | downloaded      | split_complete   | ...                 |
X2d20   | split_complete  | selfcal_prepped  | ...                 |
X2d20   | selfcal_prepped | selfcal_running  | ...                 |
X2d20   | selfcal_running | selfcal_complete | ...                 |
```

The valid MOUS states are:

```
pending → downloaded → split_complete → selfcal_running → selfcal_complete
                                                        → failed
```

The key rule for deciding whether something deserves its own state: **a state is worth having if losing it would cost real work to recover.** This is why `selfcal_running` is a distinct state from `split_complete` — if the process crashes after a job is submitted to Kueue but before it records completion, you need to know a job is already out there so you don't double-submit. Cheap steps that happen atomically within the same script (file organisation, cleanup) don't get their own states; they're either all done or none of it happened.

Failures land in a `failed` state with a diagnostic payload attached, not a crash. The pre-flight check at the start of each major step verifies that everything needed is in place before anything destructive happens. If something is missing, the MOUS goes to `failed` with a note explaining what was absent. Resetting for a retry is a manual database update back to the appropriate prior state. Leaving this being manual is going to be helpful in making sure that things are as expected, and any bugs in the codebase can be dealt with.

The imaging side (continuum and line) follows the same pattern with its own parallel set of tables. Imaging units are `(target, band, config_type)` combinations — one row per unique combination, linked to their contributing MOUSes via a junction table. Imaging states are simpler:

```
pending → ready → running → complete
                          → failed
```

`pending` means contributing MOUSes aren't selfcal-complete yet. `ready` is promoted once they are.

---

### Three-Layer Architecture

The root cause of the manual/Prefect divergence in v1 was that the work logic was coupled to the orchestration layer. The fix is to keep them separate by role:

```
┌─────────────────────────────────────────┐
│  Orchestration layer                    │  decides WHEN and WHERE to run
│  (Prefect flow, manual script, cron)    │
├─────────────────────────────────────────┤
│  Work unit                              │  does the actual work
│  (pure Python callable, no DB access)   │
├─────────────────────────────────────────┤
│  State layer                            │  records what happened
│  (DB write: transition + payload)       │
└─────────────────────────────────────────┘
```

The work unit is a plain Python function. It takes inputs, does its job, and returns a result object (payload). It has no knowledge of Prefect, no database calls, no awareness of how it was invoked. A Prefect flow calls it and writes the result to the DB. A manual script calls the exact same function and writes the result to the DB. There is one implementation of each pipeline step.

The DB write (state transition + payload) is a single atomic transaction. Either both the payload and the state change commit, or neither does and the MOUS stays in its previous state as if the step never ran. This means a crash mid-step is always safe to retry.

For headless CANFAR sessions (Kueue jobs), the session script itself plays the role of the thin wrapper — it calls the work unit and writes the result when done. The calling process (Prefect or manual) writes `selfcal_running` at submission time and exits. The headless session writes `selfcal_complete` when finished. The roles are split across two processes, but the responsibilities are still cleanly assigned.

---

### Path Duality

The project filesystem is mounted at two different paths depending on where code is running:

| Environment | Path prefix |
|---|---|
| VM (interactive shell) | `/home/ubuntu/canfar_arc/projects/ALMA-SAILS` |
| CANFAR platform (containers, headless sessions) | `/arc/projects/ALMA-SAILS` |

In v1, the `DualPath` class handled translation, but it had to be called by every script that read a path from the database.

The v2 approach is to push all path translation to the database access layer. Paths are stored in the DB in canonical form (`/arc/projects/...`). On every read, the DB layer detects the current environment and translates to the appropriate local form before returning the path to the caller. On every write, paths are translated back to canonical form before being stored.

This will hopefully work; I will just have to be careful on the times when a VM-based script is launching something for the Kueue.

```python
def _resolve_prefix() -> str:
    if os.path.exists("/arc/projects/ALMA-SAILS"):
        return "/arc/projects/ALMA-SAILS"      # running on CANFAR
    return "/home/ubuntu/canfar_arc/projects/ALMA-SAILS"  # running on VM
```

The detection logic still exists — it's just in one place. No script above the DB layer ever calls it. If the mount point ever changes, there is one file to update.

---

### Database Structure

The database is organized in three layers:

**Reference layer** — static, imported from ALMiner, never written to by the pipeline:
- `alminer_metadata` — raw ALMiner export, one row per (MOUS, target, spectral window)
- `projects` — unique project rows derived from metadata
- `mous` — unique MOUS rows, with `band` and `array_config` inferred from `antenna_arrays`
- `targets` — unique (MOUS, source) rows

**Entity layer** — pipeline state, updated as work progresses:
- `pipeline_state` — current MOUS state plus product paths
- `imaging_unit` — one row per (target, band, config_type) combination
- `mous_imaging_unit` — junction table linking MOUSes to imaging units
- `imaging_pipeline_state` — current imaging unit state

**Event layer** — append-only audit trail:
- `state_transitions` / `imaging_state_transitions` — every FSM state change
- `run_log` / `imaging_run_log` — step-level diagnostic payloads

The `mous_imaging_unit` junction table deserves a note: it represents the many-to-many relationship between MOUSes and imaging units. One imaging unit (e.g., Serpens-emb10, Band 6, 12m) may have data contributed from multiple MOUSes across different projects and epochs. One MOUS may contain multiple science targets, each of which becomes a separate imaging unit. The junction table makes both directions of that relationship queryable.

Array configuration (`12m`, `7m`, `tp`) is inferred from the `antenna_arrays` field using pad name prefixes: T7xx pads indicate TP, J/N pads with CM antennas indicate 7m, and everything else is 12m. A low antenna count fallback (≤4 antennas) catches TP observations where PM antennas were used on A-prefixed pads rather than the dedicated T7xx TP pads.

---

## Planned: Manual Ingest Script

Some older datasets cannot be retrieved from the SRDP service and must be manually downloaded and calibrated outside the pipeline. Once the measurement sets exist on disk, there is currently no way to bring them into the pipeline state without re-running steps that have already been done. What I'm thinking is to be able to prep all those ASDMs myself, drop them into a correctly made MOUS directory, and then run a script to fix the database.

The plan is a VM shell script (e.g. `scripts/manual_ingest.py`) that:

1. Takes a MOUS ID and scans the corresponding `datasets/<mous_id>/` directory
2. Finds the split MSes and validates they are present
3. Writes the paths into `pipeline_state` via `update_pipeline_paths`
4. Advances the MOUS to `split_complete` via `record_transition`, with a note indicating manual ingest
5. Optionally accepts `--state` to target a different end state if the data is further along

The end result should be a DB state that is the same as a regular pipeline run, so the selfcal step can proceed without modification. I'm thinking only a manual script for this, since the lead-up to getting the database ready will all be manual anyway.

---

## Selfcal Input Data Handling: Move vs. Copy vs. Symlink

*2026-05-20*

autoselfcal constructs its output MS paths via plain string concatenation on the input `vis` path — there is no `os.path.basename()` call. This means it only works correctly when the vis list contains bare filenames (no path components). Passing absolute or relative paths causes the path to be embedded in the output filename, producing paths like `Target_SerpM_SMM4_Band_6_../splits/uid___..._targets.selfcal.ms` that cannot be created.

Three approaches were considered for getting the split MSes into the autoselfcal working directory:

1. **Copy** — safe, but doubles disk usage for every MOUS going through selfcal. Rejected on storage grounds.
2. **Symlinks** — no data movement, but `shutil.rmtree` on a symlink to a directory has undefined/dangerous behavior. Would require special-casing in `run_post_selfcal` to detect symlinks and use `os.unlink` instead. More complexity for little gain.
3. **Move** — chosen. The split MS is moved from `splits/` into `auto_selfcal/` before autoselfcal runs. This is safe because autoselfcal reads DATA and writes CORRECTED_DATA as a separate column; the two are stored in separate subdirectories within the MS directory. A mid-run kill leaves DATA completely intact — at worst there is a partially-written CORRECTED_DATA that gets cleaned up on retry. The move is idempotent: if the destination already exists (aborted retry), it is left in place.

After selfcal completes, the MS in `auto_selfcal/` has CORRECTED_DATA added to it. The post-selfcal step extracts that to `*_targets.corrected.ms` and the selfcal_complete state is recorded. The `splits/` directory is now empty and is cleaned up as a separate explicit step.

---

## MD5 False-Positive on Multi-MS (MMS) Measurement Sets

*2026-05-22*

`post_selfcal.py` validates the CORRECTED_DATA extraction by computing an MD5 hash of
the source column in the original MS and comparing it against the DATA column in the
extracted output. Two MOUSes triggered a false-positive failure from this check:

- **uid://A001/X12d1/X22e** (2026-05-22) — 2-EB MOUS; both extractions completed
  successfully in ~13 min each (confirmed in CASA log), but the post-selfcal MD5 check
  reported a mismatch.
- **uid://A001/X2d20/X21ab** (2026-05-24) — 8-EB MOUS; same false-positive.

### Root cause

When an MS is a Multi-MS (MMS) — CASA's partitioned measurement set format, used for
parallel processing — its sub-tables are stored in separate partition directories
(`SUBMSS/`). The physical row order within each sub-partition is determined by how
CASA's MSTransformManager assigned rows during the split, and this order is not
guaranteed to be the same across the source MS and the extracted output MS. The rows
are numerically identical; they just appear in a different sequence on disk.

The original `_md5_column()` hashed raw column data in disk order, so identical data
values in a different physical arrangement produced different digests.

### Fix

`_md5_column()` in `alma_ops/selfcal.py` was updated to sort rows within each
`DATA_DESC_ID` by `(TIME, ANTENNA1, ANTENNA2)` before hashing. This produces a
canonical ordering that is invariant to physical row layout. The sort applies
unconditionally to all MS types — for a standard (non-MMS) MS the rows are already in
a consistent order so the sort is a no-op in practice, but the unconditional approach
is simpler and safer than detecting MMS explicitly.

```python
order = np.lexsort((
    sub.getcol("ANTENNA2"),
    sub.getcol("ANTENNA1"),
    sub.getcol("TIME"),
))
h.update(data[..., order].tobytes())
```

Memory is bounded by the largest single DDID loaded at once, since the function
iterates over DDIDs rather than loading the entire column.

### Recovery for affected MOUSes

Both MOUSes were reset to `selfcal_running` and `post_selfcal.py` was re-run with the
fixed `_md5_column()`. Both reached `selfcal_complete` on the re-run.

---

## Operational: CADC Proxy Certificate Renewal

CADC proxy certificates expire every **10 days**. When the certificate lapses, the CANFAR skaha service returns a `500 Internal Server Error` with the message `java.lang.RuntimeException: unexpected failure download certificate`. This shows up as repeated retries in the Prefect flow logs for any session launch (imaging, selfcal, download).

Renew the certificate on the VM before the expiry:

```bash
cadc-get-cert -u <your-cadc-username>
```

The certificate is stored at `~/.ssl/cadcproxy.pem`. If sessions are suddenly failing with 500 errors after previously working, check the certificate expiry first before investigating anything else:

```bash
openssl x509 -enddate -noout -in ~/.ssl/cadcproxy.pem
```

---

## Architectural Note: Wrapper Logic Duplication (v2 Lesson)

*2026-05-30*

The v2 rewrite was motivated partly by the fact that manual scripts and Prefect flows in v1 had diverged into parallel implementations of the same logic. The v2 fix was to centralise the *work* into a single callable — `run_continuum_imaging()` in `alma_ops/imaging/imaging_utils.py` — and make both paths call it. That part held.

What wasn't centralised is the *orchestration wrapper* logic around that work:

- **State validation** — is the unit in an acceptable state to run? Is this robust value already done?
- **Mark running** — advance the DB to `running` before work starts
- **Mark complete** — record product paths and advance to `complete` after work finishes

These three pieces are currently duplicated across `manual_continuum_imaging.py`, `imaging_flow.py`, `headless_continuum_imaging.py`, and `batch_continuum_imaging.py`. When one is updated, the others have to be remembered manually. This session exposed exactly that: the manual script had been extended to accept `complete` and `failed` states and guard against duplicate robust values, but `imaging_flow.py` had not been updated to match. The same pattern as v1, one layer higher.

### Why it's harder to fix here

The fire-and-forget architecture creates a structural split: the flow validates and marks `running` in one process, and the headless session marks `complete` in another. A single unified function can't span two processes, so some separation is unavoidable.

### What a deeper fix looks like

The validation and completion recording could live in `alma_ops.db` or a thin `alma_ops.imaging.workflow` module:

```python
# Called by: manual script, flow task, batch script
imaging_unit_id, current_state, run_id = workflow.begin_imaging_run(
    conn, source, band, config, robust
)
# Validates state, checks duplicate robust, advances to 'running', returns run context

# Called by: manual script, headless script, batch script (after run_continuum_imaging())
workflow.complete_imaging_run(conn, imaging_unit_id, run_id, products, robust, imaging_dir)
# Records products_entry with robust + completed_at, advances to 'complete'
```

The orchestration layer (CLI arg parsing, Prefect task wiring, payload JSON handling) stays in each caller — that's legitimately different between contexts. But the *what is valid* and *what gets recorded* logic is in one place. When the rules change, there is one file to update.

### Current state

This has not been implemented yet. The immediate fix (accepting `complete`/`failed` in the flow, adding the robust guard) was applied directly to `imaging_flow.py` to unblock the staging test. The deeper restructure is future work.

---

## Planned: Automated Imaging Orchestrator

Currently imaging is launched manually — either one unit at a time via `manual_continuum_imaging.py` or a random batch via `batch_continuum_imaging.py`. To make imaging fully hands-off through Prefect, the following needs to be built:

### What exists

- `imaging_unit_flow` — submits one imaging unit as a headless CANFAR session (fire-and-forget)
- `mark_imaging_ready.py` — promotes `pending` → `ready` based on contributing MOUS states (run manually on demand)
- No scheduling loop, no MOUS conflict awareness, no autonomous submission

### What needs to be built

**1. `imaging_orchestrator_flow.py`**

A new Prefect flow deployed on a schedule (e.g. every 5 minutes) that:

1. Runs the `mark_imaging_ready` logic to promote any newly-eligible imaging units
2. Queries all units in state `ready`
3. Queries all units currently in state `running` and resolves their contributing MOUSes (see below)
4. Filters the `ready` candidates to remove any that would conflict on a MOUS with a running job
5. Submits up to `max_concurrent - currently_running` new jobs from the filtered candidates
6. Exits — the next scheduled run picks up newly-freed slots

Since the headless sessions are fire-and-forget with no completion callback, the polling approach is the right model: the orchestrator is stateless and re-evaluates the full picture each invocation.

**2. MOUS conflict query**

A DB helper (e.g. `db.get_mous_ids_for_running_imaging_units()`) that returns the set of MOUS IDs currently held by in-flight imaging sessions. Used by the orchestrator to filter candidates before submitting. Implementation: join `imaging_pipeline_state` (where `current_state = 'running'`) through `mous_imaging_unit` to get the MOUS IDs.

**3. `max_concurrent` cap**

A parameter on the orchestrator controlling the maximum number of simultaneously running headless sessions. Prevents flooding the cluster. Suggested defaults: 2–4 for staging, higher for production. The orchestrator computes `slots_available = max_concurrent - len(currently_running)` and submits at most that many new jobs per invocation.

**4. Prefect schedule in `prefect.yaml`**

Wire the orchestrator deployment to an interval schedule so it runs autonomously once deployed. The `use_staging` flag should be a deployment parameter so the same flow can drive either cluster.

### Implementation notes

- The orchestrator should pass `use_staging` through to every `imaging_unit_flow` call it submits.
- `failed` units should be skipped by the orchestrator (manual intervention required to reset and retry).
- Candidate selection within the available slots can be random (matching `batch_continuum_imaging.py` behaviour) or priority-ordered — no strong preference yet.
- The main implementation effort is the orchestrator flow and the MOUS conflict query. Everything else (imaging logic, DB writes, conflict constraint) is already in place.

---

## Planned: VM-Runs Cron Orchestration

The `vm-runs` work pool exists (see `OPERATIONS.md`) but has nothing consuming it yet — it's reserved for a future mass-orchestration flow that would run on a cron schedule directly on the VM, rather than as a CANFAR headless session. The idea is a lightweight, always-on scheduled flow that periodically sweeps pipeline state (across MOUSes, imaging units, and now line-triage/ADMIT units) and decides what to submit next, similar in spirit to the imaging orchestrator above but scoped to the whole pipeline rather than just imaging. Not designed in detail yet — the `pipeline-state-monitoring` pool already covers a narrower version of this (DB monitor + flow initializer); `vm-runs` is meant for the actual lightweight work items that monitor decides to run directly on the VM, as opposed to submitting a headless session for anything CASA-related.

---

## Known Constraint: Concurrent Imaging Jobs That Share a MOUS

If multiple `imaging_unit_flow` runs are submitted to the cluster simultaneously for different imaging units that share a contributing MOUS, the headless CASA sessions will attempt to open the same `_targets.ms` files concurrently. CASA locks measurement sets during tclean access, so the second session will fail.

This is not an issue for `batch_continuum_imaging.py` since it runs sequentially within a single CASA session.

The fix requires MOUS-level conflict detection in `imaging_flow.py`: before marking a unit `running` and submitting the headless session, query `imaging_pipeline_state` for any other units currently in state `running`, resolve their contributing MOUSes via `mous_imaging_unit`, and abort/defer submission if there is overlap. In practice, for Serpens the per-MOUS source counts are small enough that manually staggering submissions avoids this — just don't submit two jobs for sources from the same MOUS at the same time.

---

## Planned: Disk Verification in mark_imaging_ready.py

`mark_imaging_ready.py` currently promotes imaging units from `pending` to `ready` based purely on DB state — it checks that all contributing MOUSes are `selfcal_complete` but never verifies that the `selfcal_products` paths actually exist on disk. A MOUS can be correctly marked `selfcal_complete` in the DB while its product MSes are absent (e.g. accidentally deleted, failed data recovery), and the script will still promote the imaging unit. The failure only surfaces later when the imaging flow tries to open the files.

The fix is a `--verify-disk` flag (off by default to keep the fast DB-only path for routine use). When enabled, after the DB readiness check passes, the script should:

1. Call `db.get_selfcal_products()` for each contributing MOUS
2. Check that every returned path exists on disk via `Path.exists()`
3. If any are missing, treat the imaging unit as not ready and print which files are absent

This mirrors the X15b situation where `selfcal_products` in the DB pointed to files that had been deleted by the v1 `post_selfcal.py`.

---

## Parallel tclean via mpicasa

*2026-05-30*

The full parallel tclean infrastructure is implemented. `ncores` flows from the flow parameter through the JSON payload to `headless_imaging.sh`, which branches on `ncores > 1`:

```bash
if [ "$NCORES" -gt 1 ]; then
    mpicasa -n "$NCORES" casa -c headless_continuum_imaging.py "$PAYLOAD"
else
    casa --nologger --nogui -c headless_continuum_imaging.py "$PAYLOAD"
fi
```

`parallel=True` is passed through to `run_continuum_imaging()` and included in the tclean call. The flow defaults to `ncores=None` (flexible mode, single-threaded CASA); setting `ncores=2` or `ncores=4` enables mpicasa with 16 GB RAM as the default.

**Not yet tested end-to-end on a real headless session** — the mpicasa path has not been exercised on CANFAR. It should work, but minor tweaks may be needed (e.g. confirming the c-casa container image includes mpicasa, or adjusting the exact mpicasa invocation syntax).

Note: parallel tclean distributes the minor cycle (deconvolution) across MPI processes; the major cycle (gridding/FFT) is the dominant cost and does not parallelise as cleanly. For typical single-field protostar continuum images the gain may be modest.

---

## Python Imports Across Environments

The `alma_ops` package needs to be importable in three distinct environments:

- **VM** — running manual or inspection scripts directly
- **c-astroml container** — download and other non-CASA steps
- **CASA container** — manual scripts for split, selfcal, imaging, run via `casa -c scripts/...`

A shared venv won't work for CASA containers because `casa -c script.py` uses CASA's own embedded Python interpreter, which is completely separate from any venv activated in the shell. Sourcing a venv before running `casa -c` has no effect on which Python CASA uses.

The solution is `sys.path` manipulation at the top of every script in `scripts/`:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
```

This tells Python's import system where to find `alma_ops` regardless of which interpreter is running the script — CASA's Python, system Python, or a venv. The `alma_ops/__init__.py` file must exist for Python to treat the directory as a package.

---

## Evaluated: fil3d for streamer identification — not a fit, kept for reference

*2026-07-10*

Investigated `fil3d` ([a-dykim/fil3d](https://github.com/a-dykim/fil3d), Putman, Kim, Clark,
Li, Holm-Hansen, Peek 2026, AJ 171, 76, arXiv:2511.18475) as a candidate for identifying
streamer structures in ALMA-SAILS line cubes, as part of a broader look at filament/streamer
identification tools (separate from the characterization side — TIPSY, `velocity_tools` —
which is a distinct problem tackled later). Concluded it's the wrong tool for the 3D/line
side of that search, for a structural reason rather than a fixable bug. Recorded here so the
reasoning isn't lost; `fil3d-fork/` (this project's MIT-licensed fork, cloned as a sibling of
`alma-sails-codebase`) is left in a working, installable state rather than reverted, in case
the 2D-continuum use described below turns out to be worth revisiting.

### What fil3d does

Runs FilFinder2D independently on every velocity channel of a PPV cube, then links the
resulting 2D masks across *adjacent* channels into 3D structures whenever the overlap
between them (area of intersection over the smaller mask's area) exceeds 85%. Sequential,
no channel-skipping, single-channel objects retained rather than discarded. This is confirmed
directly from Section 3 of the paper, not inferred from the code alone.

### Packaging state as forked

The upstream repo (as of the Oct 2025 commit this was forked from) was not runnable:
`core/tree.py` (the actual linking function, `find_all_trees_from_slices`) and `_const.py`
were referenced but missing from the repo entirely — confirmed by the one open GitHub issue
on the upstream repo, filed by someone else who hit the same thing. Packaging was also
broken (`setup.py` used `find_packages(include=["fil3d","fil3d.*"])` against a flat
directory layout with no nested `fil3d/` package dir, so nothing would have been discoverable
even without the missing files).

Fixed in `fil3d-fork/`, independent of the missing-files question:
- Restructured into a proper nested `fil3d/fil3d/` package layout.
- Replaced `setup.py` with a PEP 621 `pyproject.toml` `[project]` table.
- Found and fixed a real dependency-resolution bug: an unbounded `numpy>=1.22` pin let the
  resolver grab numpy 2.5.1 before checking `numba`'s actual `numpy<2.5` constraint, which
  triggered backtracking all the way down to `numba==0.53.1`/`llvmlite==0.36.0` — a
  combination that refuses to even build on Python 3.12. Bounding numpy to `<2.5` fixed it;
  resolves cleanly to `numba==0.66.0`/`llvmlite==0.48.0` now.
- Renamed the clone directory from `fil3d` to `fil3d-fork`: the restructuring left the outer
  clone directory without its own `__init__.py`, which silently made it a hollow PEP 420
  namespace package that shadowed the real installed package whenever Python ran with
  `ALMA-SAILS/` on `sys.path` — a very normal cwd to be in. Verified directly (same import,
  broken from `ALMA-SAILS/`, correct from elsewhere) before renaming.
- Set up `structure_id_env/` as a separate `uv`-managed umbrella project (its own
  `pyproject.toml`/`uv.lock`) for this kind of exploratory-tool testing, kept apart from
  `alma-sails-codebase`'s locked production venv — `fil3d[cli]` is added there as an
  editable path dependency.

`core/tree.py` (the linking function itself) and `_const.py` (GALFA-HI-specific edge-of-image
constants, architecturally wrong for per-cube ALMA data even once restored) were never
implemented — work stopped once the conceptual issue below was identified, before writing
the linker.

### Why it doesn't fit the 3D streamer-identification use case

fil3d's channel-linking rule assumes the *same spatial footprint* persists across adjacent
velocity channels (>85% overlap) — a reasonable assumption for a Galactic HI filament with
modest internal velocity dispersion, where the paper's own catalog shows most 3D filaments
span only 2-3 channels. A genuine kinematic streamer is close to the opposite case: its
defining physical signature is a large, systematic position-velocity gradient (envelope
material near the cloud's systemic velocity, disk-adjacent material kinematically
accelerated toward near-Keplerian velocities). That means a real streamer's emission
*moves* in position as a function of velocity — exactly the behavior the 85%-overlap
criterion is built to reject as "not the same structure." A strong streamer could fragment
into disconnected short trees, or fail to link at all. The paper itself acknowledges this
limitation in passing ("the overlap fraction may need to be smaller for datasets where the
filaments are expected to have a large velocity gradient") but that's a threshold to loosen,
not a fix — loosen it enough to tolerate a real streamer's gradient and it stops being a
meaningful discriminator at all.

The 2D continuum side is a different story: FilFinder's masking (arctangent flattening +
adaptive local thresholding) was explicitly built to handle a bright compact source
embedded in fainter extended structure — its original validation case (Koch & Rosolowsky
2015) was Herschel dust continuum with embedded YSOs, structurally the same problem as a
compact protostar/disk plus an asymmetric dust residual. If 2D continuum candidate-flagging
becomes useful later, fil3d's per-channel `FilFinder2D` call (not its cross-channel linker)
is the reusable part.

### What's next instead

Looking at SCOUSE (multi-component spectral decomposition) + ACORNS (agglomerative
clustering of the fitted components, linking on point-to-point spatial/velocity/linewidth
continuity rather than mask-shape persistence) as the 3D/line-side approach instead — its
linking model matches a position that drifts systematically with velocity, which is the
thing fil3d's model rejects. See the next entry for the full writeup.

---

## SCOUSE + ACORNS for 3D streamer identification

*2026-07-10*

Follow-on from the fil3d evaluation above: SCOUSE + ACORNS is a two-stage pipeline where
each stage answers a different question, and neither stage can answer the other's —
they're sequential, not alternatives.

### SCOUSE — "what velocity components are actually present at each point in this cube?"

A raw PPV cube gives intensity per pixel per channel; it doesn't say how many physically
distinct kinematic components contribute to any one spectrum. A line of sight through both
quiescent envelope gas and a streamer produces one blended spectrum. A single-Gaussian fit
or a moment map averages that blend into one number and throws away exactly the distinction
that matters — a streamer's kinematic signature is precisely what gets washed out. SCOUSE
([jdhenshaw/scousepy](https://github.com/jdhenshaw/scousepy)) fits each spectrum with as
many Gaussian components as the data statistically supports (derivative spectroscopy
determines the count and initial guesses; a semi-interactive stage validates a subset of
fits, which then guide automated fitting of the rest), and outputs a flat table — one row
per fitted component: position, centroid velocity, linewidth, intensity. It has no concept
of structure — component N at one pixel and component N+1 at the neighboring pixel are just
two unrelated rows in a table as far as SCOUSE is concerned.

### ACORNS — "which of these components belong to the same physical object?"

ACORNS ([jdhenshaw/acorns](https://github.com/jdhenshaw/acorns)) never touches the cube —
confirmed from its actual dependencies (astropy/numpy/scipy/matplotlib only, no cube- or
line-fitting code anywhere in it). Its input is exactly the table SCOUSE produces. It builds
a merge hierarchy via agglomerative clustering: every component starts as its own cluster,
then nearest clusters merge based on physically motivated criteria — spatial separation
under a beam, velocity difference under ~2 channels, linewidth difference under the thermal
linewidth (Henshaw et al. 2019, Appendix B). Confirmed via a real usage example in the
ACORNS repo (`examples/run.py`):

```python
from acorns import Acorns

# dataarr_acorns columns: x, y, intensity, sigma_intensity, velocity, linewidth
cluster_criteria = np.array([min_radius, velo_link, dv_link])
A = Acorns.process(
    dataarr_acorns, cluster_criteria, method="PPV",
    min_height=min_height, pixel_size=pixel_size, relax=relax, stop=stop,
)
A.save_to("example.acorn")
```

`min_radius`/`velo_link`/`dv_link` are exactly the three linking criteria above;
`min_height` sets the minimum prominence above the merge level for something to count as a
distinct cluster; `relax` loosens each criterion for borderline cases; `stop` sets the
significance cutoff on the hierarchy. Output is the actual grouping — "these N components,
across these pixels and this velocity range, are one coherent structure" — which is the
identification result itself, not an intermediate product.

### Why both, and how this compares to fil3d's shape

ACORNS cannot run without a component table (nothing to cluster otherwise); SCOUSE alone
never groups components into structures (a measurement step, not a grouping step). Cube →
components (SCOUSE) → structures (ACORNS) is one pipeline, not two options.

This is the same two-stage shape as fil3d — detect-per-slice, then link-across-slices — just
with a linking rule suited to the target. fil3d detects 2D blobs per channel and links them
by "does the same shape persist"; SCOUSE+ACORNS detects individual velocity components per
pixel and links them by "are neighboring points close in position *and* velocity," which
tolerates (rather than penalizes) a position that systematically drifts with velocity — the
defining signature of a streamer that broke fil3d's model.

### Install status (tested directly, not assumed)

- **ACORNS**: installs cleanly on Python 3.12 with two small workarounds — its `setup.py`
  uses the old `astropy_helpers` build pattern and doesn't declare `extension_helpers` as a
  build dependency (`uv pip install extension_helpers` first, then `--no-build-isolation`),
  and `matplotlib` is imported unconditionally in `plotting.py` but missing from
  `install_requires`. Both one-line fixes. Last real commit Jan 2024, small (9 stars, 1 open
  issue) but not abandoned; the README's "Python 2.6+" line is stale — `setup.cfg` actually
  enforces `>=3.7`.
- **SCOUSE (`scousepy`)**: currently fails to build at all on Python ≥3.10.
  `pyproject.toml`'s `build-system.requires` hard-pins `cython==0.29.14` (2019), which uses
  `collections.Iterable` — removed from `collections` in Python 3.10. Reproduced the exact
  failure directly. Forcing a modern Cython past that pin gets further but then hits a
  second, unrelated packaging bug (a wheel-install step fails to locate
  `SpectralDecomposer.py`). SCOUSE is otherwise actively maintained (pushed Feb 2025, real
  recent commits on parallelized fitting) — this looks like nobody having tried Python ≥3.10
  yet rather than abandonment, but it's a real, currently-unresolved blocker, not a rough
  edge like ACORNS's.

### Status / next step

Not yet installed in `structure_id_env/`. Before adopting this route for real: resolve
`scousepy`'s build (patch the Cython pin locally, then work out whatever the
`SpectralDecomposer.py` issue is — needs its own investigation, same as fil3d got). No
`core/tree.py`-equivalent gap here — both packages are otherwise complete, working code, not
partial forks like fil3d was.

---

## Deep Line Imaging: Missed Frame Transform Mis-centered Every Window

*2026-07-10*

The first real test of the deep line-imaging stage (detection 2, Cyclopropenylidene,
Serpens-SMM1) completed without error and produced a plausible-looking cube — but visual
inspection in CARTA showed real emission rising toward the very edge of the velocity axis
instead of sitting in the middle, with the rest of the cube pure noise. The window-centering
math was systematically wrong, not the data.

### Root cause

`line_imaging_utils.compute_line_window()` derives the sky-frame frequency to center the
imaging window on by inverting ADMIT's own line-identification math. That math involves
*two* frame transforms, and only one was being inverted:

1. ADMIT's `specutil.getspectrum()` runs every frequency through
   `utils.undoppler(freq_sky, vlsr) = freq_sky / (1 - vlsr/c)`, shifting the raw sky/LSRK
   frequency into a source rest frame, *before* any line searching happens at all.
2. `LineID_AT.py`'s own velocity formula (`freqtovel`) is then applied to that already-shifted
   frequency, not the raw sky frequency:
   `velocity = c * (freq_rf - frequency_ghz) / freq_rf + vlsr`.

The original implementation inverted step 2 correctly (recovering `freq_rf`) but stopped
there, using `freq_rf` directly as the window center instead of also undoing step 1. Since
`(1 - vlsr/c)` is applied twice in ADMIT's own pipeline (implicitly) but only once in the
inversion, the resulting error is systematic and close to `2 × vlsr` in every case — not a
per-line fluke. For this source (`vlsr=8.5` km/s), the miscentering was confirmed at
**exactly the expected ~8.5 km/s**: the wrong window center sat at −0.15 km/s in the produced
cube's velocity axis; the corrected center sits at +8.35 km/s — right where the real emission
was actually rising toward the edge in CARTA.

### Fix

`compute_line_window()` now inverts both transforms:

```python
freq_rf       = frequency_ghz / (1 - (velocity_kms - vlsr_kms) / C_KMS)   # undo LineID_AT's velocity formula
freq_peak_ghz = freq_rf * (1 - vlsr_kms / C_KMS)                          # undo ADMIT's own undoppler()
```

Verified against detection 2's real numbers before and after: `freq_peak_ghz` moved from
`491.865458` GHz (wrong) to `491.851512` GHz (correct), a ~13.9 MHz shift — reproduced
independently by hand and matching the corrected formula exactly. This affects **every**
detection imaged through this stage, identified or "Unknown" alike (the `undoppler` inversion
is not a no-op even in the degenerate "Unknown" case where step 2 is).

### Recovery

Detection 2 was re-run with `--force` after the fix and now centers correctly — confirmed
directly in the output. No other detections had reached deep imaging yet at the time this was
caught, so no other cleanup was needed.

### Lesson for next time

The original derivation (see the ADMIT integration work earlier in this file) was checked
carefully against `LineID_AT.py`'s velocity-assignment line directly and confirmed
self-consistent with stored `velocity_kms` values in the DB — but "reproduces ADMIT's own
reported velocity correctly" and "recovers the true sky frequency" turned out to be two
different claims. The bug only surfaced by actually looking at a real cube, not by re-deriving
the algebra harder. Worth remembering for the next math-from-a-vendored-codebase derivation.

---

## Known Constraint: sshfs Working Directory Makes Every Python Startup ~45x Slower

`prefect deploy --all` takes minutes on the VM. The cause is not Prefect, the flow
files, or the network to the Prefect server — it is the shell's *working directory*
being on the sshfs mount.

### Measurement

`strace -f -w -c` (wall-clock attribution, not the default system-time) on a bare
`prefect --version` run from `alma-sails-codebase/flows`:

```
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- -----------
 98.75  130.638330       16234      8047      1655 newfstatat
  0.89    1.174117       41932        28           futex
  0.07    0.089530          23      3834           read
```

8047 path lookups at ~16 ms each. Everything else combined is under two seconds.
16 ms is an sshfs round trip: a micro-benchmark on this mount measures ~25 ms for a
lookup that misses and ~0 ms for one already cached, and the import machinery touches
thousands of *distinct* paths, so most are first-touch.

Same command, same venv, warm caches, alternating between directories:

```
local cwd   run1:   2.60 s      (/mnt/pspace, ext4)
sshfs cwd   run1: 112.67 s      (alma-sails-codebase/flows, fuse.sshfs)
local cwd   run2:   2.60 s
sshfs cwd   run2: 124.42 s
```

The local timings are identical to the hundredth of a second; the sshfs ones vary by
12 s, which is jitter across ~8000 round trips. Prefect's venv is on local ext4 in
both cases and the flow files are not read at all at `--version` — only the working
directory differs.

`prefect deploy --all` then additionally imports all seven flow modules off sshfs to
resolve their entrypoints, on top of that startup cost.

### What is *not* established

Which part of Prefect resolves thousands of paths relative to the cwd was not isolated.
Two things rule out the obvious guess: `prefect` is a console script, so `sys.path[0]`
is `/mnt/pspace/prefect-env/bin` and the cwd is never on the import path; and
`PYTHONSAFEPATH=1` only moved it from 130 s to 104 s. The effect is reproducible, the
mechanism inside Prefect is inferred.

### Likely also behind the CANFAR ReadTimeouts

`session.create()` returning `httpx.ReadTimeout` while the session is in fact created
server-side (see the `create_checked()` work in `flows/canfar_utils.py`) is the same
link with the same latency. Worth treating the two as one cause rather than two
unrelated annoyances.

### Options, none taken

Left as-is deliberately — redeploys are rare and the cost is tolerable.

1. A local staging directory of symlinks (`/mnt/pspace/sails-deploy/` pointing at
   `prefect.yaml`, the seven flow files, and `canfar_utils.py`), deployed from there.
   Only the cwd needs to be local; the symlink targets stay on `/arc` and the relative
   `entrypoint:` values still resolve. The `pull` step already hardcodes the real sshfs
   path as the *worker's* working directory, so runtime is unaffected. Untested.
2. Absolute `entrypoint:` paths plus `prefect deploy --prefect-file <abs path>` run from
   a local cwd. More invasive — changes what is stored in each deployment.
3. Live with it.
