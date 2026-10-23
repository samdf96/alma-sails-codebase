# CASA rc files

A CASA rc file sets casacore variables that CASA reads at startup. The
pipeline does not need one for most jobs, but any job that runs `tclean` on a
large cube should get one that caps CASA's memory use. Nothing in the codebase
applies an rc file automatically: you pass one when the job needs it.

## Why a memory cap is needed on CANFAR

CASA decides how to split a cube into chunks ("subcubes") from the memory it
*thinks* it has. Inside a CANFAR container it reads the **host's** RAM, not the
session's limit. Uncapped, `tclean` plans a 100+ GB cube as a single chunk, and
the container is killed partway through the PSF step. The unit is left in
`running` with no error and no run-log entry, and needs `reset_line_triage.py`
(or the matching reset script for that stage).

With a cap, `tclean` splits the cube into subcubes that fit under it and works
through them one at a time.

## What the file must contain

The file is plain text in casacore's `aipsrc` format, one `key: value` per line:

```
system.resources.memory: 24000
system.resources.memfrac: 80
```

| Key | Unit | Meaning |
|---|---|---|
| `system.resources.memory` | MB | Total memory CASA may plan for. This is the cap. |
| `system.resources.memfrac` | percent | Fraction of that memory CASA will actually use. |

`system.resources.memory` is a casacore variable, **not** a `~/.casa/config.py`
setting; putting it in `config.py` has no effect. That is why it has to come
from an rc file.

**Choosing the value:** keep the cap at or below about 80% of the RAM the
session really has. For a session pinned with `ram_gb=N`, use roughly
`0.8 × N × 1000` MB. A flexible (unpinned) session has no guaranteed size, so
use the conservative 24 GB preset.

## Presets shipped with the codebase

| File | Cap | Use with |
|---|---|---|
| `casarc/casa_lowmem.rc` | 24 GB | Flexible sessions (no RAM pinned) |
| `casarc/casa_highmem.rc` | 150 GB | Sessions pinned at `ram_gb=192` (CANFAR's maximum) |

Their paths are also available in Python as `alma_ops.config.CASARC_LOWMEM` and
`alma_ops.config.CASARC_HIGHMEM`. For a different session size, write your own
file with the two lines above and pass its path.

## How to pass one

**Prefect flows:** every flow that runs CASA takes a `casarc` parameter
(`--casarc` when a flow file is run directly):

| Flow | Stage |
|---|---|
| `split_flow.py` | Split |
| `selfcal_flow.py` | Self-calibration |
| `imaging_flow.py` | Continuum imaging |
| `line_triage_flow.py` | Line triage |
| `line_admit_flow.py` | ADMIT line identification |
| `line_imaging_flow.py` | Deep line imaging |

```bash
prefect deployment run line-triage-imaging \
    --param mous_id=<mous-id> --param source=<source> \
    --param casarc=/arc/projects/<project>/alma-sails-codebase/casarc/casa_lowmem.rc
```

The flow checks the file exists before launching (and, for continuum imaging
and selfcal, before marking anything `running`), then sets `CASARCFILES` in
the headless session's environment. A relative path is resolved first. Each
headless script prints `CASARCFILES` at the top of the session log; the line
triage script also warns when none was set. Launchers that submit sessions
directly can do the same with `canfar_utils.casarc_env(path)`, which returns
the `env` to pass to `session.create`.

**Manual run inside a CASA container:** set the variable before starting CASA.

```bash
CASARCFILES=/arc/projects/<project>/alma-sails-codebase/casarc/casa_lowmem.rc \
    casa --nologger --nogui -c alma-sails-codebase/scripts/manual_line_triage_imaging.py \
    --mous-id <mous-id> --source <source>
```

A launcher of your own that submits sessions directly can choose a default
rc file for the sessions it starts; the codebase itself never applies one.

## Checking it took effect

The CASA log of the run reports the memory it planned with. With a cap it shows
a value near the cap (`memory: 23.4…` for the 24 GB preset); without one it
shows `memory: -`, or the host's full RAM.

## Larger cap or smaller one?

The total imaging work is the same either way: a larger cap just means fewer,
larger subcubes. But each subcube makes its own pass over the measurement set,
so for a very large cube from a large MS, hundreds of small subcubes can add
many hours of re-reading data. In that case a pinned 192 GB session with
`casa_highmem.rc` is noticeably faster. For ordinary cubes the 24 GB preset
costs little.

If the cube is very large because the image itself is huge (long-baseline
data imaged at full resolution), a bigger cap is the wrong fix. Use
`--taper-arcsec` instead (see [Step 5: Line Triage](../tutorial/05_line_triage.md#very-large-images)).
