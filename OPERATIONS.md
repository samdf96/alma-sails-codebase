# OPERATIONS.md

Infrastructure setup for running the ALMA-SAILS Prefect pipeline on the CANFAR VM. This covers provisioning the VM, connecting it to CANFAR storage, bootstrapping the database, the one-time Prefect server setup, persistent environment configuration, work pool and worker management, and how to keep processes alive across SSH sessions.

## VM Provisioning

The Prefect server and orchestration layer run on a dedicated VM — an OpenStack allocation on the Arbutus Cloud Computing Node (University of Victoria), provisioned through the CANFAR Science Platform menu. Credentials match your CANFAR login once the allocation is configured by CANFAR admins.

### Key-Pair and Networking

Generate a key-pair via `Compute -> Key Pairs`; this produces a `[key-pair-name].pem` file, which should be copied into the local `~/.ssh` directory used to `ssh` into the VM. Additional keys can later be appended to the VM's own `~/.ssh/authorized_keys`.

The default OpenStack security group blocks external access. Add the following Ingress rules via `Network -> Security Groups -> Manage Rules (default)`:

| Purpose | Protocol | Port |
|---|---|---|
| `ssh` access | TCP | 22 |
| Ping (optional) | ICMP | Any |
| Prefect UI/API | TCP | 4200 |

### Instance Creation

Create the instance via `Compute -> Instance`. A single vCPU with a few GB of RAM is sufficient — this VM only runs the orchestration layer, not CASA workloads (those run as CANFAR headless sessions). Attach the security group above and the key-pair created earlier, then attach a Floating IP so the instance is reachable externally.

```bash
ssh -i ~/.ssh/[key-pair-name].pem ubuntu@[FLOATING_IP]
```

A `~/.ssh/config` alias makes this less painful:

```
Host prefect-vm
    HostName [FLOATING_IP]
    User ubuntu
    IdentityFile ~/.ssh/[key-pair-name].pem
```

```bash
ssh prefect-vm
```

If the VM is ever deleted and re-created, the floating IP's host fingerprint changes and `ssh` will refuse to connect. Clear the stale entry before reconnecting:

```bash
ssh-keygen -f '~/.ssh/known_hosts' -R '[FLOATING_IP]'
```

### Persistent Volume

The Prefect server's database and the synced Python environment need to survive VM deletion/recreation, so they live on a separate Cinder volume rather than the VM's root disk. Create a volume via `Volumes -> Volumes`, attach it to the instance, then format and mount it:

```bash
sudo mkfs.ext4 /dev/vdc
sudo mkdir /mnt/pspace
sudo mount /dev/vdc /mnt/pspace
sudo chown -R ubuntu:ubuntu /mnt/pspace
```

(`lsblk` shows the current attach point if it's not `/dev/vdc`.) Everything below that references `/mnt/pspace` — `PREFECT_HOME`, the synced venv — depends on this mount existing.

---

## Connecting to CANFAR Storage

### Mounting `/arc` via sshfs

The VM needs access to the project space (`/arc/projects/ALMA-SAILS`) to run the codebase and read/write pipeline data. Generate a dedicated key and register it with the CANFAR Science Platform's `~/.ssh/authorized_keys`, then mount:

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/canfar_key
mkdir -p ~/canfar_arc
sshfs \
  -p 64022 \
  -o IdentityFile=/home/ubuntu/.ssh/canfar_key \
  -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=10 \
  [username]@ws-uv.canfar.net:/ \
  /home/ubuntu/canfar_arc
```

The `reconnect,ServerAliveInterval=15,ServerAliveCountMax=10` options are load-bearing, not optional extras — without them the mount silently drops on any network blip, and every script reading `/home/ubuntu/canfar_arc/...` starts failing until it's manually remounted. `ServerAliveInterval=15` sends a keepalive every 15s; `ServerAliveCountMax=10` tolerates up to 10 missed keepalives (~150s) before giving up; `reconnect` then re-establishes the mount automatically rather than leaving it dead.

### CANFAR Authentication

Launching CANFAR headless sessions (Kueue jobs) from the VM — rather than from inside an interactive CANFAR session, where auth is handled automatically — needs an explicit login:

```bash
canfar auth login
```

Add `--force` if it doesn't prompt. Choose CADC as the provider and authenticate with CANFAR credentials. This is required before Prefect (or any manual script) can submit headless sessions from the VM. See [canfar](https://github.com/opencadc/canfar) for details on the underlying client.

---

## Bootstrapping the Database

A fresh `serpens_v2.db` needs to be created from the schema and seeded from an ALMiner metadata export before any pipeline step can run. From the project root:

```bash
sqlite3 db/serpens_v2.db < schemas/serpens_pipeline_v2.sql
```

Then seed it from an ALMiner query export (CSV):

```bash
python alma-sails-codebase/scripts/seed_database.py <path-to-alminer-export.csv> --db-path db/serpens_v2.db
```

This populates the reference layer (`alminer_metadata`, `projects`, `mous`, `targets`) *and* writes the initial `pending` rows and NULL→`pending` transitions for both FSMs (`pipeline_state`/`state_transitions` and `imaging_unit`/`imaging_pipeline_state`/`imaging_state_transitions`) — see `seed_database.py`'s own docstring for the exact seeding order. Everything beyond those initial rows is written by the pipeline itself as MOUSes progress.

Finally, apply the schema migrations that came after the base schema — these add tables/columns for later pipeline stages (line-triage, ADMIT, mosaic field selection) that aren't in `schemas/serpens_pipeline_v2.sql` itself:

```bash
python alma-sails-codebase/scripts/migrate_add_line_triage_tables.py
python alma-sails-codebase/scripts/migrate_add_field_selection_columns.py
python alma-sails-codebase/scripts/migrate_add_admit_tables.py
```

All three are idempotent, but **`migrate_add_admit_tables.py` must run after `seed_database.py`, not before** — it backfills `targets.vlsr_kms` for every existing row (`UPDATE targets SET vlsr_kms = 8.5`), which silently does nothing useful if `targets` is still empty. The other two are pure DDL (new tables / nullable columns) and aren't order-sensitive, but keeping all three together after seeding is the simplest correct sequence to remember. As new pipeline stages add their own tables in the future, expect this list to grow with the same pattern rather than being folded into the base schema file.

---

## Prefect Server

### Environment and Profile

The Prefect server stores its database and profiles at `PREFECT_HOME`. This must be set to the persistent volume rather than the default `~/.prefect` so that run history survives reboots:

```bash
export PREFECT_HOME=/mnt/pspace/prefect-data
```

Create and activate a dedicated Prefect profile:

```bash
prefect profile create server
prefect profile use server
```

Set the API URLs to match the VM's floating IP (these are also in `prefect.env`):

```
PREFECT_HOME=/mnt/pspace/prefect-data
PREFECT_API_URL=http://[FLOATING_IP]:4200/api
PREFECT_UI_API_URL=http://[FLOATING_IP]:4200/api
```

### Python Environment

The Prefect server and workers need the codebase dependencies installed. To avoid running through the sshfs-mounted volume, sync a venv directly onto the persistent volume:

```bash
cd alma-sails-codebase
uv sync
uv export --locked --no-hashes > requirements-locked.txt
uv venv /mnt/pspace/prefect-env
uv pip install --python /mnt/pspace/prefect-env/bin/python -r requirements-locked.txt
```

Verify:

```bash
/mnt/pspace/prefect-env/bin/prefect version
```

### Starting the Server

The server must run in a `screen` session so it persists after SSH disconnect (see [Screen Sessions](#screen-sessions)):

```bash
screen -S prefect-server
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect server start --host 0.0.0.0
```

The UI is accessible at `http://[FLOATING_IP]:4200`.

---

## Work Pools

Three work pools segment different types of work:

| Pool | Purpose | Concurrency |
|---|---|---|
| `headless-runs` | Launches CANFAR headless sessions (download, split, selfcal, imaging) | 12 |
| `pipeline-state-monitoring` | Scheduled DB monitor and flow initializer | 1 |
| `vm-runs` | Reserved for lightweight VM-local work — currently unused; planned home for a cron-scheduled mass-orchestration flow (see `DEVLOG.md`) | 2 |

Create a pool:

```bash
prefect work-pool create headless-runs -t process
prefect work-pool create pipeline-state-monitoring -t process
prefect work-pool create vm-runs -t process
```

Set concurrency limits:

```bash
prefect work-pool set-concurrency-limit headless-runs 12
prefect work-pool set-concurrency-limit pipeline-state-monitoring 1
prefect work-pool set-concurrency-limit vm-runs 2
```

---

## Workers

Each pool needs a running worker process. Workers pick up queued flow runs and execute them. Start each worker in its own `screen` session:

```bash
screen -S worker-headless
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect worker start --pool "headless-runs"
```

```bash
screen -S worker-monitor
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect worker start --pool "pipeline-state-monitoring"
```

---

## Deployments

Deployments are defined in `flows/prefect.yaml`. Deploy all flows from the `flows/` directory:

```bash
cd alma-sails-codebase/flows
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect deploy --all
```

To redeploy a single flow after changes:

```bash
prefect deploy --name mous-download
```

Once deployed, flows appear in the Prefect UI and can be triggered with parameters (`mous_id`, `srdp_url`, etc.).

---

## Screen Sessions

`screen` keeps processes alive when the SSH session disconnects.

| Command | Effect |
|---|---|
| `screen -S <name>` | Open a new named session |
| `Ctrl+A, D` | Detach from current session (leaves it running) |
| `screen -ls` | List all running sessions |
| `screen -r <name>` | Re-attach a session |

Recommended sessions to keep running:

| Session name | Process |
|---|---|
| `prefect-server` | `prefect server start --host 0.0.0.0` |
| `worker-headless` | `prefect worker start --pool "headless-runs"` |
| `worker-monitor` | `prefect worker start --pool "pipeline-state-monitoring"` |

---

## Networking

The Prefect UI and API run on port `4200`. This port must be open in the VM's security group ingress rules for the UI to be reachable from a browser. The floating IP assigned to the VM is used as the host in `PREFECT_API_URL` and `PREFECT_UI_API_URL`.
