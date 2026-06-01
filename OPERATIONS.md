# OPERATIONS.md

Infrastructure setup for running the ALMA-SAILS Prefect pipeline on the CANFAR VM. This covers the one-time server setup, persistent environment configuration, work pool and worker management, and how to keep processes alive across SSH sessions.

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
| `headless-runs` | Launches CANFAR headless sessions (download, split, selfcal) | 12 |
| `pipeline-state-monitoring` | Scheduled DB monitor and flow initializer | 1 |

Create a pool:

```bash
prefect work-pool create headless-runs -t process
prefect work-pool create pipeline-state-monitoring -t process
```

Set concurrency limits:

```bash
prefect work-pool set-concurrency-limit headless-runs 12
prefect work-pool set-concurrency-limit pipeline-state-monitoring 1
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
