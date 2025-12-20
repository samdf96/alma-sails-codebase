# Prefect Server Setup

## Setup - General

When hosting a Prefect server, a local database is created to track the state of the flow runs, and related Prefect metadata like run history, logs, deployments, concurrency limits, etc. (see [The Prefect database](https://docs.prefect.io/v3/concepts/server)). By default, Prefect uses a SQLite database stored at `~/.prefect/prefect.db`, however, this should be mounted on the persistent volume instead.

We first set an evironment variable so that Prefect (and any subsequent `prefect` comamnd) will use the dedicated space created on the mounted volume:

```bash
export PREFECT_HOME=/mnt/pspace/prefect-data
```

Now the default `~/.prefect/prefect.db` is replaced by the dedicated space on the mounted volume. All Prefect profiles are now stored inside this space, under a file called `profiles.toml`. We now create a custom profile called server with the following:

```bash
prefect profile create server
prefect profile use server
```

Finally, the correct API urls can be set; these are also found in the `prefect.env` file. The files contents are given as the following:

```
PREFECT_HOME=/mnt/pspace/prefect-data
PREFECT_API_URL=http://[FLOATING_IP]:4200/api
PREFECT_UI_API_URL=http://[FLOATING_IP]:4200/api
```

Since we want all Prefect server actions to be performed from the persistent volume (and not through the sshfs mounted volume), we sync the codebase `uv` environment to a virtual environment on the persistent volume. This is done with the following set of actions *from the `alma-sails-codebase`*:

```bash
cd .../alma-sails-codebase
uv sync
uv export --locked --no-hashes > requirements-locked.txt
uv venv /mnt/pspace/prefect-env
uv pip install --python /mnt/pspace/prefect-env/bin/python -r requirements-locked.txt
```

Then we can check the installation by running:

```bash
/mnt/pspace/prefect-env/bin/prefect version
```

Or activating the environment and running `prefect` directly:

```bash
source /mnt/pspace/prefect-env/bin/activate
prefect version
```


Finally, we start the Prefect server from the VM with the following:

```bash
source /mnt/pspace/prefect-env/bin/activate
prefect server start --host 0.0.0.0
```

This command sets the host to run on `0.0.0.0`, which will be accessible from an external network, so long as the ingress port is setup (see [Networking Configuration](#key-pair-generation-and-networking-configuration) for details).

The UI will be accesible at the following: http://[FLOATING_IP]:4200 .

## Workers, Work Pools, and Deployments

Prefect uses three concepts to run workflows:

1. Deployments are packaged versions of Prefect flows (simply just defined python functions that do things) that can be triggered remotely. They simply define what code to run and which pool should handle it.

2. Work pools are named queues where flow runs wait to be executed.

3. Workers are processes that watch a specific work pool; they grab flow runs from their assigned queue and execute them.

When a deployment is triggered (either manually or from a schedule), it creates a flow run that goes into a work pool's queue, where an assigned worker picks it up and executes the actual code. Multiple workers can watch the same pool to handle the work in parallel. In addition, separate pools can be made to handle different types of work.

### Work Pools

For this codebase, we use three dedicated work pools to segment different types of work. These are explained below:

1. pipeline-state-monitoring: This workpool will have an assigned deployment whose flow is dedicated to parsing the central sqlite database to monitor the state of the data pipeline for each dataset. In addition, it will pass information to an initializer script whose main job is to start appropriate flows for the datasets that need them. ***This workpool will have one assigned worker, and be triggered on a schedule.***

2. vm-runs: This workpool is reponsible for running any assigned flows on the VM itself. This workpool is for any lightweight task that the vm itself can manage. ***This workpool will have a concurrency limit of 2, and be triggered by deployments which have lightweight flows attached to them (e.g., downloads, file organization, etc.)***.

3. headless-runs: This workpool will handle all headless session launches, and, as a result, be responsible for all major CASA-related execution. ***This workpool will have a concurrency limit of 12, and will be triggered by deployments that make use of the CANFAR headless session interface (e.g., listobs, auto selfcalibration, imaging, etc.)***.

To create these workpools, the following statement is used:

```bash
prefect work-pool create work_pool_name_here -t process
```

### Workers

As mentioned above, we assign a worker to each pool with a limit on how many concurrent jobs they are allowed to perform at once. This is to help mitigate processing that is done on the VM specifically (in the case of the first two pools), and help mitigate how many headless sessions are running at once (for the Kueue system on CANFAR).

To set the concurrency limits for a pool, either use the flag at creation (as written above), or use the following:

```bash
prefect work-pool set-concurrency-limit work_pool_name_here X
```

where `X` is the number of concurrent tasks the worker is allowed to perform at once. Since the `headless-runs` workpool will be dedicated to launching headless sessions and waiting for the sessions to complete, a higher concurrency limit is appropriate.

### Deployments

Deployments are made via the [`prefect.yaml`](../../prefect/prefect.yaml) file and are deployed from the directory containing the `prefect.yaml` file as:

```bash
prefect deploy --all
```

## Starting Processes

Processes can be started on the VM, but only so long as the terminal sessions that run them are active. For instance, in a local VSCode session that uses Remote-SSH, `screen` must be used in order to keep those processes active, while the Remote SSH session has been disconnected. Use the following:

```bash
screen -S process_name
```

to open a `screen` session, and then:

```bash
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect server start --host 0.0.0.0
```

To start any workers for the work-pools:
```bash
export PREFECT_HOME=/mnt/pspace/prefect-data
source /mnt/pspace/prefect-env/bin/activate
prefect worker start --pool "pipeline-state-monitoring"
```

The use of CTRL+A,D is used to detach from the screen session. This will put it into the background. To view all currently running screen sessions:

```bash
screen -ls
```

and to re-attach a specific one:

```bash
screen -r process_name
```
