## Project Dependencies Management

This codebase is managed by `uv`. The dependencies can be viewed in the [pyproject.toml](../blob/master/pyproject.toml) file, and the associate lock file can be viewed in the [uv.lock](../master/uv.lock) file.

Details on how the Prefect server (maintained on the persistent volume mounted by the VM) runs with the same `uv` configuration is found in the following section [Prefect Server Configuration](#prefect-server-configuration).

## Prefect Server Configuration

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