### Current Development Setup

I use the CANFAR Science Platform for most of my daily workflow. I currently have a project space hosted on `/arc/projects` named `ALMA-SAILS`. This will be referenced below as the `project-space`. As mentioned above, I also have a dedicated VM, the main goal of which is to host the Prefect server for project orchestration. This will be referred to as the VM.This should be done at the `project-space` level, since the current configuration (see [config.py](../master/alma_ops/config.py)) is relative to the `project-space`. For more complex setup, edit the `config.py` file with the intended directory structure wanted.

## Project Dependencies Management

This codebase is managed by `uv`. The dependencies can be viewed in the [pyproject.toml](../blob/master/pyproject.toml) file, and the associate lock file can be viewed in the [uv.lock](../master/uv.lock) file.

Details on how the Prefect server (maintained on the persistent volume mounted by the VM) runs with the same `uv` configuration is found in the following section [Prefect Server Configuration](#prefect-server-configuration).