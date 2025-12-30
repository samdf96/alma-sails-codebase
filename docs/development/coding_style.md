# Coding Style - Preferences and Guide

This document will serve to outline my choices in coding style throughout this repository including how to document scripts, the styling choices for those pieces of documentation and general outlines for how things are written.

## Header Information

Headers refer to the initial pieces of text at the start of scripts. The main styling choice is the following general outline:

```python
"""
name_of_script.py
--------------------
Main descriptor of what name_of_script does.

Any additional information that the user should know would go here.

Usage:
------
Detailed information on how the user should use this script.
"""
```

Additionally, if there are specific requirements for the script, e.g., it can only run from an environment that has `casatools` installed, that information if prominently displayed.

## Imports

Imports are generally all organized at the top of the file underneath the header information, however, a `bootstrapping` script needs to be invoked to allow the python scripts to know where the `alma_ops/` directory lies. If any modules or functions are needed from `alma_ops/` for the script, then the following should be listed first in the general imports section:

```python
# ---------------------------------------------------------------------
# Bootstrap (allow importing alma_ops)
# ---------------------------------------------------------------------
from bootstrap import setup_path

setup_path()
```

This also means that `ruff` will invoke `E402` linting errors, as other imports will be below the `setup_path()` function. We can correct for this by putting a `# ruff: noqa: E402` line underneath the header information. In addition, if there are any functions that are called but not installed on the environment (i.e., `casa` tasks which are only called via headless sessions), adding `# ruff: noqa: F821` will negate the linting error from `ruff`.

All other import organization decisions is made by `ruff` itself and is put under a import comment header of the following:

```python
# ---------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------
```

## Flow Script Organization

All scripts under the `flow/` directory are deployments which are run by the Prefect server. Organization is as follows: Prefect tasks are first, followed by Prefect flows, and finally, any command line interfaces. The following are the comment headers to be used:

```python
# =====================================================================
# Prefect Tasks
# =====================================================================

# =====================================================================
# Prefect Flows
# =====================================================================

# =====================================================================
# CLI Entry
# =====================================================================
```

### Task/Flow Naming Conventions

For Prefect flows, easy and short naming is preferred and inlcudes `_flow` at the end of the function definition. The associated flow name is set to be easily understood from the UI:

```python
@flow(name="Download MOUS")
def download_mous_flow([...]):
    pass
```

The same applies for tasks, but without the `_tasks` nomenclature, since tasks don't need to be deployed like flows do:

```python
@task(name="Build Split Job Payload")
def build_split_job_payload([...]):
    pass
```

For all flows, logging information will be given at the top of the flow for all input parameters for the flow, so they can easily be tracked at the top of the Prefect logs:

```python
@flow(name="Download MOUS")
def download_mous_flow(
    mous_id: str,
    db_path: Optional[str] = None,
    download_dir: Optional[str] = None,
    weblog_dir: Optional[str] = None,
):
    """Download a single MOUS."""
    log = get_run_logger()

    # parsing input parameters
    log.info(f"[{mous_id}] Parsing input variables...")
    db_path = db_path or DB_PATH
    log.info(f"[{mous_id}] db_path set as: {db_path}")
    download_dir = download_dir or DATASETS_DIR
    log.info(f"[{mous_id}] download_dir set as: {download_dir}")
    weblog_dir = weblog_dir or SRDP_WEBLOG_DIR
    log.info(f"[{mous_id}] weblog_dir set as: {weblog_dir}")
```

Additionally, as listed in the code snippet above, any input arguments that are not required need to be set to `Optional[str] = None` to work correctly with the default values (`null`) in the Prefect deployment configuration file.

## Comment and Logging Styles (including docstrings)

For every logging statement contained within a Prefect task or flow, the logging statement will go through the local logger initialized by:

```python
log = get_run_logger()
```

Since most Prefect tasks and flows run on MOUSes, the general style of logging statement is the following:

```python
log.info(f"[{mous_id}] Logging message here")
```

This allows all logging messages to contain the `mous_id` for traceability. This also applies to any `log.warning` or `log.error` statements.

For any specific tasks and flows, docstrings are included. These can range from basic docstrings with a simple explanation, to those that fully explain each argument and return. These use the `numpy` style docstring format, an example is given:

```python
    """A brief summary of the function.

    Any additional information.

    Parameters
    ----------
    some_parameter : str | list[str]
        A brief description.

    Returns
    -------
    some_return : Path | list[Path]
        A brief description.
    """
```
