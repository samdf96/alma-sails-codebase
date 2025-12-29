"""
casa_driver.py
--------------------
Only works in a *CASA environment*, casatask or casatools imports not needed.
This script is intended to be used as the following from a canfar.sessions create() function:

sessions.create(
    name=job_name
    image=SOME_CASA_IMAGE
    cmd="casa",
    args="--logfile [logfilepath] -c [casa_driver_filepath] --json-payload [jsonfilepath]
)
"""
# ruff: noqa: F821

import argparse
import json

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Executes casa tasks according to supplied json payload."
    )
    parser.add_argument("--json-payload", required=True)
    args = parser.parse_args()

    # opens the json payload file
    with open(args.json_payload, "r") as f:
        config = json.load(f)

    # executes all tasks given
    for task in config["tasks"]:
        name = task["task"]

        if name == "split":
            split(
                vis=task["vis"],
                outputvis=task["outputvis"],
                field=task["field"],
                spw=task["spw"],
                datacolumn=task.get("datacolumn", "data"),
            )

        elif name == "listobs":
            listobs(
                vis=task["vis"],
                listfile=task["listfile"],
                verbose=task.get("verbose", True),
            )
