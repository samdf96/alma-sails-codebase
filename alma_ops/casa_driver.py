"""
casa_driver.py
--------------------
Script called by headless session creation to run CASA tasks.

*** This script only works in a valid CASA environment. ***

Usage:
------
# called via the following command
casa --logfile "$CASA_LOGFILE_PATH" -c "$CASA_DRIVER_SCRIPT_PATH" --json-payload "$JSON_PAYLOAD_PATH"
"""
# ruff: noqa: F821

import argparse
import json

# =====================================================================
# CLI Entry
# =====================================================================

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
