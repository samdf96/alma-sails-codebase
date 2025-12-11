# ALMA-SAILS

Automated data pipeline for the ALMA-SAILS project.

## Quick start
```bash
# clone and install
git clone
cd alma-sails-codebase
uv sync
```

## Overview

The ALMA-Search for Asymmetry and Infall at Large Scales (ALMA-SAILS) is an archival ALMA-based project searching for so-called accretion streamers surrounding forming/formed protostars in publicly available datasets hosted on the ALMA Science Archive (ASA). This code base represents one student's approach on a full scale automated data pipeline following the following general sequence of steps: 

**Data ingest -> Data Verification -> Automated Self-calibration -> Automated Imaging** 

The documentation found in [docs/](./docs/) represents the instructions (written as a history) for setting up both a persistent Prefect server on a dedicated Virtual Machine (VM) in order to orchestrate all sequence of steps in the data pipeline automatically. The code base and associated documentation creates a centrally managed SQLite database which hosts all ALMA target metadata, along with the state of the pipeline for these aforementioned ALMA targets. The documentation also gives a complete history on the steps taken to configure both the VM with Prefect, and the project-space which has the necessary storage allocation to host the data and data processing.

## Project Structure
- `alma_ops/` - Core library (database querying functions, download execution, headless session launchers, etc.)
- `flows/` - Prefect orchestration workflows
- `scripts/` - Manual utility scripts
- `schemas/` - Database schemas

## Documentation

### Setup & Installation

- [Installation Guide](docs/setup/installation.md)
- [Project Space Configuration](docs/setup/project_space_setup.md)
- [VM Configuration](docs/setup/vm_space_setup.md)
- [Database Creation](docs/database_management/database_creation.md)

### Prefect Orchestration

- [Prefect Server Setup](docs/prefect/installation_and_setup.md)
- [Workers, Pools, and Deployments]()

### Pipeline Stages

- [Pipeline Overview]()
- [Data Ingest]()
- [Self-Calibration]()
- [Imaging]()

### Development

See [Development Guide]()

## License

MIT License

Copyright (c) 2025 Samuel Fielder

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
