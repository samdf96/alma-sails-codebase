## `project-space` Setup

### Directory Structure Basics

As mentioned in the [Installation](#installation) steps above, the `project-space` hosts this code base. The main aspects of the directory structure found in the `project-space` is outlined below:

```
.
├── SRDP_weblogs
├── alma-sails-codebase
├── auto_selfcal-1.3.1
├── datasets
    ├── uid___A001_X1465_X15b
        ├── casa-20251124-220938-splits.log
        ├── casa-20251125-014504-listobs.log
        ├── uid___A001_X1465_X15b_listobs.json
        ├── uid___A001_X1465_X15b_splits.json
        ├── uid___A002_Xe32bed_Xbc5.ms
        ├── uid___A002_Xe32bed_Xbc5.ms.listobs.txt
        ├── uid___A002_Xe64b7b_X1be6d.ms
        ├── uid___A002_Xe64b7b_X1be6d.ms.listobs.txt
        └── splits
            ├── uid___A002_Xe32bed_Xbc5_Serp_15.ms
            ├── uid___A002_Xe32bed_Xbc5_Serp_15.ms.listobs.txt
            └── ...
    └── ...
├── datasets_raw
├── db
├── phangs_imaging_scripts-3.1
├── schemas
```

The three main code bases that are found within the `project-space` are the following:

* `alma-sails-codebase` : this repository
* `auto_selfcal-1.3.1` : The automated self-calibration scripts provided by John Tobin at the following: [auto_selfcal](https://github.com/jjtobin/auto_selfcal).
* `phangs_imaging_scripts-3.1` : The automated post-processing and science-ready data product pipeline provided by the PHANGS Team at the following: [phangs_imaging_scripts](https://github.com/akleroy/phangs_imaging_scripts).

The remainder of the main directories listed in the tree structure above are the following:

* `datasets` : main directory to hold all MOUS datasets. An example structure is given above.
* `SRDP_Weblogs` : directory to hold all weblogs produced by the science-ready data products (SRDP) service provided by the NRAO, found at the following: [Science Ready Data Products](https://science.nrao.edu/srdp). Specifically used to gather restored calibrated measurement sets for newer ALMA data, bypassing the requirement to restore calibration from raw data provided by the ASA.
* `datasets_raw` : semi-temporary directory to hold all raw data downloaded from the ASA. Specifically used for those datasets which do not have the option to be restored by the SRDP service.
* `db` : main directory for the SQLite database meant to hold all metadata and logging for the alma-sails data pipeline.
* `schemas` : semi-temporary directory to hold schemas for the SQLite database. Mainly used for tracking changes to the database structure as the code is being developed.

### Authentication Setup

The Python API- or CLI-based infrastructure to launch headless sessions for CANFAR's Kueue system needs authentication (see [canfar](https://github.com/opencadc/canfar)). If these headless sessions are launched from any CANFAR environment, the authentication is taken care of by the session itself (since users need to login to launch interactive sessions from the portal). If outside the session, one needs to run the following:

```bash
canfar auth login
```

The use of the `--force` flag may need to be added to force authentication. Choosing the CADC is the correct option, and login via CANFAR credentials will be brought up.