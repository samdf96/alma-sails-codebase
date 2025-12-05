# ALMA-SAILS

Data pipeline code base for the ALMA-SAILS project.

- [ALMA-SAILS](#alma-sails)
  - [Overview](#overview)
    - [Current Development Setup](#current-development-setup)
  - [Installation](#installation)
  - [`project-space` Setup](#project-space-setup)
    - [Directory Structure Basics](#directory-structure-basics)
    - [Authentication Setup](#authentication-setup)
  - [VM Setup](#vm-setup)
    - [Key-Pair Generation and Networking Configuration](#key-pair-generation-and-networking-configuration)
    - [Instance Creation](#instance-creation)
    - [Persistent Volume Creation](#persistent-volume-creation)
    - [Mounting the Science Platform's /arc](#mounting-the-science-platforms-arc)
  - [Project Dependencies Management](#project-dependencies-management)
  - [Prefect Server Configuration](#prefect-server-configuration)


## Overview

The ALMA-Search for Asymmetry and Infall at Large Scales (ALMA-SAILS) is an archival ALMA-based project searching for so-called accretion streamers surrounding forming/formed protostars in publicly available datasets hosted on the ALMA Science Archive (ASA). This code base represents one student's approach on a full scale automated data pipeline following the following general sequence of steps: Data ingest -> Data Verification -> Automated Self-calibration -> Automated Imaging. 

This README supplies the instructions for setting up a persistent Prefect server on a dedicated Virtual Machine in order to orchestrate all sequence of steps automatically, for each ALMA imaging target supplied and centrally managed with a SQLite database. It also gives a full and complete history on the steps to configure both the cloud computing platform that I use, along with the VM allocation I have been setup with.

My intention was to write a user guide on how to setup, configure, and run the data pipeline, however, this reads more as a history of how the project was configured:

 ***This README will read more like a history rather than a set of instructions.***

 This may be updated in the future to read more like a set of instructions rather than a history of my learning to setup these systems.

### Current Development Setup

I use the CANFAR Science Platform for most of my daily workflow. I currently have a project space hosted on `/arc/projects` named `ALMA-SAILS`. This will be referenced below as the `project-space`. As mentioned above, I also have a dedicated VM, the main goal of which is to host the Prefect server for project orchestration. This will be referred to as the VM.

## Installation

Simply run the following:

```
git clone git@github.com:samdf96/alma-sails-codebase.git
```

This should be done at the `project-space` level, since the current configuration (see [config.py](../blob/master/alma_ops/config.py)) is relative to the `project-space`. For more complex setup, edit the `config.py` file with the intended directory structure wanted.

## `project-space` Setup

### Directory Structure Basics

As mentioned in the [Installation](#installation) steps above, the `project-space` hosts this code base. The main aspects of the directory structure found in the `project-space` is outlined below:

```
.
├── SRDP_weblogs
├── alma-sails-codebase
├── auto_selfcal-1.3.1
├── datasets
    ├── datasets/uid___A001_X1465_X15b
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

## VM Setup

I use an OpenStack allocation that is running on the Arbutus Cloud Computing Node, hosted at the University of Victoria. This is accessible from the main CANFAR menu: [CANFAR](https://www.canfar.net/en/). Once configured by CANFAR admins, credentials are identical to the ones used on CANFAR itself.

My current allocation consists of the following:

* 4 vCPUs
* 16 GB RAM
* Volume Storage of 100GB
* Max Instance Number of 4

Since this is only running the project orchestration layer of the ALMA-SAILS project, minimal allocation is acceptable. I refer to this allocation as the VM in this README.

### Key-Pair Generation and Networking Configuration

Only one key-pair can be *initially* associated for any created instance running on OpenStack. Key-Pair generation is done through the `Compute -> Key Pairs` menu on OpenStack, and for each key-pair created, a `[key-pair-name].pem` file is created. This is copied into the `~/.ssh` directory of whichever client is used to `ssh` into the server space. Once the instance exists, the addition of any other keys is possible by appending them to the  `authorized_keys` file in the `~/.ssh` directory of the VM (not tested yet!!!).

Additionally, the default Security Group for the OpenStack allocation does not allow outside access (at least to my knowledge). Rules need to be added to the Ingress Direction to allowing for `ssh` (and ping, if wanted) access to the VM. Rules can be managed through `Network -> Security Groups -> Manage Rules (default)`. I've added the following two rules for the Ingress direction to allow ping access and `ssh` access:

* Ping Access:
  * Direction: Ingress
  * Ether Type: IPv4
  * IP Protocol: ICMP
  * Port Range: Any
  * Remote IP Prefix: 0.0.0.0/0
* `ssh` Access:
  * Direction: Ingress
  * Ether Type: IPv4
  * IP Protocol: TCP
  * Port Range: 22 (ssh)
  * Remote IP Prefix: 0.0.0.0/0

Since Prefect uses the 4200 port by default for its API and UI url, we will also set an Ingress direction rule for this port, as the following:

* Prefect UI Access:
  * Direction: Ingress
  * Ether Type: IPv4
  * IP Protocol: TCP
  * Port Range: 4200
  * Remote IP Prefix: 0.0.0.0/0

More information on the Prefect Server setup can be found in [Prefect Server Configuration](#prefect-server-configuration).

### Instance Creation

Creating an instance is relatively simple in the `Compute -> Instance` menu. The current configuration uses the `Ubuntu-24.04-Noble-x64-2024-06` image, as it's the newest Ubuntu-based image presently available as a preset. Additionally, the compute resources chosen is a subset of the allocation: `c1-7.5gb-36`, meaning I get a single vCPU, 7.5GB of RAM, 20GB on the root disk (`/`), as well as 36GB on an ephemeral disk (attached as `/dev/vdb` by default). Both the default ALMA-SAILS-network interface, and the IPv6-GUA are selected, although the IPv6-GUA interface is never used (by me at least). The default security group (with the appended rules listed in [Network Configuration](#key-pair-generation-and-networking-configuration)) is selected, along with the key-pair generated for the `project-space`.

For any internal interfaces (of which we have appended new rules for) to talk to any external networks, a Floating IP needs to be generated and attached to the instance. In the `Compute -> Instance` menu, a drop down is provided to attach a Floating IP to a running instance. This will be the externally facing IP to log into.

Logging into the VM is relatively simple with the following[^1]:

```bash
ssh -i ~/.ssh/[key-pair-name].pem ubuntu@[FLOATING_IP]
```

The default user for any Ubuntu image is `ubuntu`. The use of the default name is suggested for any VM use.

I've also set an `ssh` alias for this host, through the `config` file found in `~/.ssh`. Here is the template to set this up:

```
Host prefect-vm
    HostName [FLOATING_IP] 
    User ubuntu
    IdentityFile ~/.ssh/[key-pair-name].pem
```

Then I log into the VM through the following simpler command:

```bash
ssh prefect-vm
```

[^1]: If at any point the VM is deleted and re-created, the server fingerprint will have changed. This means the host identification will have changed, and the key sent by the VM will be not be accepted by the client. Regeneration of this host key is required; the old key can be removed with the following: `ssh-keygen -f '~/.ssh/known_hosts' -R '[FLOATING_IP]'`. When attempting to `ssh` at any following time, a new `known_hosts` entry will be created.

### Persistent Volume Creation

The Prefect server requires a location to host its own internal database. The root disk that is created for the VM is persistent, so long as the instance exists. If the instance is ever deleted, the disk disappears forever. This prompts the created of a persistent disk space not contained withing the root disk for the Prefect server to run on, such that if anything happens to the running VM, the internal data of the Prefect server will be saved.

A new Cinder volume was created through the `Volumes -> Volumes` menu in OpenStack. I chose 50GB as the disk size, and subsequently mounted it to the running instance. It's attach point was set as `/dev/vdc/`. I formatted this disk with the `ext4` format via the following:

```bash
sudo mkfs.ext4 /dev/vdc/
```

And mounted the volume with the following:

```bash
sudo mkdir /mnt/pspace/
sudo mount /dev/vdc /mnt/pspace
```

To view all the disks associated with the instance, and their associated mount points:

```bash
lsblk
```

Finally, we need to change the ownership of the mounted volume, so that the default user can write to the volume (by default only root has access).

```bash
sudo chown -R ubuntu:ubuntu /mnt/pspace
```

### Mounting the Science Platform's /arc

Since the `alma-sails-codebase` is the hub for all the code provided for the project, accessing the `project-space` from outside any internal CANFAR environment is necessary (i.e., the Prefect server space running on the VM node). I used `sshfs` to mount the entire of the Science Platform's `/arc` space, which itself includes the `project-space`.

First, `ssh` keys need to be generated in order to mount `/arc`. This is done from the VM through the following:

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/canfar_key
```

Then we copy the public key to the `authorized_keys` file found on the Science Platform's `~/.ssh` folder. Then we mount `/arc` with the following:

```bash
mkdir -p ~/canfar_arc
sshfs \
  -p 64022 \
  -o IdentityFile=/home/ubuntu/.ssh/canfar_key \
  -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=10 \
  [username]@ws-uv.canfar.net:/ \
  /home/ubuntu/canfar_arc
```

## Project Dependencies Management

This codebase is managed by `uv`. The dependencies can be viewed in the [pyproject.toml](../blob/master/pyproject.toml) file, and the associate lock file can be viewed in the [uv.lock](../blob/master/uv.lock) file.

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
