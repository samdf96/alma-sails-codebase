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

### Authentication Setup (For the Prefect Server)

The Python API- or CLI-based infrastructure to launch headless sessions for CANFAR's Kueue system needs authentication (see [canfar](https://github.com/opencadc/canfar)). If these headless sessions are launched from any CANFAR environment, the authentication is taken care of by the session itself (since users need to login to launch interactive sessions from the portal). If outside the session, one needs to run the following:

```bash
canfar auth login
```

The use of the `--force` flag may need to be added to force authentication. Choosing the CADC is the correct option, and login via CANFAR credentials will be brought up. This is necessary for Prefect to be able to launch jobs from the VM.