# Blackrock

Blackrock allows a cluster of machines to act as a single Sandstorm instance.

Goals:

* Treat anywhere from 2 to ~2^16 colocated machines (or VM instances) as a single Sandstorm instance with a user interface essentially the same as a single-machine Sandstorm instance.
* Enforce per-user (and perhaps per-app) quotas on storage space and RAM usage.
* Allow machines to be added to or removed from the cluster dynamically, with machine roles automatically assigned and updated as needed.
* Avoid any user disruption or data loss when a machine dies.
* Migrate grains between machines to balance load and optimize resource sharing (e.g. sharing of application binaries).
* Mitigate sandbox breakouts by avoiding colocation of suspicious apps with high-value targets, monitoring hosts for suspicious activity, and periodically wiping and restarting individual machines.
* Take advantage of cheap object storage where available.
* Integrate with common legacy enterprise infrastructure like LDAP, Active Directory, etc.

Non-goals:

* Allowing geographically disperate machines to act as a single instance. Blackrock is optimized for machines living in a single datacenter on a single LAN. Sandstorm's usual federation facilities should be utilized to move apps between clusters.

## Machine roles

All machines in a Blackrock cluster boot the same read-only operating system image which contains all the Blackrock platform software. A machine will be assigned to one or more roles after startup by the cluster master.

The machine roles are as follows.

### Master

Each cluster has a single master machine. The master's responsibility is to assign roles to all other machines. The master monitors resource usage across all machines and dynamically decides where resources are needed. If machines have heterogenous specs, the master shall take this into account when choosing their role -- e.g. a machine with tons of RAM should be a worker, and a machine with tons of disk should be storage.

The master is responsible for updating all other machines about which peers they should be talking to. E.g. when a new storage node is added, the master will update all other machines that might need to use storage so that they can add the new node to their load-balacing pool.

The master machine is not on the critical path for serving any individual request. If it dies, the rest of the cluster can continue operating under the most-recently-assigned roles while waiting for the master to come back up.

### Storage

Storage nodes provide the interface to all persistent storage. Typically, the storage node does not literally operate the disks on which data is stored, but rather acts as a proxy to some traditional object storage system like S3. To distinguish between these storage nodes and actual physical storage, prefer the word "disks" for the latter.

The purpose of this proxy (rather than having other machines go directly to "disk") is:

* Implement a capability-based object graph storage interface based on Cap'n Proto's persistence layer.
* Implement per-object encryption, such that the actual disks need not be trusted for privacy. Each object shall have its own encryption key, and those keys themselves shall be stored as part of the SturdyRefs used to access the objects. Since these SturdyRefs are themselves typically stored in other (encrypted) objects, it becomes impossible to access any object without having followed the object graph from some base capability held by the user. Those base capabilities can theoretically be stored encrypted using the user's GPG key for "perfect" encrypted storage.
* Protect any credentials needed to access disks, e.g. S3 credentials.

Possible back-end "disk" options might include:

* Amazon S3
* Google Cloud Storage
* Tahoe-LAFS
* A distributed filesystem utilizing all disks in the local cluster.

### Workers

A worker machine runs grains as ordered by a coordinator (see below).

Due to the possibility of sandbox breakouts, worker machines should always be treated with suspicion. Use of Cap'n Proto capability-based security throughout the Blackrock cluster ensures that a compromised worker machine cannot access anything beyond the grains that happened to be running on it. Worker machines should be cycled out of rotation and wiped on a regular basis to prevent any malicious process from lingering. Worker machines should additionally be monitored for statistically suspicious activity and taken out of rotation immediately when any is seen.

Each grain's storage will be mounted from a block device implemented in userspace via the "nbd" driver. The userspace daemon implementing the block device syncs blocks back to storage. This allows grains with lots of data (e.g. a music library) to start up quickly from cold state as we can pull in blocks on-demand. It also allows us to keep long-term storage close to current so that machine failure of a worker doesn't lead to excessive data loss. Meanwhile, the kernel implements a well-tested caching layer on top of this block device which minimizes runtime overhead once pages are cached. We can explore various heuristics to optimize initial load time, e.g. keeping track of blocks that are usually loaded at startup and making sure to start reading them from storage right away. Note that nbd implements the "trim" command, which allows us to keep track of which blocks are actually used by the filesystem and avoid storing others.

### Coordinators

Coordinators tell workers what to do. To launch a new grain, or start up one that isn't currently running, you must contact a coordinator. It will then find an appropriate worker to which to delegate.

Each coordinator keeps track of some set of workers. These sets can be overlapping, but don't have to be -- you could have two coordinators each monitoring the whole pool of workers, or each monitoring half. A request to start up a grain can be delivered to any coordinator; callers should load-balance among them.

The coordinator monitors resource usage across its workers and issues orders to migrate grains between machines as-needed. The coordinator should take into account the typical resource footprint of each app and also the "suspiciousness" of the app. Suspicious apps should be segregated from security-critical apps to help mitigate the effects of sandbox breakouts.

There will be a lot of room for development of algorithms and heuristics here.

### Shells

Shell machines run the Sandstorm shell UI, a Meteor app.

### Mongo

Unfortunately, the Sandstorm shell currently depends on Mongo as a database. Mongo machines will run this database, with one master and the rest being slaves.

The database will be synced back to object storage as well.

Long-term, we should seek to replace Mongo with our own storage interface.

### Gateways

Gateways bridge to the public internet (or to the broader corporate network outside of the Sandstorm cluster). This breaks down into a few jobs:

* Run nginx to receive incoming HTTP requests, terminate SSL on those requests, and forward them to the shell.
* Implement a Cap'n Proto interface for establishing and accepting TCP connections as well as engaging in UDP traffic with the outside world. Such connections will be prohibited from connecting to other machines inside the cluster. This capability can then be handed off to a device driver in order to give it *external* network access without giving it any internal network access.
* Proxy Cap'n Proto itself to the outside world. The internal network uses a different parameterization of the Cap'n Proto protocol than the public internet will, so when capabilities pass across this boundary, they must be actively proxied. This also gives us a chance to keep track of the existence of external capabilities separately from internal ones and hide the internal SturdyRef representation from external eyes (as an extra layer of security).

## Networking / Cap'n Proto considerations

A Blackrock cluster uses a custom parameterization of the Cap'n Proto RPC protocol designed and optimized for the particular use case.

### Transport

Internal communications will likely be characterized as a fully-connected mesh, with nearly every machine sending occasional messages to every other machine. Since we assume a LAN, we can optimize for this by using UDP rather than TCP. Packet loss will be rare, while UDP will allow us to avoid a TCP handshake every time a machine wants to talk to another machine to which it is not already connected.

Moreover, this model will make it easier to be more resilient to temporary network hiccups. Dropping a connection can be fairly disruptive for Cap'n-Proto-based software since all capabilities that haven't been persisted are lost. With UDP we can easily design a policy that allows waiting a bit for the network to come back.

### Crypto

Each vat is identified by a unique "vat ID". A vat ID is actually an ed25519 public key generated by the vat itself -- the respective private key should never leave the machine on which it was generated, and should be discarded whenever the machine as a whole is wiped. A vat can prove itself to be the owner of a particular vat ID by signing a challenge.

In most cases, packet encryption is probably not necessary on a Blackrock instance's internal network, as high-end network hardware can typically be trusted to deliver packets only to their rightful destinations. However, if encryption is desired, the obvious way to do it is via a curve25519 key exchange. Essentially, two vats can agree on a shared secret based only on knowning each other's vat IDs (and knowing each's own private key). Thus they can start sending each other traffic without any handshake, which plays nicely with our UDP transport.

### Persistent caps (SturdyRefs)

Within the Blackrock network, SturdyRefs come in a limited number of varieties:

* Storage refs point directly to objects in storage. Storage nodes can restore these refs.
* Grain refs point to capabilities hosted within particular grains. Coordinators can restore these refs.
* External refs point to capabilities on the public internet. Gateways can restore these refs.
* Ephemeral refs point to capabilities hosted on some particular vat, and can only be restored by that vat. These caps are inherently short-lived because vats themselves are short-lived (due to frequent rotation). They can, however, survive reboots and process crashes, making them a bit sturdier than live refs.

Every SturdyRef is "sealed" to the "trust zone" of the vat which requested its creation, and can only be restored by that same trust zone. This prevents bits leaked across trust boundaries from being useful. Each vat is its own trust zone, but there are additionally four special zones: storage, gateways, coordinators, and shells. Vats in each of these groups have the ability to save SturdyRefs such that any other member of the group can later restore them.

### Log sink

All other machines shunt logs here for analysis.

## Implementation plan

### Observations

We can build a minimally-viable system without implementing the storage component. Instead, we will temporarily rely on vats being long-lived. Each grain will reside on a particular worker machine for the long haul. If a worker runs out of storage, we can just add more disks to it (this is easy in GCE). Users can also relocate grains by simply downloading a zip and re-uploading.

If vats are long-lived, then we don't initially need the master. We can initialize vats manually.

Coordinators may not be necessary initially either. The shell can implement a very simple round-robin and just remember which worker vat owns each grain.

Early on, gateways need only run nginx. The IP proxy functionality is only needed once we implement device drivers, and the Cap'n Proto proxy is not needed until we want to start supporting Cap'n-Proto-based federation.

The gateway, shell, and Mongo can be deployed manually early on.

Cap'n Proto level 3 RPC (the ability to pass off capabilities from machine to machine) is largely an optimization and therefore is also not urgent to implement.

Cap'n Proto level 2 RPC is probably necessary. We will need to save some sort of handle to each grain which can be stored in Mongo, and that might as well be a SturdyRef.

### Order

1. Implement level 2 RPC in Cap'n Proto.
2. Implement worker.
   * Just an RPC wrapper around the exsiting supervisor.
   * Every grain gets a unique uid for another layer of sandboxing.
   * cgroups to track memory usage.
   * Loopback block devices just backed by disk.
3. Hook shell to call out to workers as an alternative to invoking sandstorm-supervisor directly.
4. **LAUNCH BETA**
5. Implement level 3 RPC.
6. Implement storage.
7. Implement coordinators.
8. Migrate grains to new storage layer.
9. **LAUNCH OPEN SIGNUPS**
10. Implement master.
11. Migrate gateways, shells, and Mongos to be master-controlled.
12. **RELEASE ENTERPRISE TOOLS BETA**
