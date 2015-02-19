# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x96022888188b4f2f;

$import "/capnp/c++.capnp".namespace("sandstorm::blackrock");

using ClusterRpc = import "cluster-rpc.capnp";
using Storage = import "storage.capnp";
using Worker = import "worker.capnp";
using Util = import "/sandstorm/util.capnp";

using VatId = ClusterRpc.VatId;
using Address = ClusterRpc.Address;
using SturdyRef = ClusterRpc.SturdyRef;

struct ObjectStorageCredentials {
  # TODO(soon): Credentials for accessing S3, GCS, etc.
}

interface Restorer(Ref) {
  restore @0 (sturdyRef :Ref) -> (value :AnyPointer);
  release @1 (sturdyRef :Ref);
}

interface MasterRestorer(Ref) {
  # Represents a Restorer that can restore capabilities for any owner. This capability should only
  # be given to the cluster master, which must then attenuate it for specific owners before passing
  # it on to said owners.

  getForOwner @0 (domain :SturdyRef.Owner) -> (attenuated :Restorer(Ref));
}

interface Gateway {
  # Gateway machines bridge between the cluster and the external network (usually the internet).
  # They bridge between different parameterizations of Cap'n Proto, serve as a firewall, and
  # provide a way for internal apps to make external requests which are explicitly prevented from
  # accessing internal machines (e.g. if an app requests to connect to some IP, we need to make
  # sure that IP is on the public internet, not internal; the best way to do that is to make
  # sure the connection is formed using a public network interface that can't even route to
  # internal IPs in the first place).

  # TODO(soon): Methods for:
  # - Sending / receiving general internet traffic. (In-cluster traffic is NOT permitted.)
  # - Making and accepting external Cap'n Proto connections and bridging those capabilities into
  #   the fold.

  # TODO(cleanup): Move to its own file.
}

interface Frontend {
  # Front-ends run the Sandstorm shell UI (a Meteor app). They accept HTTP connections proxied
  # from the Gateways.

  getHttpAddress @0 () -> (address :Address);
  # Get the address and port of the frontend's HTTP interface.

  # TODO(cleanup): Move this and Mongo stuff to a separate file frontend.capnp.
}

interface Mongo {
  getMongoAddress @0 () -> (address :Address, username :Text, password :Text);
}

interface MongoSibling {
  # TODO(soon): Information needed to set up replicas.
}

using Collection = Storage.Collection;

interface Machine {
  # A machine, ready to serve.
  #
  # When a new machine is added to the cluster, its Machine capability is given to the cluster
  # master via an appropriately secure mechanism. Only the master should ever hold this capability.
  #
  # The master will call the methods below in order to tell the machine what it should do. Multiple
  # become*() method can be called to make the machine serve multiple purposes.

  becomeStorage @0 (credentials :ObjectStorageCredentials,
                    siblings :Collection(Storage.StorageSibling).Cursor,
                    hostedRestorers :Collection(Restorer(SturdyRef.Hosted)).Cursor,
                    gatewayRestorers :Collection(Restorer(SturdyRef.Stored)).Cursor)
                -> (sibling :Storage.StorageSibling,
                    rootZone :Storage.StorageZone,
                    storageRestorer :MasterRestorer(SturdyRef.Stored));
  becomeWorker @1 () -> (worker :Worker.Worker);
  becomeCoordinator @2 (workers :Collection(Worker.Worker).Observer,
                        storageRestorers :Collection(Restorer(SturdyRef.Stored)).Cursor)
                    -> (coordinator :Worker.Coordinator,
                        hostedRestorer :MasterRestorer(SturdyRef.Hosted));
  becomeGateway @3 (storage :Storage.StorageZone,
                    storageRestorers :Collection(Restorer(SturdyRef.Stored)).Cursor,
                    frontends :Collection(Frontend).Observer)
                -> (gateway :Gateway,
                    externalRestorer :MasterRestorer(SturdyRef.External));
  becomeFrontend @4 (userStorage :Storage.StorageZone,
                     storageRestorers :Collection(Restorer(SturdyRef.Stored)).Cursor,
                     hostedRestorers :Collection(Restorer(SturdyRef.Hosted)).Cursor,
                     mongo :Collection(Mongo).Observer)
                 -> (frontend :Frontend);
  becomeMongo @5 (siblings :Collection(MongoSibling).Observer) -> (mongo :Mongo);

  shutdown @6 ();
  # Do whatever is necessary to prepare this machine for safe shutdown. Do not return until it's
  # safe.

  getResources @7 () -> Resources;

  struct Resources {
    # Manifest of hardware resources available to a particular machine.

    disks @0 :List(Disk);
    # List of disks attached to this machine.

    struct Disk {
      uuid @0 :Data;
      size @1 :UInt64;
    }

    hasInternet @1 :Bool;
    # Whether or not this machine is connected to the internet.

    # TODO(soon): CPU, RAM, etc.
  }
}

interface Master {
  # Default interface exported by the cluster master.
  #
  # TODO(someday): Have some sort of authentication process so that rogue machines cannot add
  #   themselves? Note that it's unclear how such authentication would work for machines booting
  #   from common read-only images; they have nothing to authenticate with.

  addMachine @0 (machine :Machine);
  # Submit a new machine for work.
}
