# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x96022888188b4f2f;

$import "/capnp/c++.capnp".namespace("blackrock");

using ClusterRpc = import "cluster-rpc.capnp";
using Storage = import "storage.capnp";
using StorageSchema = import "storage-schema.capnp";
using Worker = import "worker.capnp";
using Util = import "/sandstorm/util.capnp";

using VatId = ClusterRpc.VatId;
using Address = ClusterRpc.Address;
using SturdyRef = ClusterRpc.SturdyRef;
using Restorer = ClusterRpc.Restorer;
using BackendSet = ClusterRpc.BackendSet;

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

interface Machine {
  # A machine, ready to serve.
  #
  # When a new machine is added to the cluster, its Machine capability is given to the cluster
  # master via an appropriately secure mechanism. Only the master should ever hold this capability.
  #
  # The master will call the methods below in order to tell the machine what it should do. Multiple
  # become*() method can be called to make the machine serve multiple purposes. Calling the same
  # become*() method twice, however, only updates the existing instance of that role and returns
  # the same capabilities as before.
  #
  # This interface is intentionally designed such that the master machine can perform its duties
  # without ever actually parsing any of the response messages. Everything the master does --
  # introducing machines to each other -- can be expressed via pipelining. This implies that it is
  # not possible to confuse or compromise the master machine by sending it weird messages. In the
  # future we could even literally extend the VatNetwork to discard incoming messages.

  becomeStorage @0 ()
                -> (sibling :Storage.StorageSibling,
                    rootSet :Storage.StorageRootSet,
                    storageRestorer :MasterRestorer(SturdyRef.Stored),
                    storageFactory :Storage.StorageFactory,
                    siblingSet: BackendSet(Storage.StorageSibling),
                    hostedRestorerSet: BackendSet(Restorer(SturdyRef.Hosted)),
                    gatewayRestorerSet: BackendSet(Restorer(SturdyRef.External)));
  becomeWorker @1 () -> (worker :Worker.Worker);
  becomeCoordinator @2 ()
                    -> (coordinator :Worker.Coordinator,
                        hostedRestorer :MasterRestorer(SturdyRef.Hosted),
                        workerSet: BackendSet(Worker.Worker),
                        storageRestorerSet: BackendSet(Restorer(SturdyRef.Stored)));
  becomeGateway @3 (storage :Storage.Assignable(StorageSchema.GatewayStorage))
                -> (gateway :Gateway,
                    externalRestorer :MasterRestorer(SturdyRef.External),
                    storageRestorers :BackendSet(Restorer(SturdyRef.Stored)),
                    frontends :BackendSet(Frontend));
  becomeFrontend @4 (userStorage :Storage.Collection(StorageSchema.AccountStorage))
                 -> (frontend :Frontend,
                     storageRestorerSet: BackendSet(Restorer(SturdyRef.Stored)),
                     storageRootSet: BackendSet(Storage.StorageRootSet),
                     storageFactorieSet: BackendSet(Storage.StorageFactory),
                     hostedRestorerSet: BackendSet(Restorer(SturdyRef.Hosted)),
                     mongoSet: BackendSet(Mongo));
  becomeMongo @5 () -> (mongo :Mongo, siblingSet :BackendSet(MongoSibling));

  shutdown @6 ();
  # Do whatever is necessary to prepare this machine for safe shutdown. Do not return until it's
  # safe.
}
