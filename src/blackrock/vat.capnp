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

interface Collection {
  # Represents a collection of items which may change over time. You can subscribe to update
  # notifications.

  # TODO(soon): Fill out.
  #
  # TODO(someday): Storage should support stored collections with indexes.

  interface Observer {
    # Allows observing changes to a collection, but cannot make changes.
  }
}

struct ObjectStorageCredentials {
  # TODO(soon): Credentials for accessing GCE, etc.
}

interface Restorer {
  restore @0 (sturdyRef :AnyPointer) -> (value :AnyPointer);
  release @1 (sturdyRef :AnyPointer);
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

interface Vat {
  # A machine, ready to serve.
  #
  # The master will call the methods below in order to tell the machine what it should do. Multiple
  # become*() method can be called to make the vat serve multiple purposes, but each individual
  # method cannot be called more than once.

  # TODO(now): Figure out how to allow worker vats to use storage restorers. Each vat should be its
  #   own SturdyRef sealing domain, probably. Maybe have a SealedRestorer interface where the vat
  #   proves its VatId to get access?

  becomeStorage @0 (credentials :ObjectStorageCredentials, siblings :Collection.Observer,
                    grainHostedRestorers :Collection.Observer,
                    gatewayRestorers :Collection.Observer)
                -> (rootZone :Storage.StorageZone,
                    storageRestorerForCoordinators :Restorer,
                    storageRestorerForGateways :Restorer,
                    storageRestorerForFrontends :Restorer);
  becomeWorker @1 () -> (worker :Worker.Worker);
  becomeCoordinator @2 (workers :Collection.Observer, storageRestorers :Collection.Observer)
                    -> (coordinator :Worker.Coordinator, grainHostedRestorer :Restorer);
  becomeGateway @3 (storage :Storage.StorageZone, storageRestorers :Collection.Observer,
                    frontends :Collection.Observer)
                -> (gateway :Gateway);
  becomeFrontend @4 (userStorage :Storage.StorageZone, storageRestorers :Collection.Observer)
                 -> (frontend :Frontend);
  becomeMongo @5 (siblings :Collection.Observer) -> (mongo :Mongo);

  shutdown @6 ();
  # Do whatever is necessary to prepare this machine for safe shutdown. Do not return until it's
  # safe.
}

interface Master {
  addVat @0 (vatId :VatId, vat :Vat);
}
