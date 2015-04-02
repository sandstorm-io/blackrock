# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf1a0240d9d1e831b;
# Main storage schemas for Blackrock.

$import "/capnp/c++.capnp".namespace("blackrock");

using Storage = import "storage.capnp";
using OwnedAssignable = Storage.OwnedAssignable;
using OwnedVolume = Storage.OwnedVolume;

struct StorageRoot {
  # The root of the storage system is an Assignable(StorageRoot).

  # TODO(someday):
  # - Collection of users.
  # - Collection of apps.
  # - Gateway storage.
  # - Others?
}

struct AccountStorage {
  # TODO(someday):
  # - Basic metadata.
  # - Quota etc.
  # - Opaque collection of received capabilities.

  grains @0 :List(OwnedAssignable(GrainState));
  # All grains owned by the user.
  #
  # TODO(perf): Use a Collection here, when they are implemented.
}

struct GatewayStorage {
  # TODO(someday):
  # - Incoming and outgoing SturdyRefs.
}

struct GrainState {
  union {
    inactive @0 :Void;
    # No worker is currently assigned to this grain.

    active @1 :Capability;
    # This grain is currently running on a worker machine.
    #
    # Upon loading the `GrainState` from storage and finding `active` is set, the first thing you
    # should do is call `checkHealth()` on this capability. If that fails or times out, then it
    # would appear that the worker running this grain no longer exists or is no longer responsive.
    #
    # TODO(cleanup): This should be type `HostedGrain` from `worker.capnp` but that seems to
    #   create a cyclic dependency between capnp files.
  }

  volume @2 :OwnedVolume;

  savedCaps @3 :List(SavedCap);
  # TODO(perf): Use a Collection here, when they are implemented.

  struct SavedCap {
    token @0 :Data;
    # Token given to the app. (We can't give the app the raw SturdyRef because it contains the
    # encryption key which means the bits are powerful outside the context of the app.)

    cap @1 :Capability;
  }
}
