# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf1a0240d9d1e831b;
# Main storage schemas for Blackrock.

$import "/capnp/c++.capnp".namespace("blackrock");

using Storage = import "storage.capnp";
using OwnedAssignable = Storage.OwnedAssignable;
using OwnedVolume = Storage.OwnedVolume;
using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;

struct AccountStorage {
  # TODO(someday):
  # - Basic metadata.
  # - Quota etc.
  # - Opaque collection of received capabilities.

  grains @0 :List(GrainInfo);
  # All grains owned by the user.
  #
  # TODO(perf): Use a Collection here, when they are implemented.

  struct GrainInfo {
    id @0 :Text;
    state @1 :OwnedAssignable(GrainState);
  }
}

struct GatewayStorage {
  # TODO(someday):
  # - Incoming and outgoing SturdyRefs.
}

struct GrainState {
  union {
    inactive @0 :Void;
    # No worker is currently assigned to this grain.

    active @1 :Supervisor;
    # This grain is currently running on a worker machine.
    #
    # Upon loading the `GrainState` from storage and finding `active` is set, the first thing you
    # should do is call `keepAlive()` on this capability. If that fails or times out, then it
    # would appear that the grain is no longer running. Now we get into a complicated situation
    # where it's necessary to either convince the worker holding the grain to give it up or revoke
    # that worker's access to the grain state and volume entirely, but hopefully this is
    # infrequent.
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
