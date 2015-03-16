# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x95ec494d81e25bb1;

$import "/capnp/c++.capnp".namespace("blackrock");

using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using Grain = import "/sandstorm/grain.capnp";
using Storage = import "storage.capnp";

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface Worker {
  # Top-level interface to a Sandstorm worker node, which runs apps.

  newGrain @0 (package :Storage.Blob, actionIndex :UInt32, storage :Storage.StorageFactory)
           -> (grain :HostedGrain, grainState :Storage.OwnedAssignable(GrainState));
  # Start a new grain using the given package. `actionIndex` is an index into the package's
  # manifest's action table specifying the action to run.

  restoreGrain @1 (package :Storage.Blob, storage :Storage.StorageFactory,
                   grainState :Storage.OwnedAssignable(GrainState))
               -> (grain :HostedGrain);
  # Continue an existing grain.

  # TODO(now): Enumerate grains.
  # TODO(now): Resource usage stats.
}

interface HostedGrain {
  # "Admin" interface to a grain. Includes functionality that only the owner should be allowed to
  # access. Holding a live reference to HostedGrain does not necessarily mean the grain is running.

  getMainView @0 () -> (view :Grain.UiView);
  # Get the grain's main UiView. Starts up the grain if it is not already started.

  shutdown @1 () -> ();
  # Kills the running grain. The next call to getMainView() or attempt to restore a capability
  # hosted by this grain will restore it.

  setPackage @2 (package :Storage.Blob) -> ();
  # Switch the grain to a new package. Implies shutdown().

  backup @3 () -> (package :Storage.Blob);
  # Creates a zip of the grain and stores it to blob storage.
}

interface Coordinator {
  # Decides which workers should be running which apps.

  newGrain @0 (package :Storage.Blob, actionIndex :UInt32, storage :Storage.StorageFactory)
           -> (grain :HostedGrain, grainZone :Storage.OwnedAssignable(GrainState));
  # Create a new grain.
  #
  # Params:
  # `package`: The app package to use.
  # `actionIndex`: The index of the startup action to use, from those defined in the grain's
  #   manifest.
  # `storage`: Where to create the grain's per-instance storage.
  #
  # Results:
  # `grain`: The interface to the new grain.
  # `grainZone`: The StorageZone created using `storage`. This needs to be linked into the parent
  #   zone in order to persist long-term.
}

struct GrainState {
  union {
    inactive @0 :Void;
    # No worker is currently assigned to this grain.

    active @1 :HostedGrain;
    # This grain is currently running on a worker machine.
  }

  volume @2 :Storage.OwnedVolume;

  savedCaps @3 :Storage.OwnedCollection(SavedCap);
  struct SavedCap {
    id @0 :UInt64;
    cap @1 :AnyPointer;
  }
}
