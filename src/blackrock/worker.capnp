# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x95ec494d81e25bb1;

$import "/capnp/c++.capnp".namespace("sandstorm::blackrock");

using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using Grain = import "/sandstorm/grain.capnp";
using Storage = import "storage.capnp";

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface Worker {
  # Top-level interface to a Sandstorm worker node, which runs apps.

  newGrain @0 (package :Storage.Blob, actionIndex :UInt32, zone :Storage.StorageZone)
           -> (grain :HostedGrain, grainState :Storage.Assignable);
  # Start a new grain using the given package. `actionIndex` is an index into the package's
  # manifest's action table specifying the action to run.

  restoreGrain @1 (package :Storage.Blob, grainState :Storage.Assignable, zone :Storage.StorageZone)
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
  #
  # The coordinator can also field "restore" requests for grainHosted SturdyRefs.

  newGrain @0 (package :Storage.Blob, actionIndex :UInt32, parentZone :Storage.StorageZone)
           -> (ui :Grain.UiView, grainZone :Storage.StorageZone);
}

struct GrainState {
  union {
    frozen @0 :Void;
    # No worker is currently assigned to this grain.

    thawed :group {
      # A worker is currently assigned to this grain and has transferred some or all of its data
      # to local disk, but as yet no changes have been made to that data.
      #
      # It is safe to transition from this mode to frozen even while the worker is offline.

      grain @1 :HostedGrain;
    }

    active :group {
      # A worker is actively executing this grain, and its local copy has changes not yet saved to
      # storage.
      #
      # If the worker is currently unreachable, you must wait for it to come back up or risk data
      # loss.

      grain @2 :HostedGrain;

      # TODO(someday): Also keep a modification journal tracking recent writes, in case of machine
      #   failure.
    }
  }

  currentContent @3 :Snapshot;
  struct Snapshot {
    timestamp @0 :Timepoint;
    # Time at which this snapshot was current.

    zip @1 :Storage.Blob;
    # ZIP backup of the grain.

    # TODO(someday): Break out large files into separate blobs.
  }

  # TODO(someday): Archived snapshots for time travel.

  # TODO(someday): Probably need to store outbound capabilities explicitly so storage system can
  #   track.
}
