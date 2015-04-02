# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x95ec494d81e25bb1;

$import "/capnp/c++.capnp".namespace("blackrock");

using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using Grain = import "/sandstorm/grain.capnp";
using Storage = import "storage.capnp";
using StorageSchema = import "storage-schema.capnp";
using Package = import "/sandstorm/package.capnp";

using GrainState = StorageSchema.GrainState;

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface Worker {
  # Top-level interface to a Sandstorm worker node, which runs apps.

  newGrain @0 (package :PackageInfo,
               command :Package.Manifest.Command,
               storage :Storage.StorageFactory)
           -> (grain :HostedGrain, grainState :Storage.OwnedAssignable(GrainState));
  # Start a new grain using the given package. `actionIndex` is an index into the package's
  # manifest's action table specifying the action to run.

  restoreGrain @1 (package :PackageInfo,
                   command :Package.Manifest.Command,
                   storage :Storage.StorageFactory,
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

  setPackage @2 (package :PackageInfo) -> ();
  # Switch the grain to a new package. Implies shutdown().

  backup @3 () -> (blob :Storage.Blob);
  # Creates a zip of the grain and stores it to blob storage.

  checkHealth @4 ();
  # If the grain is operating normally, returns. Otherwise, throws an exception of type FAILED
  # or OVERLOADED.
}

interface Coordinator {
  # Decides which workers should be running which apps.

  newGrain @0 (package :PackageInfo,
               command :Package.Manifest.Command,
               storage :Storage.StorageFactory)
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

struct PackageInfo {
  id @0 :Data;
  # First half of SHA-256 hash of the original package file.
  #
  # TODO(someday): Switch to BLAKE2b?

  volume @1 :Storage.Volume;
  # Read-only volume containing the unpacked package.
}
