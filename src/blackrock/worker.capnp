# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x95ec494d81e25bb1;

$import "/capnp/c++.capnp".namespace("blackrock");

using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using Grain = import "/sandstorm/grain.capnp";
using Storage = import "storage.capnp";
using StorageSchema = import "storage-schema.capnp";
using Package = import "/sandstorm/package.capnp";
using Util = import "/sandstorm/util.capnp";

using GrainState = StorageSchema.GrainState;

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface Worker {
  # Top-level interface to a Sandstorm worker node, which runs apps.

  newGrain @0 (package :PackageInfo,
               command :Package.Manifest.Command,
               storage :Storage.StorageFactory)
           -> (grain :Supervisor, grainState :Storage.OwnedAssignable(GrainState));
  # Start a new grain using the given package. `actionIndex` is an index into the package's
  # manifest's action table specifying the action to run.

  restoreGrain @1 (package :PackageInfo,
                   command :Package.Manifest.Command,
                   storage :Storage.StorageFactory,
                   grainState :Storage.Assignable(GrainState),
                   volume :Storage.Volume)
               -> (grain :Supervisor);
  # Continue an existing grain.
  #
  # It is the caller's (coordinator's) responsibility to have updated `grainState` to take
  # ownership of the grain before calling this. `volume` is the volume from `grainState`, but the
  # caller must have already called `disconnectAllClients()` at the same time as it claimed the
  # grain.

  unpackPackage @2 (volume :Storage.Volume) -> (stream :PackageUploadStream);
  # Initiate upload of a package, unpacking it into the given Volume (which should start out
  # uninitialized).

  interface PackageUploadStream extends(Util.ByteStream) {
    getResult @0 () -> (appId :Text, manifest :Package.Manifest);
    # Waits until `ByteStream.done()` is called, then returns:
    #
    # `appId`: The verified application ID string, as produced by the `spk` tool.
    # `manifest`: The parsed package manifest.
  }

  # TODO(someday): Enumerate grains.
  # TODO(someday): Resource usage stats.
}

interface Coordinator {
  # Decides which workers should be running which apps.

  newGrain @0 (package :PackageInfo,
               command :Package.Manifest.Command,
               storage :Storage.StorageFactory)
           -> (grain :Supervisor, grainState :Storage.OwnedAssignable(GrainState));
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
  # `grainState`: The storage object created using `storage`. This needs to be linked into the
  #   parent storage in order to persist long-term.
}

struct PackageInfo {
  id @0 :Data;
  # Some unique identifier for this package (not assigned by the worker).
  #
  # TODO(someday): Identify packages by capability. If it's the same `Volume`, it's the same
  #   package. This is arguably a security issue if an attacker can get access to the `Worker`
  #   or `Coordinator` interfaces and then poison workers by forging package IDs, though no
  #   attacker should ever have direct access to those interfaces, of course.

  volume @1 :Storage.Volume;
  # Read-only volume containing the unpacked package.
  #
  # TODO(security): Enforce read-only.
}
