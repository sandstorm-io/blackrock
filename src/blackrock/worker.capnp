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
               storage :Storage.StorageFactory,
               grainIdForLogging :Text)
           -> (grain :Supervisor, grainState :Storage.OwnedAssignable(GrainState));
  # Start a new grain using the given package.
  #
  # The caller needs to save `grainState` into a user's grain collection to make the grain
  # permanent.

  restoreGrain @1 (package :PackageInfo,
                   command :Package.Manifest.Command,
                   storage :Storage.StorageFactory,
                   grainState :GrainState,
                   exclusiveGrainStateSetter :Util.Assignable(GrainState).Setter,
                   exclusiveVolume :Storage.Volume,
                   grainIdForLogging :Text)
               -> (grain :Supervisor);
  # Continue an existing grain.
  #
  # It is the caller's (coordinator's) responsibility to have updated `grainState` to take
  # ownership of the grain before calling this. `exclusiveGrainStateSetter` and `exclusiveVolume`
  # are obtained from calling getSetter() on the grain's state Assignable and getExclusive() on the
  # state's Volume. These capabilities will thus become disconnected if a new worker takes
  # ownership of the grain, thereby preventing the old owner from corrupting data. Normally a new
  # worker will only be assigned if the old worker appears unhealthy or dead; the forceable
  # disconnection is performed to prevent zombies from corrupting data after the new worker has
  # taken over.

  unpackPackage @2 (storage :Storage.StorageFactory) -> (stream :PackageUploadStream);
  # Initiate upload of a package, unpacking it into a fresh Volume.

  interface PackageUploadStream extends(Util.ByteStream) {
    getResult @0 () -> (appId :Text, manifest :Package.Manifest, volume :Storage.OwnedVolume);
    # Waits until `ByteStream.done()` is called, then returns:
    #
    # `appId`: The verified application ID string, as produced by the `spk` tool.
    # `manifest`: The parsed package manifest.
  }

  unpackBackup @3 (data :Storage.Blob, storage :Storage.StorageFactory)
               -> (volume :Storage.OwnedVolume, metadata :Grain.GrainInfo);
  packBackup @4 (volume :Storage.Volume, metadata :Grain.GrainInfo, storage :Storage.StorageFactory)
             -> (data :Storage.OwnedBlob);

  # TODO(someday): Enumerate grains.
  # TODO(someday): Resource usage stats.
}

interface Coordinator {
  # Decides which workers should be running which apps.
  #
  # The Coordinator's main interface is actually Restorer(SturdyRef.Hosted) -- the Coordinator will
  # start up the desired grain and restore the capability. The `Coordinator` interface is only
  # used for creating new grains.

  newGrain @0 (app :Util.Assignable(AppRestoreInfo).Getter,
               initCommand :Package.Manifest.Command,
               storage :Storage.StorageFactory)
           -> (grain :Supervisor, grainState :Storage.OwnedAssignable(GrainState));
  # Create a new grain, just like Worker.newGrain().

  restoreGrain @1 (storage :Storage.StorageFactory,
                   grainState :Storage.Assignable(GrainState))
               -> (grain :Supervisor);
  # Restore a grain. Permanently sets the grain's package to `package` and continue command to
  # `command` if these weren't already the case.
}

struct AppRestoreInfo {
  package @0 :PackageInfo;
  restoreCommand @1 :Package.Manifest.Command;
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
