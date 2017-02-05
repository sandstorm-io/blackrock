# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@0x95ec494d81e25bb1;

$import "/capnp/c++.capnp".namespace("blackrock");

using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using SandstormCore = import "/sandstorm/supervisor.capnp".SandstormCore;
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
               grainId :Text,
               core :SandstormCore)
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
                   grainId :Text,
                   core :SandstormCore)
               -> (grain :Supervisor);
  # Continue an existing grain.
  #
  # `grainState` is the current value of the grain's GrainState assignable, and
  # `exclusiveGrainStateSetter` is the setter returned by the get() call that returned
  # `grainState`. Thus, a `set()` call on `grainStateSetter` will fail if the grain state has
  # changed.
  #
  # The first thing the worker will do is attempt to set the grain state in order to assert its
  # exclusive ownership. If the initial `set()` fails, `restoreGrain()` throws a "disconnected"
  # exception, and the caller should start over.
  #
  # Assuming the `set()` succeeds, the worker will call `volume.getExclusive()` to make absolutely
  # sure that no other worker might still be writing to the voluse.

  unpackPackage @2 (storage :Storage.StorageFactory) -> (stream :PackageUploadStream);
  # Initiate upload of a package, unpacking it into a fresh Volume.

  interface PackageUploadStream extends(Util.ByteStream) {
    getResult @0 () -> (appId :Text, manifest :Package.Manifest, volume :Storage.OwnedVolume,
                        authorPgpKeyFingerprint :Text);
    # Waits until `ByteStream.done()` is called, then returns:
    #
    # `appId`: The verified application ID string, as produced by the `spk` tool.
    # `manifest`: The parsed package manifest.
    # `volume`: The new Volume containing the unpacked app.
    # `authorPgpKeyFingerprint`: If the app was PGP-signed, the author's key fingerprint.
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
