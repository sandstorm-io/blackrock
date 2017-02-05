// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef BLACKROCK_NBD_BRIDGE_H_
#define BLACKROCK_NBD_BRIDGE_H_

#include "common.h"
#include <kj/string.h>
#include <kj/async-io.h>
#include <blackrock/storage.capnp.h>
#include <linux/nbd.h>

namespace blackrock {

enum class NbdAccessType {
  READ_ONLY,
  READ_WRITE
};

class NbdVolumeAdapter: private kj::TaskSet::ErrorHandler {
  // Implements the NBD protocol in terms of `Volume`.
public:
  NbdVolumeAdapter(kj::Own<kj::AsyncIoStream> socket, Volume::Client volume,
                   NbdAccessType access);
  // NBD requests are read from `socket` and implemented via `volume`.

  void updateVolume(Volume::Client newVolume);
  // Replaces the Volume capability with a new one, which must point to the exact same volume.
  // Useful for recovering after disconnects, if the driver hasn't noticed the disconnect yet.

  kj::Promise<void> run();
  // Actually runs the loop. The promise resolves successfully when the device has been shut down.
  // It is extremely important to wait for this before destroying the NbdVolumeAdapter; failure
  // to do so can leave the kernel in an unhappy state.

  kj::Promise<void> onDisconnected() { return kj::mv(disconnectedPaf.promise); }
  // Resolves if the underlying volume becomes disconnected, in which case it's time to force-kill
  // everything using it. Can only be called once.

private:
  kj::Own<kj::AsyncIoStream> socket;
  Volume::Client volume;
  kj::PromiseFulfillerPair<void> disconnectedPaf;
  NbdAccessType access;
  bool disconnected = false;
  kj::TaskSet tasks;

  kj::Promise<void> replyQueue = kj::READY_NOW;
  // Promise for completion of previous write() operation to handle.socket.
  //
  // Becomes null when the run loop completes.
  //
  // TODO(someday): When overlapping write()s are supported by AsyncIoStream, simplify this.

  struct nbd_request request;
  // We only read one of these at a time, so might as well allocate it here.

  struct RequestHandle;
  struct ReplyAndIovec;
  void reply(RequestHandle reqHandle, int error = 0);
  void replyError(RequestHandle reqHandle, kj::Exception&& exception, const char* op);
  void taskFailed(kj::Exception&& exception) override;
};

class NbdDevice {
  // Represents a claim to a specific `/dev/nbdX` device node.

public:
  NbdDevice();
  // Claims an unused NBD device and binds it to the given socket. (The other end of the socket
  // pair should be passed to `NbdVolumeAdapter`.)

  explicit NbdDevice(uint number);
  // Explicitly claim a specific device number. For debugging purposes only!

  kj::StringPtr getPath() { return path; }
  // E.g. "/dev/nbd12".

  int getFd() { return fd; }

  void format();
  // Format the device as an ext4 filesystem with an initial size of 8GB. This is accomplished by
  // simply writing a template image directly to the disk, so format() will result in exactly the
  // same disk image every time.

  void trimJournalIfClean();
  // Verify that the journal is currently clean, and then TRIM it. Call immediately after a clean
  // unmount to reduce disk usage. (The journal normally doesn't get TRIMed even when the contents
  // have already been committed. This seems to be a deficiency in the ext4 driver.)

  void fixSurpriseFeatures();
  // Check if this volume has the "surprise features" of 64bit and metadata_checksum enabled. If
  // so, fix the situation. The surprise features were accidentally enabled on many grains in
  // production due to an unexpected change in /etc/mke2fs.conf landing in Debian Testing. Since
  // we use mke2fs to create the zygote image at compile time, these features ended up enabled
  // in production. The metadata_checksum feature is buggy on our older production kernels, and
  // the 64bit option breaks trimJournalIfClean() (we could fix that, but the 64bit option is
  // not helpful to us, so better to avoid having multiple code paths!).
  //
  // Note: This method runs subprocesses and may block. It CANNOT be run from the main worker
  //   process!

  static void resetAll();
  // Iterate through all the nbd devices and reset them, in order to un-block processes wedged
  // trying to read disconnected devices.
  //
  // THIS WILL BREAK EVERYTHING CURRENTLY USING ANY NBD DEVICE.

  static void disconnectAll();
  // Iterate through all the nbd devices and disconnect them, in an attempt to forcefully tear
  // down a worker.
  //
  // THIS WILL BREAK EVERYTHING CURRENTLY USING ANY NBD DEVICE.

  static void loadKernelModule();
  // Make sure the NBD kernel module is loaded.

private:
  kj::String path;
  kj::AutoCloseFd fd;
};

class NbdBinding {
  // Given an NBD device and a socket implementing the NBD protocol, makes the NBD device live and
  // mountable.
  //
  // NbdBinding MUST NOT be used in the same thread that is running the NbdVolumeAdapter. This is
  // because NbdBinding performs blocking system calls that will cause the kernel to issue reads
  // and writes to the device, and will not return until those operations complete.

public:
  NbdBinding(NbdDevice& device, kj::AutoCloseFd socket, NbdAccessType access);
  // Binds the given NBD device to the given socket. (The other end of the socket pair should be
  // passed to `NbdVolumeAdapter`.)

  ~NbdBinding() noexcept(false);
  // Disconnects the binding.

private:
  NbdDevice& device;
  kj::Thread doItThread;
  // Executes the NBD_DO_IT ioctl(), which runs the NBD device loop in the kernel, not returning
  // until the device is disconnected.

  static NbdDevice& setup(NbdDevice& device, kj::AutoCloseFd socket, NbdAccessType access);
};

class Mount {
  // Mounts a device at a path. As with `NbdDevice`, `Mount` MUST NOT be used in the same thread
  // that is executing the NbdVolumeAdapter implementing the device.

public:
  Mount(kj::StringPtr devPath, kj::StringPtr mountPoint, uint64_t flags, kj::StringPtr options);
  ~Mount() noexcept(false);

private:
  kj::String mountPoint;
  uint64_t flags;
};

}  // namespace blackrock

#endif // BLACKROCK_NBD_BRIDGE_H_
