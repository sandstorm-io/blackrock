// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_NBD_BRIDGE_H_
#define BLACKROCK_NBD_BRIDGE_H_

#include "common.h"
#include <kj/string.h>
#include <kj/async-io.h>
#include <blackrock/storage.capnp.h>
#include <linux/nbd.h>

namespace blackrock {

class NbdVolumeAdapter: private kj::TaskSet::ErrorHandler {
  // Implements the NBD protocol in terms of `Volume`.
public:
  NbdVolumeAdapter(kj::Own<kj::AsyncIoStream> socket, Volume::Client volume);
  // NBD requests are read from `socket` and implemented via `volume`.

  kj::Promise<void> run();
  // Actually runs the loop.

private:
  kj::Own<kj::AsyncIoStream> socket;
  Volume::Client volume;
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
  explicit NbdDevice();
  // Claims an unused NBD device and binds it to the given socket. (The other end of the socket
  // pair should be passed to `NbdVolumeAdapter`.)

  kj::StringPtr getPath() { return path; }
  // E.g. "/dev/nbd12".

  int getFd() { return fd; }

  static void resetAll();
  // Iterate through all the nbd devices and reset them, in order to un-block processes wedged
  // trying to read disconnected devices.
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
  NbdBinding(NbdDevice& device, kj::AutoCloseFd socket);
  // Binds the given NBD device to the given socket. (The other end of the socket pair should be
  // passed to `NbdVolumeAdapter`.)

  ~NbdBinding() noexcept(false);
  // Disconnects the binding.

private:
  NbdDevice& device;
  kj::Thread doItThread;
  // Executes the NBD_DO_IT ioctl(), which runs the NBD device loop in the kernel, not returning
  // until the device is disconnected.

  static NbdDevice& setup(NbdDevice& device, kj::AutoCloseFd socket);
};

class Mount {
  // Mounts a device at a path. As with `NbdDevice`, `Mount` MUST NOT be used in the same thread
  // that is executing the NbdVolumeAdapter implementing the device.

public:
  Mount(kj::StringPtr devPath, kj::StringPtr mountPoint, uint64_t flags, kj::StringPtr options);
  ~Mount() noexcept(false);

private:
  kj::String mountPoint;
};

}  // namespace blackrock

#endif // BLACKROCK_NBD_BRIDGE_H_
