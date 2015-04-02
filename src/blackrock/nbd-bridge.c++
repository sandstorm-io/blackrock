// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "nbd-bridge.h"
#include <errno.h>
#include <linux/nbd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sandstorm/util.h>
#include <kj/async-unix.h>
#include <sys/eventfd.h>
#include <sys/file.h>
#include <unistd.h>
#include <sodium/randombytes.h>

#include <sys/mount.h>
#undef BLOCK_SIZE  // #defined in mount.h, ugh

namespace blackrock {

namespace {

static uint64_t ntohll(uint64_t a) {
  uint32_t lo = a & 0xffffffff;
  uint32_t hi = a >> 32U;
  lo = ntohl(lo);
  hi = ntohl(hi);
  return ((uint64_t) lo) << 32U | hi;
}

constexpr uint64_t VOLUME_SIZE = 1ull << 40;
// Volumes in our storage interface do not have a defined size, since they are sparse. But Linux
// wants us to tell it a size, so we'll claim 1TB. We will actually create a much smaller
// filesystem in this space, only growing it if needed.

constexpr uint MAX_NBDS = 17;
// Maximum number of NBD devices. Prime so that a random probing interval will hit all slots.
//
// TODO(someday): Can we jack this up to, say, 65521?

constexpr uint MAX_RPC_BLOCKS = 512;
// Maximum number of blocks we'll transfer in a single Volume RPC.

}  // namespace

NbdVolumeAdapter::NbdVolumeAdapter(kj::Own<kj::AsyncIoStream> socket, Volume::Client volume)
    : socket(kj::mv(socket)), volume(kj::mv(volume)), tasks(*this) {}

struct NbdVolumeAdapter::RequestHandle {
  char handle[8];

  inline RequestHandle(const char other[sizeof(handle)]) {
    memcpy(handle, other, sizeof(handle));
  }
};

struct NbdVolumeAdapter::ReplyAndIovec {
  kj::Array<capnp::Response<Volume::ReadResults>> responses;
  kj::Array<kj::ArrayPtr<const byte>> iov;
  struct nbd_reply reply;

  ReplyAndIovec(kj::Array<capnp::Response<Volume::ReadResults>> responsesParam,
                RequestHandle handle, uint startPad, uint endPad)
      : responses(kj::mv(responsesParam)),
        iov(kj::heapArray<kj::ArrayPtr<const byte>>(responses.size() + 1)) {
    iov[0] = kj::arrayPtr(&reply, 1).asBytes();
    for (uint i: kj::indices(responses)) {
      iov[i + 1] = responses[i].getData();
    }

    if (startPad != 0) {
      auto piece = iov[1];
      iov[1] = piece.slice(startPad, piece.size());
    }
    if (endPad != 0) {
      auto piece = iov.back();
      iov.back() = piece.slice(0, piece.size() - endPad);
    }

    reply.magic = htonl(NBD_REPLY_MAGIC);
    reply.error = 0;
    memcpy(reply.handle, handle.handle, sizeof(handle.handle));
  }
};

kj::Promise<void> NbdVolumeAdapter::run() {
  return socket->read(&request, sizeof(request))
      .then([this]() -> kj::Promise<void> {
    KJ_ASSERT(ntohl(request.magic) == NBD_REQUEST_MAGIC);
    switch (ntohl(request.type)) {
      case NBD_CMD_READ: {
        // Unfortunately, NBD sometimes receives read requests that are not block-aligned. For
        // example, on mount, it receives a request for the first 1024 bytes of the volume.
        uint64_t startByte = ntohll(request.from);
        uint64_t endByte = startByte + ntohl(request.len);
        uint32_t startBlock = startByte / Volume::BLOCK_SIZE;
        uint32_t startPad = startByte % Volume::BLOCK_SIZE;
        uint32_t endBlock = endByte / Volume::BLOCK_SIZE;
        uint32_t endPad = endByte % Volume::BLOCK_SIZE;
        if (endPad != 0) {
          endPad = Volume::BLOCK_SIZE - endPad;
          ++endBlock;
        }

        uint32_t blockCount = endBlock - startBlock;

        // Split into requests of no more than the maximum size.
        uint reqCount = (blockCount + (MAX_RPC_BLOCKS - 1)) / MAX_RPC_BLOCKS;
        auto promises =
            kj::heapArrayBuilder<kj::Promise<capnp::Response<Volume::ReadResults>>>(reqCount);
        for (uint i = 0; i < reqCount; i++) {
          auto req = volume.readRequest();
          uint o = i * MAX_RPC_BLOCKS;
          req.setBlockNum(startBlock + o);
          req.setCount(kj::min(blockCount - o, MAX_RPC_BLOCKS));
          promises.add(req.send());
        }

        // Send all requests and handle responses.
        RequestHandle reqHandle = request.handle;
        tasks.add(kj::joinPromises(promises.finish())
            .then([this,reqHandle,startPad,endPad](auto responses) {
          auto reply = kj::heap<ReplyAndIovec>(kj::mv(responses), reqHandle, startPad, endPad);
          replyQueue = replyQueue.then([this,KJ_MVCAP(reply)]() mutable {
            auto promise = socket->write(reply->iov);
            return promise.attach(kj::mv(reply));
          });
        }, [this,reqHandle](kj::Exception&& e) {
          replyError(reqHandle, kj::mv(e), "read");
        }));
        return run();
      }
      case NBD_CMD_WRITE: {
        auto req = volume.writeRequest();
        uint64_t offset = ntohll(request.from);
        uint32_t size = ntohl(request.len);
        KJ_ASSERT(offset % Volume::BLOCK_SIZE == 0);
        req.setBlockNum(offset / Volume::BLOCK_SIZE);
        KJ_ASSERT(size % Volume::BLOCK_SIZE == 0);
        auto data = req.initData(size);

        RequestHandle reqHandle = request.handle;
        return socket->read(data.begin(), data.size())
            .then([this,reqHandle,data,KJ_MVCAP(req)]() mutable {
          // Check if the data is all-zero.
          bool allZero = true;
          for (const uint64_t* ptr = reinterpret_cast<uint64_t*>(data.begin()),
               *end = reinterpret_cast<uint64_t*>(data.end());
               ptr < end; ++ptr) {
            if (*ptr != 0) {
              allZero = false;
              break;
            }
          }

          if (allZero) {
            // Oh, this write is just zeros. Convert it to a zero() call instead. This optimization
            // alone drastically cuts the initial size of an ext4 filesystem and also works around
            // many databases aggressively preallocating space.
            //
            // TODO(perf): What if a large write has many pages of zeros interleaved with non-zero
            //   pages? Do we want to break it up into some writes and some zeros? This would
            //   fragment the request and also possibly fragment the storage. To avoid fragmenting
            //   the request, we might want to do this detection server-side, or attach a list of
            //   hints on the client side. Fragmenting the disk might not be worth it, though.
            //
            // TODO(perf): Apparently the Linux kernel supports block drivers informing it that
            //   TRIMed bytes will be read back as zeros, and ext4 takes advantage of this.
            //   NBD doesn't appear to have a way to set this. Maybe we should tweak the driver?
            auto req2 = volume.zeroRequest();
            req2.setBlockNum(req.getBlockNum());
            req2.setCount(data.size() / Volume::BLOCK_SIZE);
            tasks.add(req2.send().then([this,reqHandle](auto resp) {
              reply(reqHandle);
            }, [this,reqHandle](kj::Exception&& e) {
              replyError(reqHandle, kj::mv(e), "zero");
            }));
          } else {
            tasks.add(req.send().then([this,reqHandle](auto resp) {
              reply(reqHandle);
            }, [this,reqHandle](kj::Exception&& e) {
              replyError(reqHandle, kj::mv(e), "write");
            }));
          }
          return run();
        });
      }
      case NBD_CMD_DISC: {
        // Disconnect requested. Stop reading, finish writes and shutdown write end.
        return replyQueue.then([this]() {
          socket->shutdownWrite();
        });
      }
      case NBD_CMD_FLUSH: {
        RequestHandle reqHandle = request.handle;
        tasks.add(volume.syncRequest().send().then([this,reqHandle](auto resp) {
          reply(reqHandle);
        }, [this,reqHandle](kj::Exception&& e) {
          replyError(reqHandle, kj::mv(e), "sync");
        }));
        return run();
      }
      case NBD_CMD_TRIM: {
        auto req = volume.zeroRequest();
        uint64_t offset = ntohll(request.from);
        uint32_t size = ntohl(request.len);
        KJ_ASSERT(offset % Volume::BLOCK_SIZE == 0);
        req.setBlockNum(offset / Volume::BLOCK_SIZE);
        KJ_ASSERT(size % Volume::BLOCK_SIZE == 0);
        req.setCount(size / Volume::BLOCK_SIZE);

        RequestHandle reqHandle = request.handle;
        tasks.add(req.send().then([this,reqHandle](auto resp) {
          reply(reqHandle);
        }, [this,reqHandle](kj::Exception&& e) {
          replyError(reqHandle, kj::mv(e), "zero");
        }));
        return run();
      }
      default:
        KJ_FAIL_ASSERT("unknown NBD command", request.type);
    }
  });
}

void NbdVolumeAdapter::reply(RequestHandle reqHandle, int error) {
  auto reply = kj::heap<struct nbd_reply>();
  reply->magic = htonl(NBD_REPLY_MAGIC);
  reply->error = error;
  memcpy(reply->handle, reqHandle.handle, sizeof(reqHandle.handle));
  replyQueue = replyQueue.then([this,KJ_MVCAP(reply)]() mutable {
    auto promise = socket->write(reply.get(), sizeof(*reply));
    return promise.attach(kj::mv(reply));
  });
}

void NbdVolumeAdapter::replyError(
    RequestHandle reqHandle, kj::Exception&& exception, const char* op) {
  // TODO(soon): Reconnect when disconnected. Probably should be accomplished by wrapping the
  //   Volume rather than handling errors here.
  KJ_LOG(ERROR, "Volume I/O threw exception!", op, exception);
  reply(reqHandle, EIO);
}

void NbdVolumeAdapter::taskFailed(kj::Exception&& exception) {
  // In theory this should never happen as every place where we create a task, we explicitly
  // handle errors.
  KJ_LOG(ERROR, "task failure in NbdDevice", exception);
}

// =======================================================================================

NbdDevice::NbdDevice() {
  // We try to claim a random NBD device. If it's already locked, we try another one, with a random
  // probing interval. The number of devices is prime so we will eventually probe all slots
  // regardless of interval.

  uint index = randombytes_uniform(MAX_NBDS);
  uint step = randombytes_uniform(MAX_NBDS);
  for (uint i = 0; i < MAX_NBDS; i++) {
    kj::String tryPath = kj::str("/dev/nbd", index);
    auto tryFd = sandstorm::raiiOpen(tryPath, O_RDWR | O_CLOEXEC);

    // Try to lock the file.
    int flockResult;
    KJ_NONBLOCKING_SYSCALL(flockResult = flock(tryFd, LOCK_EX | LOCK_NB));
    if (flockResult >= 0) {
      // Success! It's ours! The lock will be dropped when the fd is closed.
      fd = kj::mv(tryFd);
      path = kj::mv(tryPath);
      return;
    }

    index = (index + step) % MAX_NBDS;
  }

  // Since MAX_NBDS is prime and we stepped by a constant interval MAX_NBDS times, we tried every
  // single number. But it seems none were available. Give up.

  KJ_FAIL_ASSERT("all NBD devices are in use");
}

void NbdDevice::resetAll() {
  for (uint i = 0; i < MAX_NBDS; i++) {
    auto devname = kj::str("/dev/nbd", i);
    KJ_LOG(WARNING, "killing nbd device", devname);
    auto fd = sandstorm::raiiOpen(devname, O_RDWR | O_CLOEXEC);
    int flockResult;
    KJ_NONBLOCKING_SYSCALL(flockResult = flock(fd, LOCK_EX | LOCK_NB));
    if (flockResult < 0) {
      KJ_LOG(WARNING, "device still locked", devname);
    }
    KJ_SYSCALL(ioctl(fd, NBD_CLEAR_SOCK));
  }
}

void NbdDevice::loadKernelModule() {
  sandstorm::Subprocess({"modprobe", "nbd", kj::str("nbds_max=", MAX_NBDS)}).waitForSuccess();
}

// =======================================================================================

NbdBinding::NbdBinding(NbdDevice& device, kj::AutoCloseFd socket)
    : device(setup(device, kj::mv(socket))),
      doItThread([&device,KJ_MVCAP(socket)]() mutable {
        KJ_SYSCALL(ioctl(device.getFd(), NBD_DO_IT));
      }) {}

NbdBinding::~NbdBinding() noexcept(false) {
  KJ_SYSCALL(ioctl(device.getFd(), NBD_DISCONNECT)) { break; }
}

NbdDevice& NbdBinding::setup(NbdDevice& device, kj::AutoCloseFd socket) {
  int nbdFd = device.getFd();
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_BLKSIZE, Volume::BLOCK_SIZE));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_SIZE, VOLUME_SIZE));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_FLAGS, NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM));
  KJ_SYSCALL(ioctl(nbdFd, NBD_CLEAR_SOCK));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_SOCK, socket.get()));
  return device;
}

// =======================================================================================

Mount::Mount(kj::StringPtr devPath, kj::StringPtr mountPoint, kj::StringPtr options)
    : mountPoint(kj::heapString(mountPoint)) {
  KJ_SYSCALL(mount(devPath.cStr(), mountPoint.cStr(), "ext4",
                   MS_NODEV | MS_NOATIME | MS_NOSUID, options.cStr()));
}

Mount::~Mount() noexcept(false) {
  // TODO(now): Do we need to handle EBUSY and maybe do a force-unmount? It *should* be the case
  //   that the app has been killed off by this point and therefore there should be no open files.
  //   A force-unmount is presumably no better than killing off the mount namespace.
  KJ_SYSCALL(umount(mountPoint.cStr())) { break; };
}

}  // namespace blackrock