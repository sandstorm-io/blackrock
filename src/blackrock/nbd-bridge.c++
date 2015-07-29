// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
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
#include <capnp/message.h>
#include <blackrock/sparse-data.capnp.h>
#include <blackrock/blank-ext4.capnp.h>
#include <limits.h>

#include <sys/mount.h>
#include <linux/fs.h>  // must come after mount.h because of macro pollution
#undef BLOCK_SIZE  // #defined in mount.h and fs.h, ugh

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

constexpr uint MAX_NBDS = 4093;
// Maximum number of NBD devices. Prime so that a random probing interval will hit all slots.

constexpr uint MAX_RPC_BLOCKS = 512;
// Maximum number of blocks we'll transfer in a single Volume RPC.

}  // namespace

NbdVolumeAdapter::NbdVolumeAdapter(kj::Own<kj::AsyncIoStream> socket, Volume::Client volume,
                                   NbdAccessType access)
    : socket(kj::mv(socket)), volume(kj::mv(volume)),
      disconnectedPaf(kj::newPromiseAndFulfiller<void>()),
      access(access), tasks(*this) {}

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
          if (access != NbdAccessType::READ_WRITE) {
            // Whoops, read-only block device. This shouldn't happen since we mount the filesystem
            // read-only and set the block device read-only at the kernel level.
            KJ_LOG(ERROR, "caught write() on read-only NBD device");
            reply(reqHandle, EPERM);
            return run();
          }

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
        if (access != NbdAccessType::READ_WRITE) {
          // Whoops, read-only block device. This shouldn't happen since we mount the filesystem
          // read-only and set the block device read-only at the kernel level.
          KJ_LOG(ERROR, "caught flush() on read-only NBD device");
          reply(reqHandle, EPERM);
          return run();
        }

        tasks.add(volume.syncRequest().send().then([this,reqHandle](auto resp) {
          reply(reqHandle);
        }, [this,reqHandle](kj::Exception&& e) {
          replyError(reqHandle, kj::mv(e), "sync");
        }));
        return run();
      }
      case NBD_CMD_TRIM: {
        RequestHandle reqHandle = request.handle;
        if (access != NbdAccessType::READ_WRITE) {
          // Whoops, read-only block device. This shouldn't happen since we mount the filesystem
          // read-only and set the block device read-only at the kernel level.
          KJ_LOG(ERROR, "caught trim() on read-only NBD device");
          reply(reqHandle, EPERM);
          return run();
        }

        auto req = volume.zeroRequest();
        uint64_t offset = ntohll(request.from);
        uint32_t size = ntohl(request.len);
        KJ_ASSERT(offset % Volume::BLOCK_SIZE == 0);
        req.setBlockNum(offset / Volume::BLOCK_SIZE);
        KJ_ASSERT(size % Volume::BLOCK_SIZE == 0);
        req.setCount(size / Volume::BLOCK_SIZE);

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
  reply->error = htonl(error);
  memcpy(reply->handle, reqHandle.handle, sizeof(reqHandle.handle));
  replyQueue = replyQueue.then([this,KJ_MVCAP(reply)]() mutable {
    auto promise = socket->write(reply.get(), sizeof(*reply));
    return promise.attach(kj::mv(reply));
  });
}

void NbdVolumeAdapter::replyError(
    RequestHandle reqHandle, kj::Exception&& exception, const char* op) {
  if (exception.getType() == kj::Exception::Type::DISCONNECTED) {
    // The volume was disconnected, probably because we killed this grain.
    if (!disconnected) {
      disconnected = true;
      KJ_LOG(WARNING, "RARE: NBD volume disconnected. Maybe due to STONITH? "
                      "But client is still alive.", exception);
      disconnectedPaf.fulfiller->fulfill();
    }
  } else {
    KJ_LOG(ERROR, "Volume I/O threw exception!", op, exception);
  }
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
      // We locked the file, meaning it doesn't appear to be in-use by any Blackrock process.
      // However, just to be safe, let's also use the kernel's nifty O_EXCL trick to check whether
      // the device is mounted.
    retry:
      int fd2 = open(tryPath.cStr(), O_RDONLY | O_EXCL | O_CLOEXEC);
      if (fd2 < 0) {
        int error = errno;
        if (error == EINTR) {
          goto retry;
        } else if (error == EBUSY) {
          // Yikes, this NBD is mounted! Let's steer clear.
          KJ_LOG(ERROR, "unlocked nbd device appears to be mounted; skipping");
        } else {
          KJ_FAIL_SYSCALL("open(nbd device, O_EXCL)", error, tryPath);
        }
      } else {
        KJ_SYSCALL(close(fd2));

        // Success! It's ours! The lock will be dropped when the fd is closed.
        fd = kj::mv(tryFd);
        path = kj::mv(tryPath);
        return;
      }
    }

    index = (index + step) % MAX_NBDS;
  }

  // Since MAX_NBDS is prime and we stepped by a constant interval MAX_NBDS times, we tried every
  // single number. But it seems none were available. Give up.

  KJ_FAIL_ASSERT("all NBD devices are in use");
}

NbdDevice::NbdDevice(uint number)
    : path(kj::str("/dev/nbd", number)),
      fd(sandstorm::raiiOpen(path, O_RDWR | O_CLOEXEC)) {
  KJ_SYSCALL(flock(fd, LOCK_EX | LOCK_NB), "requested nbd device is already in-use", path);
}

static void pwriteAll(int fd, const void* data, size_t size, off_t offset) {
  while (size > 0) {
    ssize_t n;
    KJ_SYSCALL(n = pwrite(fd, data, size, offset));
    KJ_ASSERT(n != 0, "zero-sized write?");
    data = reinterpret_cast<const byte*>(data) + n;
    size -= n;
    offset += n;
  }
}

void NbdDevice::format() {
  SparseData::Reader sparse = BLANK_EXT4;

  for (auto chunk: sparse.getChunks()) {
    auto data = chunk.getData();
    pwriteAll(fd, data.begin(), data.size(), chunk.getOffset());
  }
}

namespace {

template <uint size>
class alignas(uint64_t) Ext4Record {
  // Convenience class for reading fixed-width ext4 records, e.g. the superblock.

public:
  Ext4Record(int fd, uint64_t offset) {
    ssize_t n;
    KJ_SYSCALL(n = pread(fd, data, size, offset));
    KJ_ASSERT(n == size);
  }

  uint16_t le16(uint offset) { return *reinterpret_cast<uint16_t*>(data + offset); }
  uint32_t le32(uint offset) { return *reinterpret_cast<uint32_t*>(data + offset); }

  uint16_t be16(uint offset) { return __builtin_bswap16(le16(offset)); }
  uint32_t be32(uint offset) { return __builtin_bswap32(le32(offset)); }

  byte* bytes(uint offset) { return data + offset; }

private:
  byte data[size];
};

}  // namespace

void NbdDevice::trimJournalIfClean() {
#define EXPECT(exp, cond, message) KJ_ASSERT(exp cond, message, exp)
#define EXPECTN(exp, cond, message) KJ_ASSERT(!(exp cond), message, exp)
  KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
    // Carefully verify that the journal is clean, and then trim it.
    //
    // This code made possible by: https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout

    // Examine the superblock and check all our assuptions about the filesystem.
    Ext4Record<1024> superblock(fd, 1024);
    EXPECT(superblock.le32(0x18), == 2, "block size not 4k?");
    EXPECT(superblock.le32(0x5C), & 0x4, "no journal?");
    EXPECTN(superblock.le32(0x60), & 0x80, "not 32-bit?");
    EXPECT(superblock.le32(0xE0), == 8, "journal is not inode 8?");
    EXPECT(superblock.le32(0x28), >= 8, "fewer than 8 inodes per group?");
    EXPECT(superblock.le16(0x58), == 256, "inodes not 256 bytes?");

    // Read the group descriptor for the first group, which ought to contain the journal inode.
    Ext4Record<64> group(fd, 4096);
    uint inodeTableLocation = group.le32(0x8);

    // Read the journal inode. Since there is no inode 0, inode 8 is located 7 inodes away from
    // the table start.
    Ext4Record<256> inode(fd, inodeTableLocation * 4096ull + 7 * 256);
    EXPECT(inode.le32(0x1C), == (128 << 11), "journal is not 128 MB?");
    EXPECT(inode.le32(0x20), & 0x80000, "journal not extents-based?");
    EXPECT(inode.le16(0x28 + 0x0), == 0xF30Au, "journal inode extent tree has bad magic number?");

    uint extentCount = inode.le16(0x28 + 0x2);
    EXPECT(extentCount, <= 4, "journal inode has more than four extents?");

    uint logicalPos = 0;
    uint journalLocation = 0;
    uint physicalPos = 0;
    for (uint i = 0; i < extentCount; i++) {
      uint off = 0x28 + 0xc + i * 0xc;
      EXPECT(inode.le32(off + 0x0), == logicalPos, "journal exntents not contiguous from zero?");
      EXPECT(inode.le16(off + 0x6), == 0, "journal location outside 32-bit range?");

      if (i == 0) {
        journalLocation = inode.le32(off + 0x8);
        physicalPos = journalLocation;
      } else {
        EXPECT(inode.le32(off + 0x8), == physicalPos, "journal extents not contiguous?");
      }

      uint len = inode.le32(off + 0x4) & 0x7fffu;  // don't care if it's initialized
      logicalPos += len;
      physicalPos += len;
    }
    EXPECT(logicalPos, == 32768, "journal extents do not add to 128MB?");

    KJ_ASSERT(memcmp(superblock.bytes(0x10C), inode.bytes(0x28), 60) == 0,
              "superblock's backup of journal i_block doesn't match?");

    if (journalLocation != 0x108000) {
      KJ_LOG(WARNING, "journal location is different than it used to be", journalLocation);
    }

    Ext4Record<1024> journal(fd, journalLocation * 4096ull);
    EXPECT(journal.be32(0x0), == 0xC03B3998u, "journal superblock has bad magic number?");
    EXPECT(journal.be32(0x4), == 4, "journal doesn't start with superblock?");
    KJ_ASSERT(memcmp(superblock.bytes(0xD0), journal.bytes(0x30), 16) == 0,
              "journal uuid doesn't match?");

    // The ext4 layout page claims journal.s_start == 0 does NOT imply the journal is clean, but
    // several comments inside the kernel source code claim it does. I am not sure how else to
    // tell, so we'll rely on this.
    EXPECT(journal.be32(0x1C), == 0, "journal is not clean!");

    // Trim everything except the superblock.
    uint64_t range[2] = { (journalLocation + 1) * 4096ull, (logicalPos - 1) * 4096 };
    KJ_SYSCALL(ioctl(fd, BLKDISCARD, &range));
  })) {
    KJ_LOG(ERROR, "exception while trimming journal", *exception);
  }
#undef EXPECT
#undef EXPECTN
}

void NbdDevice::resetAll() {
  for (uint i = 0; i < MAX_NBDS; i++) {
    auto devname = kj::str("/dev/nbd", i);
//    KJ_LOG(WARNING, "killing nbd device", devname);
    auto fd = sandstorm::raiiOpen(devname, O_RDWR | O_CLOEXEC);
    int flockResult;
    KJ_NONBLOCKING_SYSCALL(flockResult = flock(fd, LOCK_EX | LOCK_NB));
    if (flockResult < 0) {
      KJ_LOG(WARNING, "device still locked", devname);
    }
    KJ_SYSCALL(ioctl(fd, NBD_CLEAR_SOCK));
  }
}

void NbdDevice::disconnectAll() {
  for (uint i = 0; i < MAX_NBDS; i++) {
    auto devname = kj::str("/dev/nbd", i);
//    KJ_LOG(WARNING, "disconnecting nbd device", devname);
    auto fd = sandstorm::raiiOpen(devname, O_RDWR | O_CLOEXEC);

    // We ignore ioctl() errors on NBD_DISCONNECT because if the device isn't connected then the
    // ioctl fails.
    ioctl(fd, NBD_DISCONNECT);

    // Clear socket for good measure.
    KJ_SYSCALL(ioctl(fd, NBD_CLEAR_SOCK));
  }
}

void NbdDevice::loadKernelModule() {
  sandstorm::Subprocess({"modprobe", "nbd", kj::str("nbds_max=", MAX_NBDS), "max_part=0"})
      .waitForSuccess();
}

// =======================================================================================

NbdBinding::NbdBinding(NbdDevice& device, kj::AutoCloseFd socket, NbdAccessType access)
    : device(setup(device, kj::mv(socket), access)),
      doItThread([&device,KJ_MVCAP(socket)]() mutable {
        KJ_DEFER(KJ_SYSCALL(ioctl(device.getFd(), NBD_CLEAR_SOCK)) { break; });
        KJ_SYSCALL(ioctl(device.getFd(), NBD_DO_IT));
      }) {}

NbdBinding::~NbdBinding() noexcept(false) {
  KJ_DEFER(KJ_SYSCALL(ioctl(device.getFd(), NBD_DISCONNECT)) { break; });

  // Before we actually try to disconnect, make sure the device is not still in-use.
  uint delay = 1;
retry:
  int fd = open(device.getPath().cStr(), O_RDONLY | O_EXCL | O_CLOEXEC);
  if (fd < 0) {
    int error = errno;
    if (error == EINTR) {
      goto retry;
    } else if (error == EBUSY) {
      KJ_LOG(WARNING, "requested disconnect of nbd device that is still mounted; waiting a bit "
                      "for unmount", device.getPath(), delay);
      sleep(delay);
      delay = delay * 2;  // exponential backoff
      goto retry;
    } else {
      KJ_FAIL_SYSCALL("open(nbd device, O_EXCL)", error, device.getPath()) { break; }
    }
  } else {
    close(fd);
  }
}

NbdDevice& NbdBinding::setup(NbdDevice& device, kj::AutoCloseFd socket, NbdAccessType access) {
  int nbdFd = device.getFd();
  int readOnly = access == NbdAccessType::READ_ONLY;
  KJ_SYSCALL(ioctl(nbdFd, NBD_CLEAR_SOCK));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_BLKSIZE, Volume::BLOCK_SIZE));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_SIZE, VOLUME_SIZE));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_FLAGS, NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM));
  KJ_SYSCALL(ioctl(nbdFd, BLKROSET, &readOnly));
  KJ_SYSCALL(ioctl(nbdFd, NBD_SET_SOCK, socket.get()));
  return device;
}

// =======================================================================================

Mount::Mount(kj::StringPtr devPath, kj::StringPtr mountPoint, uint64_t flags, kj::StringPtr options)
    : mountPoint(kj::heapString(mountPoint)), flags(flags) {
  KJ_SYSCALL(mount(devPath.cStr(), mountPoint.cStr(), "ext4",
                   MS_NODEV | MS_NOATIME | MS_NOSUID | flags, options.cStr()),
             devPath, mountPoint);
}

Mount::~Mount() noexcept(false) {
  if (!(flags & MS_RDONLY)) {
    KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
      // Before unmounting a writable device, trim unused pages.
      // TODO(cleanup): This is mainly here to fix existing files made before we started setting
      //   -o discard. Once migration is complete, we should remove this, as it is redundant given
      //   -o discard is set.
      struct fstrim_range range;
      memset(&range, 0, sizeof(range));
      range.len = ULLONG_MAX;
      auto fd = sandstorm::raiiOpen(mountPoint, O_RDONLY);
      KJ_SYSCALL(ioctl(fd, FITRIM, &range));
    })) {
      KJ_LOG(ERROR, "exception during fstrim", *exception);
    }
  }

  // TODO(soon): Do we need to handle EBUSY and maybe do a force-unmount? It *should* be the case
  //   that the app has been killed off by this point and therefore there should be no open files.
  //   A force-unmount is presumably no better than killing off the mount namespace.
  KJ_SYSCALL(umount(mountPoint.cStr())) { return; };
}

}  // namespace blackrock
