// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "blob-local.h"
#include <kj/debug.h>
#include <sandstorm/util.h>
#include <stdio.h>
#include <sys/xattr.h>
#include <errno.h>

namespace blackrock {
namespace storage {

class LocalBlobLayer::ContentImpl final: public BlobLayer::Content {
public:
  explicit ContentImpl(kj::AutoCloseFd&& fd): fd(kj::mv(fd)) {}

  int getFd() { return fd; }

  void setXattr(const Xattr& xattr) {
    KJ_SYSCALL(fsetxattr(fd, Xattr::NAME, &xattr, sizeof(xattr), 0));
  }

  void setXattr(const TemporaryXattr& xattr) {
    KJ_SYSCALL(fsetxattr(fd, TemporaryXattr::NAME, &xattr, sizeof(xattr), 0));
  }

  Size getSize() override {
    struct stat stats;
    KJ_SYSCALL(fstat(fd, &stats));
    return { static_cast<uint64_t>(stats.st_size), static_cast<uint32_t>(stats.st_blocks / 8) };
  }

  uint64_t getStart() override {
    for (;;) {
      off_t result = lseek(fd, 0, SEEK_DATA);
      if (result >= 0) return result;

      int error = errno;
      if (error == ENXIO) {
        // The file has no data, so return the end of the file.
        auto size = getSize();
        KJ_ASSERT(size.blockCount == 0, "no data but non-zero block count?");
        return size.endMarker;
      } else if (error != EINTR) {
        KJ_FAIL_SYSCALL("lseek(fd, 0, SEEK_DATA)", error);
      }
    }
  }

  kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) override {
    // TODO(someday): Use async I/O syscalls (they are complicated).
    while (buffer.size() > 0) {
      ssize_t n;
      KJ_SYSCALL(n = pread(fd, buffer.begin(), buffer.size(), offset));
      if (n == 0) {
        // Reading past EOF. Assume all-zero.
        memset(buffer.begin(), 0, buffer.size());
        break;
      }
      buffer = buffer.slice(n, buffer.size());
      offset += n;
    }

    return kj::READY_NOW;
  }

  void write(uint64_t offset, kj::ArrayPtr<const byte> data) override {
    // TODO(someday): Use async I/O syscalls? Unclear if it matters here since writes are
    //   presumably cached.
    while (data.size() > 0) {
      ssize_t n;
      KJ_SYSCALL(n = pwrite(fd, data.begin(), data.size(), offset));
      KJ_ASSERT(n != 0, "zero-sized write?");
      data = data.slice(n, data.size());
      offset += n;
    }
  }

  void zero(uint64_t offset, uint64_t size) override {
    KJ_SYSCALL(fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size));
  }

  kj::Promise<void> sync() override {
    // TODO(someday): Use async I/O syscalls (they are complicated).
    KJ_SYSCALL(fdatasync(fd));
    return kj::READY_NOW;
  }

private:
  kj::AutoCloseFd fd;
};

class LocalBlobLayer::TemporaryImpl final: public BlobLayer::Temporary {
public:
  explicit TemporaryImpl(LocalBlobLayer& blobLayer)
      : blobLayer(blobLayer),
        content(kj::heap<ContentImpl>(sandstorm::raiiOpenAt(
            blobLayer.directoryFd, ".", O_RDWR | O_TMPFILE | O_CLOEXEC))) {}
  // Create a new temporary with no recovery ID.

  TemporaryImpl(LocalBlobLayer& blobLayer, RecoveryId recoveredId)
      : blobLayer(blobLayer),
        content(kj::heap<ContentImpl>(sandstorm::raiiOpenAt(blobLayer.directoryFd,
            toPath(recoveredId, "recovery/"), O_RDWR | O_CLOEXEC))),
        recoveredId(recoveredId) {}
  // Recover a temporary.

  ~TemporaryImpl() noexcept(false) {
    if (blobLayer.testingDirtyExit) return;

    // Unlink from filesystem, if it has been linked in.
    KJ_IF_MAYBE(r, recoveryId) {
      KJ_SYSCALL(unlinkat(blobLayer.directoryFd, toPath(*r).cStr(), 0)) { break; }
    }

    // If this temporary was recovered during this process's startup, also unlink it from the
    // recovered directory. (But only if said recovery actually completed.)
    KJ_IF_MAYBE(r, recoveredId) {
      if (blobLayer.recoveryFinished) {
        KJ_SYSCALL(unlinkat(blobLayer.directoryFd,
            toPath(*r, "recovered/").cStr(), 0)) { break; }
      }
    }
  }

  kj::Own<ContentImpl> moveToMain(ObjectId id, const Xattr& xattr) {
    content->setXattr(xattr);
    moveTo(kj::str("main/", id.filename('o')));
    return kj::mv(content);
  }

  TemporaryXattr getTemporaryXattr() {
    TemporaryXattr xattr;
    memset(&xattr, 0, sizeof(xattr));
    KJ_SYSCALL(fgetxattr(content->getFd(), TemporaryXattr::NAME, &xattr, sizeof(xattr)));
    return xattr;
  }

  void setRecoveryId(RecoveryId id) override {
    KJ_REQUIRE(id.type != RecoveryType::BACKBURNER, "backburner requires temporary xattrs");
    moveTo(toPath(id));
    recoveryId = id;
  }

  void setRecoveryId(RecoveryId id, TemporaryXattr xattr) override {
    KJ_REQUIRE(id.type == RecoveryType::BACKBURNER, "only backburner has temporary xattrs");
    KJ_SYSCALL(fsetxattr(content->getFd(), TemporaryXattr::NAME, &xattr, sizeof(xattr), 0));
    moveTo(toPath(id));
    recoveryId = id;
  }

  void overwrite(TemporaryXattr xattr, kj::Own<Temporary> replacement) override {
    auto other = replacement.downcast<TemporaryImpl>();
    other->setXattr(xattr);

    // We require that we have a recovery ID already because otherwise if the replacement already
    // has a recovery ID then we would need to unlink it, but Linux does not allow a once-linked
    // file to be unlinked and then linked again later. In any case, there's no good reason to
    // atomically replace a non-recoverable temporary.
    other->moveTo(toPath(KJ_REQUIRE_NONNULL(recoveryId,
        "can't call replaceContent() before setRecoveryId()")));
    content = kj::mv(other->content);
  }

  TemporaryXattr getXattr() override {
    TemporaryXattr xattr;
    memset(&xattr, 0, sizeof(xattr));
    KJ_SYSCALL(fgetxattr(content->getFd(), TemporaryXattr::NAME, &xattr, sizeof(xattr)));
    return xattr;
  }

  void setXattr(TemporaryXattr xattr) override {
    content->setXattr(xattr);
  }

  Content& getContent() override {
    return *content;
  }

private:
  LocalBlobLayer& blobLayer;
  kj::Own<ContentImpl> content;
  kj::Maybe<RecoveryId> recoveryId;

  kj::Maybe<RecoveryId> recoveredId;
  // Recovery ID from a previous run, recovered during this process's recovery phase.

  void moveTo(kj::StringPtr path) {
    KJ_IF_MAYBE(r, recoveryId) {
      KJ_SYSCALL(renameat(
          blobLayer.directoryFd, toPath(*r).cStr(),
          blobLayer.directoryFd, path.cStr()), toPath(*r), path);
      recoveryId = nullptr;
    } else {
      if (!blobLayer.recoveryFinished && recoveredId != nullptr) {
        // Looks like we're recovering, so it's safe to be non-atomic, since we'll reach this
        // point again.
        unlinkat(blobLayer.directoryFd, path.cStr(), 0);
      }
      KJ_SYSCALL(linkat(
          AT_FDCWD, kj::str("/proc/self/fd/", content->getFd()).cStr(),
          blobLayer.directoryFd, path.cStr(), AT_SYMLINK_FOLLOW), path);
    }
  }

  kj::String toPath(RecoveryId id, kj::StringPtr prefix = "") {
    return kj::str(prefix, id.type, '/', id.id);
  }
};

class LocalBlobLayer::ObjectImpl final: public BlobLayer::Object {
public:
  ObjectImpl(LocalBlobLayer& blobLayer, ObjectId id, kj::Own<ContentImpl>&& content)
      : blobLayer(blobLayer), id(id), content(kj::mv(content)) {}

  void overwrite(Xattr xattr, kj::Own<Temporary> replacement) override {
    ++generation;
    content = replacement.downcast<TemporaryImpl>()->moveToMain(id, xattr);
  }

  Xattr getXattr() override {
    Xattr xattr;
    memset(&xattr, 0, sizeof(xattr));
    KJ_SYSCALL(fgetxattr(content->getFd(), Xattr::NAME, &xattr, sizeof(xattr)));
    return xattr;
  }

  void setXattr(Xattr xattr) override {
    ++generation;
    content->setXattr(xattr);
  }

  void remove() override {
    ++generation;
    KJ_SYSCALL(unlinkat(blobLayer.directoryFd,
        kj::str("main/", id.filename('o').begin()).cStr(), 0), id);
  }

  uint64_t getGeneration() override {
    return generation;
  }

  Content& getContent() override {
    return *content;
  }

private:
  LocalBlobLayer& blobLayer;
  ObjectId id;
  kj::Own<ContentImpl> content;
  uint64_t generation = 0;
};

class LocalBlobLayer::RecoveredTemporaryImpl final: public BlobLayer::RecoveredTemporary {
public:
  RecoveredTemporaryImpl(RecoveryId oldId, kj::Own<TemporaryImpl> inner)
      : oldId(oldId), inner(kj::mv(inner)) {}

  RecoveryId getOldId() override {
    return oldId;
  }

  TemporaryXattr getTemporaryXattr() override {
    return inner->getTemporaryXattr();
  }

  BlobLayer::Content& getContent() override {
    return inner->getContent();
  }

  kj::Own<BlobLayer::Temporary> keepAs(RecoveryId newId) override {
    inner->setRecoveryId(newId);
    return kj::mv(inner);
  }

  kj::Own<BlobLayer::Temporary> keepAs(RecoveryId newId, TemporaryXattr newXattr) override {
    inner->setRecoveryId(newId, newXattr);
    return kj::mv(inner);
  }

  void keepAs(ObjectId newId, Xattr xattr) override {
    inner->moveToMain(newId, xattr);
  }

private:
  RecoveryId oldId;
  kj::Own<TemporaryImpl> inner;
};

LocalBlobLayer::LocalBlobLayer(kj::UnixEventPort& eventPort, int directoryFd)
    : eventPort(eventPort), directoryFd(directoryFd) {
  if (faccessat(directoryFd, "recovered", F_OK, AT_EACCESS) >= 0) {
    // Clean up stuff that was successfully recovered in a previous run.
    sandstorm::recursivelyDeleteAt(directoryFd, "recovered");
  }

  // Make main storage if it doesn't already exist.
  mkdirat(directoryFd, "main", 0777);  // might already exist

  // Make the recovery directory and move all the temp directories to it. Note that there may
  // already be a recovery directory if we failed *during* recovery last time! Moreover, it's
  // possible that we failed *while* moving the temp directories in, but it's also possible that
  // we moved the directories in and then created new temp directories as part of recovery and
  // then failed. So be careful to check which directories already exist under "recovery" and
  // only move in the ones that aren't already there.
  mkdirat(directoryFd, "recovery", 0777);  // might already exist

  for (auto type: ALL_RECOVERY_TYPES) {
    auto name = kj::str(type);
    auto recoveryPath = kj::str("recovery/", name);
    bool sourceExists = faccessat(directoryFd, name.cStr(), F_OK, AT_EACCESS) >= 0;
    bool destExists = faccessat(directoryFd, recoveryPath.cStr(), F_OK, AT_EACCESS) >= 0;
    if (destExists) {
      // Looks like the recovery directory already contains this recovery type (this implies that
      // the previous run definitely failed during recovery).
      if (sourceExists) {
        // Looks like we created a new temporary directory during the previous run and possibly
        // started populating it, but we need to delete all that because that run failed.
        sandstorm::recursivelyDeleteAt(directoryFd, name.cStr());
      }
    } else {
      // Did not already move this directory.
      if (sourceExists) {
        // Move it.
        KJ_SYSCALL(renameat(directoryFd, name.cStr(), directoryFd, recoveryPath.cStr()));
      } else {
        // Apparently this recovery directory does not already exist. Perhaps this is the first
        // run, or perhaps a new recovery type was added. Create an empty directory to mark that
        // we got this far.
        KJ_SYSCALL(mkdirat(directoryFd, recoveryPath.cStr(), 0777));
      }
    }

    // Now create the new temporary directory for this run.
    KJ_SYSCALL(mkdirat(directoryFd, name.cStr(), 0777));
  }
}

void LocalBlobLayer::testDirtyExit() {
  testingDirtyExit = true;
}

kj::Maybe<kj::Own<BlobLayer::Object>> LocalBlobLayer::getObject(ObjectId id) {
  KJ_REQUIRE(!recoveryFinished, "finish() already called");

  return getObjectInternal(id);
}

kj::Array<kj::Own<BlobLayer::RecoveredTemporary>>
LocalBlobLayer::recoverTemporaries(RecoveryType type) {
  KJ_REQUIRE(!recoveryFinished, "finish() already called");

  auto typeName = kj::str(type);

  return KJ_MAP(name, sandstorm::listDirectoryAt(directoryFd, kj::str("recovery/", typeName)))
      -> kj::Own<BlobLayer::RecoveredTemporary> {
    uint64_t number = KJ_ASSERT_NONNULL(sandstorm::parseUInt(name, 10));
    RecoveryId id = { type, number };
    return kj::heap<RecoveredTemporaryImpl>(id, kj::heap<TemporaryImpl>(*this, id));
  };
}

BlobLayer& LocalBlobLayer::finish() {
  // Make sure entire recovery process has been synced to disk.
  sync();

  // Move the "recovery" directory over to "recovered" to indicate that we finished recovery.
  KJ_SYSCALL(renameat(directoryFd, "recovery", directoryFd, "recovered"));
  recoveryFinished = true;
  return *this;
}

kj::Own<BlobLayer::Object> LocalBlobLayer::createObject(
    ObjectId id, Xattr xattr, kj::Own<Temporary> content) {
  return kj::heap<ObjectImpl>(*this, id, content.downcast<TemporaryImpl>()->moveToMain(id, xattr));
}

kj::Promise<kj::Maybe<kj::Own<BlobLayer::Object>>> LocalBlobLayer::openObject(ObjectId id) {
  return getObjectInternal(id);
}

kj::Own<BlobLayer::Temporary> LocalBlobLayer::newTemporary() {
  return kj::heap<TemporaryImpl>(*this);
}

kj::Maybe<kj::Own<BlobLayer::Object>> LocalBlobLayer::getObjectInternal(ObjectId id) {
  auto filename = kj::str("main/", id.filename('o'));

  return sandstorm::raiiOpenAtIfExists(directoryFd, filename, O_RDWR | O_CLOEXEC)
      .map([&](kj::AutoCloseFd&& fd) {
    return kj::heap<ObjectImpl>(*this, id, kj::heap<ContentImpl>(kj::mv(fd)));
  });
}

}  // namespace storage
}  // namespace blackrock
