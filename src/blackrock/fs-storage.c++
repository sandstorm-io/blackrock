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

#include "fs-storage.h"
#include <kj/debug.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <sodium/randombytes.h>
#include <sodium/crypto_generichash_blake2b.h>
#include <sandstorm/util.h>
#include <capnp/serialize.h>
#include <sys/eventfd.h>
#include <kj/thread.h>
#include <kj/async-unix.h>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <capnp/persistent.capnp.h>
#include <dirent.h>

namespace blackrock {

namespace {

void preadAllOrZero(int fd, void* data, size_t size, off_t offset) {
  // pread() the whole buffer. If EOF is reached, zero the remainder of the buffer -- i.e. treat
  // the file as having infinite size where all bytes not explicitly written are zero.

  while (size > 0) {
    ssize_t n;
    KJ_SYSCALL(n = pread(fd, data, size, offset));
    if (n == 0) {
      // Reading past EOF. Assume all-zero.
      memset(data, 0, size);
      return;
    }
    data = reinterpret_cast<byte*>(data) + n;
    size -= n;
    offset += n;
  }
}

void pwriteAll(int fd, const void* data, size_t size, off_t offset) {
  while (size > 0) {
    ssize_t n;
    KJ_SYSCALL(n = pwrite(fd, data, size, offset));
    KJ_ASSERT(n != 0, "zero-sized write?");
    data = reinterpret_cast<const byte*>(data) + n;
    size -= n;
    offset += n;
  }
}

uint64_t getFileSize(int fd) {
  struct stat stats;
  KJ_SYSCALL(fstat(fd, &stats));
  return stats.st_size;
}

uint64_t getFileBlockCount(int fd) {
  struct stat stats;
  KJ_SYSCALL(fstat(fd, &stats));
  return (stats.st_blocks * 512 + Volume::BLOCK_SIZE - 1) / Volume::BLOCK_SIZE;
}

uint64_t getFilePosition(int fd) {
  off_t offset;
  KJ_SYSCALL(offset = lseek(fd, 0, SEEK_CUR));
  return offset;
}

template <typename T>
kj::Array<T> removeNulls(kj::Array<kj::Maybe<T>> array) {
  size_t count = 0;
  for (auto& e: array) count += e != nullptr;

  auto result = kj::heapArrayBuilder<T>(count);
  for (auto& e: array) {
    KJ_IF_MAYBE(e2, e) {
      result.add(kj::mv(*e2));
    }
  }

  return result.finish();
}

static constexpr uint64_t EVENTFD_MAX = (uint64_t)-2;

typedef capnp::Persistent<SturdyRef, SturdyRef::Owner> StandardPersistent;
typedef capnp::CallContext<StandardPersistent::SaveParams, StandardPersistent::SaveResults>
     StandardSaveContext;

class RefcountedMallocMessageBuilder: public kj::Refcounted {
public:
  template <typename T>
  inline typename T::Builder getRoot() {
    return capTable.imbue(message.getRoot<T>());
  }

  inline kj::ArrayPtr<kj::Maybe<kj::Own<capnp::ClientHook>>> getCapTable() {
    return capTable.getTable();
  }

  void writeToFd(int fd) {
    capnp::writeMessageToFd(fd, message);
  }

private:
  capnp::MallocMessageBuilder message;
  capnp::BuilderCapabilityTable capTable;
};

template <size_t size>
kj::StringPtr fixedStr(kj::FixedArray<char, size>& data) {
  return kj::StringPtr(data.begin(), size - 1);
}

kj::FixedArray<char, 17> hex64(uint64_t value) {
  kj::FixedArray<char, 17> result;
  static const char DIGITS[] = "0123456789ABCDEF";
  for (uint i = 0; i < 16; i++) {
    // Big-endian sucks. Look at this ugly subtraction.
    result[15 - i] = DIGITS[(value >> (4*i)) & 0x0fu];
  }
  result[16] = '\0';
  return result;
}

}  // namespace

// =======================================================================================

FilesystemStorage::ObjectKey FilesystemStorage::ObjectKey::generate() {
  ObjectKey result;
  randombytes_buf(result.key, sizeof(result.key));

  return result;
}

FilesystemStorage::ObjectId::ObjectId(const ObjectKey &key) {
  KJ_ASSERT(crypto_generichash_blake2b(
      reinterpret_cast<byte*>(id), sizeof(id),
      reinterpret_cast<const byte*>(key.key), sizeof(key.key),
      nullptr, 0) == 0);
}

kj::FixedArray<char, 24> FilesystemStorage::ObjectId::filename(char prefix) const {
  // base64 the ID to create a filename.

  static const char DIGITS[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  kj::FixedArray<char, 24> result;

  const byte* __restrict__ input = reinterpret_cast<const byte*>(id);
  const byte* end = input + sizeof(id);
  char* __restrict__ output = result.begin();

  *output++ = prefix;

  uint window = 0;
  uint windowBits = 0;
  while (input < end) {
    window <<= 8;
    window |= *input++;
    windowBits += 8;
    while (windowBits >= 6) {
      windowBits -= 6;
      *output++ = DIGITS[(window >> windowBits) & 0x37];
    }
  }

  if (windowBits > 0) {
    window <<= 6 - windowBits;
    *output++ = DIGITS[window & 0x37];
  }

  *output++ = '\0';

  KJ_ASSERT(output == result.end());
  return result;
}

enum class FilesystemStorage::Type: uint8_t {
  // (zero skipped to help detect errors)
  BLOB = 1,
  VOLUME,
  IMMUTABLE,
  ASSIGNABLE,
  COLLECTION,
  OPAQUE,
  REFERENCE
};

struct FilesystemStorage::Xattr {
  // Format of the xattr block stored on each file. On ext4 we have about 76 bytes available in
  // the inode to store this attribute, but in theory this space could get smaller in the future,
  // so we should try to keep this minimal.

  static constexpr const char* NAME = "user.sandstor";
  // Extended attribute name. Abbreviated to be 8 bytes to avoid losing space to alignment (ext4
  // doesn't store the "user." prefix). Actually short for "sandstore", not "sandstorm". :)

  Type type;

  bool readOnly;
  // For volumes, prevents the volume from being modified. For Blobs, indicates that initialization
  // has completed with a `done()` call, indicating the entire stream was received (otherwise,
  // either the stream is still uploading, or it failed to fully upload). Once set this
  // can never be unset.

  byte reserved[2];
  // Must be zero.

  uint32_t accountedBlockCount;
  // The number of 4k blocks consumed by this object the last time we considered it for
  // accounting/quota purposes. The on-disk size could have changed in the meantime.

  uint64_t transitiveBlockCount;
  // The number of 4k blocks in this object and all child objects.

  ObjectId owner;
  // What object owns this one?
};

class FilesystemStorage::DeathRow {
public:
  explicit DeathRow(FilesystemStorage& storage)
      : storage(storage),
        eventFd(newEventFd(0, EFD_CLOEXEC)),
        thread([this]() { doThread(); }) {}

  ~DeathRow() noexcept(false) {
    // Write the maximum possible value to the eventfd. This write will actually block until the
    // eventfd reaches 0, which is nice because it means the processing thread will be able to
    // receive the previous event and process it before it receives this one, resulting in clean
    // shutdown.
    writeEvent(eventFd, EVENTFD_MAX);

    // Now the destructor of the thread will wait for the thread to exit.
  }

  void notifyNewInmates() {
    // Alert deleter thread that there's new stuff.
    writeEvent(eventFd, 1);
  }

private:
  FilesystemStorage& storage;
  kj::AutoCloseFd eventFd;
  kj::Thread thread;

  // TODO(perf): Replace use of eventFd in DeathRow and in Journal with a thread signaling
  //   primitive ultimately based on futex?

  void doThread() {
    KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
      for (;;) {
        // Scan directory, delete all files.
        auto files = sandstorm::listDirectoryFd(storage.deathRowFd);
        if (files.size() == 0) {
          // Wait for signal that more files have arrived to be deleted.
          uint64_t count = readEvent(eventFd);
          if (count == EVENTFD_MAX) {
            // Clean shutdown requested.
            break;
          }
        } else {
          // Delete the files, but not before moving their children to death row.
          for (auto& file: files) {
            auto fd = sandstorm::raiiOpenAt(storage.deathRowFd, file, O_RDONLY | O_CLOEXEC);
            Xattr xattr;
            memset(&xattr, 0, sizeof(xattr));
            KJ_SYSCALL(fgetxattr(fd, Xattr::NAME, &xattr, sizeof(xattr)));
            if (isStoredObjectType(xattr.type)) {
              // Read children to move them to death row.
              capnp::StreamFdMessageReader reader(fd.get());

              for (auto child: reader.getRoot<StoredChildIds>().getChildren()) {
                storage.moveToDeathRowIfExists(child, false);
              };
            }
            KJ_SYSCALL(unlinkat(storage.deathRowFd, file.cStr(), 0));
          }
        }
      }
    })) {
      // exception!
      KJ_LOG(FATAL, "exception in death row thread", *exception);

      // Tear down the process because we're no longer garbage-collecting.
      abort();
    }
  }
};

class FilesystemStorage::Journal {
  struct Entry;
public:
  Journal(FilesystemStorage& storage, kj::UnixEventPort& unixEventPort, kj::AutoCloseFd journalFd)
      : storage(storage),
        journalFd(kj::mv(journalFd)),
        journalEnd(getFileSize(this->journalFd)),
        journalSynced(journalEnd),
        journalExecuted(journalEnd),
        journalReadyEventFd(newEventFd(0, EFD_CLOEXEC)),
        journalProcessedEventFd(newEventFd(0, EFD_CLOEXEC | EFD_NONBLOCK)),
        journalProcessedEventFdObserver(unixEventPort, journalProcessedEventFd,
            kj::UnixEventPort::FdObserver::OBSERVE_READ),
        syncQueueTask(syncQueueLoop().catch_([](kj::Exception&& exception) {
          KJ_LOG(FATAL, "journal sync loop threw exception", exception);
          abort();
        })),
        processingThread([this]() { doProcessingThread(); }) {
    KJ_ON_SCOPE_FAILURE(writeEvent(journalReadyEventFd, EVENTFD_MAX));
    doRecovery();
  }

  ~Journal() noexcept(false) {
    // Write the maximum possible value to the eventfd. This write will actually block until the
    // eventfd reaches 0, which is nice because it means the processing thread will be able to
    // receive the previous event and process it before it receives this one, resulting in clean
    // shutdown.
    writeEvent(journalReadyEventFd, EVENTFD_MAX);

    // Now the destructor of the thread will wait for the thread to exit.
  }

  kj::Maybe<kj::AutoCloseFd> openObject(ObjectId id, Xattr& xattr) {
    // Obtain a file descriptor and current attributes for the given object, as if all transactions
    // had already completed. `xattr` is filled in with the attributes.

    auto iter = cache.find(id);
    if (iter == cache.end()) {
      return storage.openObject(id).map([&](kj::AutoCloseFd&& result) {
        memset(&xattr, 0, sizeof(xattr));
        KJ_SYSCALL(fgetxattr(result, Xattr::NAME, &xattr, sizeof(xattr)));
        return kj::mv(result);
      });
    } else {
      xattr = iter->second.xattr;
      if (iter->second.stagingId != 0) {
        KJ_IF_MAYBE(fd, storage.openStaging(iter->second.stagingId)) {
          return kj::mv(*fd);
        }
      }
      // Note: Even though it's in cache, the file may not be present on disk if it was recently
      //   deleted.
      return storage.openObject(id);
    }
  }

  kj::AutoCloseFd createTempFile() {
    return storage.createTempFile();
  }

  class Transaction: private kj::ExceptionCallback {
  public:
    explicit Transaction(Journal& journal): journal(journal) {
      KJ_REQUIRE(!journal.txInProgress, "only one transaction is allowed at a time");
      journal.txInProgress = true;
    }
    ~Transaction() noexcept(false) {
      if (journal.txInProgress) {
        // Bad news: We built part of a transaction and failed to finish it. Unfortunately our
        // in-memory objects are now in an inconsistent state compared to disk. We have no choice
        // but to abort the process and recover from journal. :(
        //
        // TODO(someday): We could perhaps make this less severe by only killing the objects
        //   involved in the transaction (make them all start throwing DISCONNECTED, remove them
        //   from the already-open table).
        KJ_LOG(FATAL, "INCOMPLETE TRANSACTION; ABORTING");
        abort();
      }
    }

    KJ_DISALLOW_COPY(Transaction);

    void createObject(ObjectId id, const Xattr& attributes, int tmpFd) {
      // Replace the object on disk with the file referenced by `tmpFd` with the given attributes.
      //
      // Should never replace an existing object.

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Link temp file into staging.
      uint64_t stagingId = journal.nextStagingId++;
      journal.storage.linkTempIntoStaging(stagingId, tmpFd, attributes);

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.type = Entry::Type::CREATE_OBJECT;
      entry.stagingId = stagingId;
      entry.objectId = id;
      entry.xattr = attributes;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.location = CacheEntry::Location::STAGING;
      cache.stagingId = stagingId;
      cache.xattr = attributes;
      journal.cacheDropQueue.push({cache.lastUpdate, entry.objectId});
    }

    void updateObject(ObjectId id, const Xattr& attributes, int tmpFd) {
      // Replace the object on disk with the file referenced by `tmpFd` with the given attributes.

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Link temp file into staging.
      uint64_t stagingId = journal.nextStagingId++;
      journal.storage.linkTempIntoStaging(stagingId, tmpFd, attributes);

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.type = Entry::Type::UPDATE_OBJECT;
      entry.stagingId = stagingId;
      entry.objectId = id;
      entry.xattr = attributes;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.location = CacheEntry::Location::STAGING;
      cache.stagingId = stagingId;
      cache.xattr = attributes;
      journal.cacheDropQueue.push({cache.lastUpdate, entry.objectId});
    }

    void updateObjectXattr(ObjectId id, const Xattr& attributes) {
      // Overwrite the object's attributes with the given ones.

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.type = Entry::Type::UPDATE_XATTR;
      entry.objectId = id;
      entry.xattr = attributes;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.xattr = attributes;
      journal.cacheDropQueue.push({cache.lastUpdate, entry.objectId});
    }

    uint64_t moveToDeathRow(ObjectId id) {
      // Delete an object (recursively, if it has children).
      //
      // Returns the number of blocks transitively erased.

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.type = Entry::Type::MOVE_TO_DEATH_ROW;
      entry.objectId = id;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      bool isNew = cache.location == CacheEntry::Location::UNDEFINED;
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.location = CacheEntry::Location::DELETED;
      journal.cacheDropQueue.push({cache.lastUpdate, entry.objectId});

      if (isNew) {
        // I guess we have to open this file to get the transitive size.
        memset(&cache.xattr, 0, sizeof(cache.xattr));
        KJ_IF_MAYBE(fd, journal.storage.openObject(id)) {
          KJ_SYSCALL(fgetxattr(*fd, Xattr::NAME, &cache.xattr, sizeof(cache.xattr)));
        } else {
          // Apparently this object isn't on disk. This can happen if the parent (from which this
          // object is being deleted) has itself already been deleted asynchronously. We can safely
          // set any value here because updating the parent's size is irrelevant at this point.
        }
      }

      // Prevent later changes to the object or its children (if there are still live refs) from
      // affecting the owner.
      cache.xattr.owner = nullptr;

      return cache.xattr.transitiveBlockCount;
    }

    kj::Promise<void> commit() {
      // Commit the transaction, resolving when the transaction is safely written to the journal on
      // disk.
      //
      // The `Transaction` object can (and should) be destroyed as soon as `commit()` returns; the
      // `Promise` is free-standing. Dropping the promise will NOT cancel submission of the
      // transaction (but the power could go out before it is fully committed).

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Set `txSize` for all entries in the transaction.
      size_t i = entries.size();
      for (auto& entry: entries) {
        entry.txSize = i--;
      }
      KJ_DASSERT(i == 0);

      // Write the whole transaction to disk.
      auto bytes = entries.asPtr().asBytes();
      pwriteAll(journal.journalFd, bytes.begin(), bytes.size(), journal.journalEnd);
      journal.journalEnd += bytes.size();

      // Notify journal thread.
      writeEvent(journal.journalReadyEventFd, entries.size());

      journal.txInProgress = false;

      // Arrange to be notified when sync completes.
      auto paf = kj::newPromiseAndFulfiller<void>();
      journal.syncQueue.push({journal.journalEnd, kj::mv(paf.fulfiller)});
      return kj::mv(paf.promise);
    }

  private:
    Journal& journal;
    kj::UnwindDetector unwindDetector;

    kj::Vector<Entry> entries;
    // The entries being written.

    // We implement ExceptionCallback in order to log exceptions being thrown that are likely
    // to force us to abort in the destructor. Unfortunately there is apparently no way to
    // determine the exception being thrown *during* the destructor.

    void onRecoverableException(kj::Exception&& exception) override {
      if (journal.txInProgress) {
        KJ_LOG(ERROR, "exception during transaction", exception);
      }
      kj::ExceptionCallback::onRecoverableException(kj::mv(exception));
    }

    void onFatalException(kj::Exception&& exception) override {
      if (journal.txInProgress) {
        KJ_LOG(ERROR, "exception during transaction", exception);
      }
      kj::ExceptionCallback::onFatalException(kj::mv(exception));
    }
  };

private:
  struct Entry {
    // In order to implement atomic transactions, we organize disk changes into a stream of
    // idempotent modifications. Each change is appended to the journal before being actually
    // performed, so that on system failure we can replay the journal to get up-to-date.

    // Note that the logical "first" fields are `type` and `txSize`, but we put them at the end of
    // the Entry in order to better-detect when an entry is only partially-written. I'm actually
    // not sure that such a thing is possible, but being safe is free in this case.

    uint64_t stagingId;
    // If non-zero, identifies a staging file which should be rename()ed to replace this object.
    // The xattrs should be written to the file just before it is renamed. If no such staging file
    // exists, this operation probably already occurred; ignore it.
    //
    // If zero, then an existing file should be modified in-place. If there is no existing file
    // matching the ID, we are probably replaying an operation that was already completed, and some
    // later operation probably deletes this object; ignore the op.

    ObjectId objectId;
    // ID of the object to update.

    Xattr xattr;
    // Updated Xattr structure to write into the file.

    enum class Type :uint8_t {
      CREATE_OBJECT,
      // Move the staging file identified by `stagingId` into main storage, first setting the xattrs
      // on the staging file, then rename()ing it into place, expecting that there is no existing
      // file there already. If no such staging file exists, this operation probably already
      // occurred; ignore it.

      UPDATE_OBJECT,
      // Replace the object with a staging file identified by `stagingId`, first setting the xattrs
      // on the staging file, then rename()ing it into place. If no such staging file exists, this
      // operation probably already occurred; ignore it.
      //
      // If there is not already a file in the target location, assume it was asynchronously
      // deleted and immediately delete this file as well.

      UPDATE_XATTR,
      // Update the xattrs on an existing file.

      MOVE_TO_DEATH_ROW
      // Move this object's file from main storage to death row.
    };

    Type type;
    // Transaction type.

    byte reserved[3];

    uint32_t txSize;
    // Number of entries remaining in this transaction, including this one. Do not start applying
    // operations unless the full transaction is available and all `txSize` values are correct.
    // If recovering from a failure, ignore an incomplete transaction at the end of the journal.
  };

  static_assert(sizeof(Entry) == 64,
      "journal entry size changed; please keep power-of-two and consider migration issues");
  // We want the entry size to be a power of two so that they are page-aligned.

  FilesystemStorage& storage;

  kj::AutoCloseFd journalFd;
  uint64_t journalEnd;
  uint64_t journalSynced;
  uint64_t journalExecuted;

  kj::AutoCloseFd journalReadyEventFd;
  kj::AutoCloseFd journalProcessedEventFd;
  kj::UnixEventPort::FdObserver journalProcessedEventFdObserver;
  // Event FDs used to communicate with journal thread.
  //
  // When a new transaction is written, the main thread posts the number of entries in the
  // transaction to `journalReadyEventFd`. When the journal processing thread receives this event,
  // it syncs the journal to disk, then posts the number of bytes processed to
  // `journalProcessedEventFd`.

  struct CacheEntry {
    enum class Location :uint8_t {
      UNDEFINED,
      // Indicates a blank cache entry.

      NORMAL,
      // The file -- if it exists -- is in its normal location.

      STAGING,
      // The file *may* still be in staging, under `stagingId`. If `stagingId` no longer exists,
      // then the file is actually now live.

      DELETED
      // The file has been deleted, but may still be present on disk.
    };

    Location location = Location::UNDEFINED;

    uint64_t lastUpdate;
    // Offset in the journal of the last update to this object. We may discard the cache entry
    // once the journal has been committed past this point.

    uint64_t stagingId;
    // ID of staging file which is the object's current content. If this file no longer exists,
    // then it has been moved to the file's final location.

    Xattr xattr;
    // Attributes as of the last update.
  };
  std::unordered_map<ObjectId, CacheEntry, ObjectId::Hash> cache;
  // Cache of attribute changes that are in the journal but haven't been written to disk yet.

  uint32_t nextStagingId = 0;
  // Counter to use to generate staging file names. The names are 7-digit zero-padded hex.

  bool txInProgress = false;
  // True if a `Transaction` exists which has not been committed.

  struct SyncQueueEntry {
    uint64_t offset;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
  };
  std::queue<SyncQueueEntry> syncQueue;
  kj::Promise<void> syncQueueTask;

  struct CacheDropQueueEntry {
    uint64_t offset;
    ObjectId objectId;
  };
  std::queue<CacheDropQueueEntry> cacheDropQueue;
  // Queue used to decide when to drop entries from `cache`. Whenever we add a new cache entry,
  // we also add an entry to `cacheDropQueue` with `offset` equal to the point the journal must
  // reach before the new cache entry is no longer needed. Keep in mind that cache entries might
  // be overwritten with later modifications and therefore we must check the current value of
  // the cache entry, not just delete it indiscriminently.

  kj::Thread processingThread;

  kj::Promise<void> syncQueueLoop() {
    return journalProcessedEventFdObserver.whenBecomesReadable().then([this]() {
      uint64_t byteCount;
      ssize_t n;
      KJ_NONBLOCKING_SYSCALL(n = read(journalProcessedEventFd, &byteCount, sizeof(byteCount)));

      if (n < 0) {
        // Oops, not actually ready.
      } else {
        KJ_ASSERT(n == sizeof(byteCount), "eventfd read had unexpected size", n);
        journalSynced += byteCount;
        while (!syncQueue.empty() && syncQueue.front().offset <= journalSynced) {
          syncQueue.front().fulfiller->fulfill();
          syncQueue.pop();
        }

        while (!cacheDropQueue.empty() && cacheDropQueue.front().offset <= journalExecuted) {
          auto iter = cache.find(cacheDropQueue.front().objectId);
          if (iter != cache.end() && iter->second.lastUpdate <= journalExecuted) {
            // This cache entry is not longer needed.
            cache.erase(iter);
          }
          cacheDropQueue.pop();
        }
      }

      return syncQueueLoop();
    });
  }

  void doRecovery() {
    // Find the first actual data (skip leading hole).
  retry:
    off_t position = lseek(journalFd, 0, SEEK_DATA);
    if (position < 0) {
      int error = errno;
      if (error == EINTR) {
        goto retry;
      } else if (error == ENXIO) {
        // The journal is empty.
        return;
      } else {
        KJ_FAIL_SYSCALL("lseek", error);
      }
    }

    if (journalEnd > position) {
      // Recover from previous journal failure.

      // Read all entries.
      auto entries = kj::heapArray<Entry>((journalEnd - position) / sizeof(Entry));
      preadAllOrZero(journalFd, entries.begin(), entries.asBytes().size(), position);

      // Process valid entries and discard any incomplete transaction.
      for (auto& entry: validateEntries(entries, true)) {
        executeEntry(entry);
      }
    }

    storage.deleteAllStaging();
  }

  void doProcessingThread() {
    // This thread reads the journal, makes sure things are synced to disk, and actually executes
    // the transactions.

    KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
      // Get the current position from journalSynced rather than journalEnd since journalEnd could
      // possibly have changed already, but journalSynced can't change until we signal back to the
      // main thread.
      uint64_t position = journalSynced;

      for (;;) {
        // Wait for some data to read.
        uint64_t count = readEvent(journalReadyEventFd);

        KJ_ASSERT(count > 0);

        if (count == EVENTFD_MAX) {
          // Clean shutdown requested.
          break;
        }

        // Read the entries.
        auto entries = kj::heapArray<Entry>(count);
        preadAllOrZero(journalFd, entries.begin(), entries.asBytes().size(), position);

        // Make sure the journal is synced. We do fsync() instead of fdatasync() because:
        // - We want to make sure that the metadata for all staging files used in this transaction
        //   are also synced. Ext4 by default guarantees ordering of metadata changes, so syncing
        //   the journal metadata should ensure that all the other files are in-place.
        // - It probably makes no difference anyway because we always extend the endpoint of the
        //   journal when adding a transaction, therefore a metadata flush is necessary even if
        //   we use fdatasync().
        KJ_SYSCALL(fsync(journalFd));

        // Post back to main thread that sync is finished through these bytes.
        uint64_t byteCount = entries.asBytes().size();
        writeEvent(journalProcessedEventFd, byteCount);

        // Now process them.
        for (auto& entry: validateEntries(entries, false)) {
          executeEntry(entry);
        }

        storage.sync();

        // Now we can punch out any journal pages we've completed.
        static constexpr uint64_t pageMask = ~4095ull;
        uint64_t holeStart = position & pageMask;
        position += byteCount;
        uint64_t holeEnd = position & pageMask;

        // Instead of using a second eventFd, we just update `journalExecuted` with a sloppy memory
        // write, because it's only used to decide when to clear cache entries anyway.
        __atomic_store_n(&journalExecuted, position, __ATOMIC_RELAXED);

        if (holeStart < holeEnd) {
          KJ_SYSCALL(fallocate(journalFd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                               holeStart, holeEnd - holeStart),
                     holeStart, holeEnd);
        }
      }

      // On clean shutdown, the journal is empty and we can discard it all.
      KJ_ASSERT(getFileSize(journalFd) == position, "journal not empty after clean shutdown");
      KJ_SYSCALL(ftruncate(journalFd, 0));
    })) {
      // exception!
      KJ_LOG(FATAL, "exception in journal thread", *exception);

      // Tear down the process because otherwise it will probably deadlock.
      abort();
    }
  }

  kj::ArrayPtr<const Entry> validateEntries(
      kj::ArrayPtr<const Entry> entries, bool discardIncompleteTrailing) {
    uint64_t expected = 0;
    const Entry* end = entries.begin();
    for (auto& entry: entries) {
      if (entry.txSize == 0 && discardIncompleteTrailing) {
        // txSize of zero is illegal, but could be caused by ext4 failure mode in which the file
        // size was advanced before the content was flushed and then a power failure occurred. In
        // this case, we expect the entire rest of the contet to be zero.
        //
        // Note that we can safely assume that zeros don't begin mid-entry because entries are
        // always block-aligned and thus atomically written.
        for (auto b: kj::arrayPtr(reinterpret_cast<const byte*>(&entry),
                                  entries.asBytes().end())) {
          if (KJ_UNLIKELY(b != 0)) {
            KJ_FAIL_ASSERT("journal corrupted");
          }
        }

        // Looks like this was indeed a case of ext4 zero-extending our journal.
        //
        // Note that in practice `expected` should always be zero here: ext4 only ever zero-extends
        // a file to the end of the current block, and all entries in the same transaction and same
        // block are written at the same time atomically.
        KJ_LOG(ERROR, "detected ext4 zero-extension on journal recovery", expected);

        if (expected == 0) {
          // The previous transaction completed, though!
          end = &entry;
        }
        return kj::arrayPtr(entries.begin(), end);
      }

      if (expected == 0) {
        // We expect to start a new transaction.
        KJ_ASSERT(entry.txSize != 0, "journal corrupted");
        expected = entry.txSize;

        // Everything before this point is valid.
        end = &entry;
      } else {
        KJ_ASSERT(entry.txSize == expected, "journal corrupted");
      }
      --expected;

      KJ_ASSERT((entry.reserved[0] | entry.reserved[1] | entry.reserved[2]) == 0,
          "journal contains entries I don't understand; seems it was written using a future "
          "version or simply corrupted");
    }
    if (expected == 0) {
      // We ended at the end of a transaction -- yay.
      end = entries.end();
    } else if (!discardIncompleteTrailing) {
      KJ_FAIL_ASSERT("incomplete transaction written to journal");
    }

    return kj::arrayPtr(entries.begin(), end);
  }

  void executeEntry(const Entry& entry) {
    switch (entry.type) {
      case Entry::Type::CREATE_OBJECT:
        storage.createFromStagingIfExists(entry.stagingId, entry.objectId, entry.xattr);
        break;
      case Entry::Type::UPDATE_OBJECT:
        storage.replaceFromStagingIfExists(entry.stagingId, entry.objectId, entry.xattr);
        break;
      case Entry::Type::UPDATE_XATTR:
        storage.setAttributesIfExists(entry.objectId, entry.xattr);
        break;
      case Entry::Type::MOVE_TO_DEATH_ROW:
        storage.moveToDeathRowIfExists(entry.objectId);
        break;
    }
  }
};

// =======================================================================================

class FilesystemStorage::ObjectFactory: public kj::Refcounted {
  // Class responsible for keeping track of live objects.
  //
  // This is refcounted because ObjectBase's destructor needs to call it, and it's hard to ensure
  // that ObjectBase's destructor runs before FilesystemStorage's destructor when an exception
  // unwinds the stack while an RPC to storage is in-progress in parallel. (This mostly happens in
  // tests.)

public:
  explicit ObjectFactory(Journal& journal, kj::Timer& timer,
                         Restorer<SturdyRef>::Client&& restorer);

  template <typename T, typename U>
  struct ClientObjectPair {
    typename T::Client client;
    U& object;

    template <typename OtherT, typename OtherU>
    ClientObjectPair(ClientObjectPair<OtherT, OtherU>&& other)
        : client(kj::mv(other.client)), object(other.object) {}
    ClientObjectPair(typename T::Client client, U& object)
        : client(kj::mv(client)), object(object) {}
  };

  template <typename T>
  ClientObjectPair<typename T::Serves, T> newObject();
  // Create a new storage object with random ID of type T, where T is a subclass of ObjectBase
  // inheriting all of ObjectBase's constructors.

  ClientObjectPair<capnp::Capability, ObjectBase> openObject(ObjectKey key);

  kj::Promise<kj::Maybe<ObjectBase&>> getLiveObject(capnp::Capability::Client& client);
  // If the given capability points to an OwnedStorage implemented by this server, get the
  // underlying ObjectBase.

  void destroyed(ObjectBase& object);
  // Called by destructor of ObjectBase. Shouldn't be called anywhere else.

  auto restoreRequest() { return restorer.restoreRequest(); }
  auto dropRequest() { return restorer.dropRequest(); }
  // Call methods on the `Restorer` capbaility.

  inline kj::Timer& getTimer() { return timer; }

  void modifyTransitiveSize(ObjectId id, int64_t deltaBlocks, Journal::Transaction& txn);
  // Update the transitive size of the given object and its parents, adding `deltaBlocks` to each.
  // Call this when a new child was added.

  void disowned(ObjectId id);
  // Notes that the given object ID has been disowned by its owner. If the object is live, it needs
  // to have its owner reference cleared so that any later changes to the object's size don't
  // cause the owner to be updated.

private:
  Journal& journal;
  kj::Timer& timer;

  capnp::CapabilityServerSet<capnp::Capability> serverSet;
  // Lets us map our own capabilities -- when they come back from the caller -- back to the
  // underlying objects.

  std::unordered_map<ObjectId, ObjectBase*, ObjectId::Hash> objectCache;
  // Maps object IDs to live objects representing them, if any.

  Restorer<SturdyRef>::Client restorer;

  template <typename T>
  ClientObjectPair<typename T::Serves, T> registerObject(kj::Own<T> object);
};

// =======================================================================================

class FilesystemStorage::ObjectBase: public virtual capnp::Capability::Server {
  // Base class which all persistent storage objects implement. Often accessed by first
  // unwrapping a Capability::Client into a native object and then doing dynamic_cast.

public:
  ObjectBase(Journal& journal, kj::Own<ObjectFactory> factory, Type type)
      : journal(journal), factory(kj::mv(factory)),
        key(ObjectKey::generate()), id(key), state(ORPHAN) {
    // Create a new object. A key will be generated.

    memset(&xattr, 0, sizeof(xattr));
    xattr.type = type;
  }

  ObjectBase(Journal& journal, kj::Own<ObjectFactory> factory,
             const ObjectKey& key, const ObjectId& id, const Xattr& xattr,
             kj::AutoCloseFd fd)
      : journal(journal), factory(kj::mv(factory)),
        key(key), id(id), xattr(xattr), state(COMMITTED) {
    // Construct an ObjectBase around an existing on-disk object.

    CurrentData data;
    data.fd = kj::mv(fd);

    if (isStoredObjectType(xattr.type)) {
      capnp::StreamFdMessageReader reader(data.fd.get());

      data.children = KJ_MAP(child, reader.getRoot<StoredChildIds>().getChildren()) {
        return ObjectId(child);
      };

      data.storedChildIdsWords = getFilePosition(data.fd) / sizeof(capnp::word);
      data.storedObjectWords = getFileSize(data.fd) / sizeof(capnp::word) -
                               data.storedChildIdsWords;
    } else {
      data.storedChildIdsWords = 0;
      data.storedObjectWords = 0;
    }

    currentData = kj::mv(data);
  }

  ~ObjectBase() noexcept(false) {
    factory->destroyed(*this);

    // Note: If the object hasn't been committed yet, then our FD is an unlinked temp file and
    // closing it will delete the data from disk, so we don't have to worry about it here. If the
    // file has been linked to disk, then either it's in staging as part of a not-yet-committed
    // transaction, or it's all the way in main storage already.
  }

  capnp::Capability::Client self() {
    return thisCap();
  }

  inline const ObjectId& getId() const { return id; }
  inline const ObjectKey& getKey() const { return key; }
  inline Xattr& getXattrRef() { return xattr; }

  class AdoptionIntent {
    // When an orphaned object is being adopted by a new owner, first the new owner has to ensure
    // that all of the objects it proposed to adopt are adoptable before it actually commits to
    // the transaction that adopts them. So, it goes around creating AdoptionIntents for each one.
    // These are reversible -- if an exception is thrown and the AdoptionIntent discarded, no
    // harm is done. But, only one owner can intend to adopt a particular orphan at a time. Once
    // all the intents are created, the owner can actually commit a transaction adopting them.

  public:
    AdoptionIntent(ObjectBase& object, capnp::Capability::Client cap)
        : object(object), cap(kj::mv(cap)), committed(false) {
      KJ_REQUIRE(object.state == ORPHAN, "can't take OwnedStorage already owned by someone else");
      KJ_REQUIRE(object.currentData != nullptr, "can't adopt uninitialized object");
      object.state = CLAIMED;
    }

    ~AdoptionIntent() {
      if (!committed) {
        object.state = ORPHAN;
      }
    }

    KJ_DISALLOW_COPY(AdoptionIntent);
    inline AdoptionIntent(AdoptionIntent&& other)
        : object(other.object), cap(kj::mv(other.cap)), committed(other.committed) {
      other.committed = true;  // make sure `other`'s destructor does nothing
    }

    const ObjectId& getId() const { return object.getId(); }

    uint64_t prepCommit(ObjectId owner) {
      // Must call before commit() (but after opening the transaction) to prepare all Xattrs with
      // correct transitive size numbers.
      //
      // Returns the transitive block count for the adopted object.

      KJ_ASSERT(!prepped);
      prepped = true;

      object.xattr.owner = owner;

      // Prep children.
      for (auto& adoption: KJ_ASSERT_NONNULL(object.currentData).transitiveAdoptions) {
        object.xattr.transitiveBlockCount += adoption.prepCommit(object.id);
      }

      return object.xattr.transitiveBlockCount;
    }

    void commit(Journal::Transaction& transaction) {
      // Add an operation to the transaction which officially adopts this object.

      KJ_ASSERT(prepped);
      KJ_ASSERT(!committed);
      committed = true;
      object.state = COMMITTED;

      auto& data = KJ_ASSERT_NONNULL(object.currentData);

      // Currently, only adopting of newly-created objects is allowed, so we know data.fd is a
      // temp file, and we should call createObject() here. Later, when we support ownership
      // transfers, this may not be true.
      transaction.createObject(object.id, object.xattr, data.fd);

      // Recurse to all children. Note that it's important to move the parent into place before
      // children because the code that finalizes an object will immediately delete it if the
      // parent doesn't exist.
      for (auto& adoption: data.transitiveAdoptions) {
        adoption.commit(transaction);
      }
      data.transitiveAdoptions = nullptr;
    }

  private:
    ObjectBase& object;
    capnp::Capability::Client cap;  // Make sure `object` can't be deleted.
    bool committed;
    bool prepped = false;
  };

protected:
  kj::Promise<void> setStoredObject(capnp::AnyPointer::Reader value) {
    // Overwrites the object with the given value saved as a StoredObject.

    KJ_ASSERT(value.targetSize().wordCount < (1u << 21),
        "Stored Cap'n Proto objects must be less than 16MB. Use Volume or Blob for bulk data.");

    uint seqnum = nextSetSeqnum++;

    // Start constructing the message to write to disk, copying over the input.
    // TODO(security): Protect encryption keys in this message by zeroing it in the destructor.
    //   Also, make sure the RPC system zeros plaintext of messages when encryption is used.
    auto message = kj::refcounted<RefcountedMallocMessageBuilder>();
    auto root = message->getRoot<StoredObject>();
    root.getPayload().set(value);

    // Arrange to save each capability to the cap table.
    auto capTableIn = message->getCapTable();
    auto capTableOut = root.initCapTable(capTableIn.size());
    auto promises = kj::heapArrayBuilder<kj::Promise<kj::Maybe<SavedChild>>>(capTableIn.size());
    for (auto i: kj::indices(capTableIn)) {
      KJ_IF_MAYBE(cap, capTableIn[i]) {
        promises.add(saveCap(cap->get()->addRef(), capTableOut[i]));
      } else {
        promises.add(kj::Maybe<SavedChild>(nullptr));
      }
    }

    // Cache the value that we are writing, to serve get()s in the meantime.
    auto oldCachedValue = kj::mv(cachedValue);
    auto oldSeqnum = nextSetSeqnum - 1;
    cachedValue = kj::addRef(*message);
    RefcountedMallocMessageBuilder& msgRef = *message;
    auto dropCache = kj::defer([&msgRef,KJ_MVCAP(oldCachedValue),oldSeqnum,this]() mutable {
      KJ_IF_MAYBE(c, cachedValue) {
        if (c->get() == &msgRef) {
          // The value we are writing is still cached. Remove it.
          if (commitedSetSeqnum < oldSeqnum) {
            // The old value is not commited yet (and neither was the new value, so I guess an
            // exception was thrown), so restore it.
            cachedValue = kj::mv(oldCachedValue);
          } else {
            cachedValue = nullptr;
          }
        }
      }
    });

    // Wait for all the saves to complete.
    return kj::joinPromises(promises.finish())
        .then([KJ_MVCAP(message),KJ_MVCAP(dropCache),seqnum,this](
            kj::Array<kj::Maybe<SavedChild>> results) mutable -> kj::Promise<void> {
      if (commitedSetSeqnum > seqnum) {
        // Some later set() was already written to disk.
        // TODO(leak): Drop references that we just saved.
        return kj::READY_NOW;
      }

      // Children we need to disown.
      std::unordered_set<ObjectId, ObjectId::Hash> disowned;

      // Intents to adopt each child in `adopted`.
      kj::Vector<AdoptionIntent> adoptions;

      // New children after the change.
      std::unordered_set<ObjectId, ObjectId::Hash> newChildren;

      // Fill in `disowned` and `adoptions` based on results from saves.
      {
        auto savedChildren = removeNulls(kj::mv(results));

        KJ_IF_MAYBE(data, currentData) {
          // We're replacing some existing data.

          // Children we had before that we're removing, in set form.
          std::unordered_set<ObjectId, ObjectId::Hash> oldChildren(
              data->children.begin(), data->children.end());

          // Start with disowned being a copy of oldChildren, and then remove children that we
          // still have.
          disowned = oldChildren;

          // Update sets to reflect all the children.
          for (auto child: savedChildren) {
            auto& childId = child.object.getId();
            bool isNew = newChildren.insert(childId).second;
            if (oldChildren.count(childId)) {
              // We had this child before, and we still have it. Don't disown it.
              disowned.erase(childId);
            } else {
              // We didn't have this child before, so we need to adopt it if we haven't already.
              if (isNew) {
                adoptions.add(child.object, kj::mv(child.client));
              }
            }
          }
        } else {
          // We're writing fresh. All children are new.

          for (auto child: savedChildren) {
            if (newChildren.insert(child.object.getId()).second) {
              adoptions.add(child.object, kj::mv(child.client));
            }
          }
        }
      }

      // Write the new temp file.
      CurrentData newData;
      newData.fd = journal.createTempFile();

      // Write the StoredChildIds part.
      {
        capnp::MallocMessageBuilder childIdsBuilder;
        auto list = childIdsBuilder.initRoot<StoredChildIds>().initChildren(newChildren.size());
        auto array = kj::heapArrayBuilder<ObjectId>(newChildren.size());
        uint i = 0;
        for (auto& child: newChildren) {
          array.add(child);
          child.copyTo(list[i++]);
        }
        KJ_ASSERT(i == list.size());
        capnp::writeMessageToFd(newData.fd, childIdsBuilder);
        newData.storedChildIdsWords = getFilePosition(newData.fd) / sizeof(capnp::word);
        newData.children = array.finish();
      }

      // Write the StoredObject part.
      message->writeToFd(newData.fd);
      newData.storedObjectWords = getFilePosition(newData.fd) / sizeof(capnp::word) -
                                  newData.storedChildIdsWords;

      uint64_t newBlockCount = getFileBlockCount(newData.fd);

      if (state == COMMITTED) {
        // This object is already in the tree, so any other objects it adopted are now becoming
        // part of the tree. It's time to commit a transaction adding them.

        // Create the transaction.
        Journal::Transaction txn(journal);

        int64_t deltaBlocks = newBlockCount - xattr.accountedBlockCount;

        for (auto& adoption: adoptions) {
          deltaBlocks += adoption.prepCommit(id);
        }
        for (auto& adoption: adoptions) {
          adoption.commit(txn);
        }
        for (auto& disown: disowned) {
          deltaBlocks -= txn.moveToDeathRow(disown);
          factory->disowned(disown);
        }

        if (deltaBlocks < 0 && -deltaBlocks > xattr.transitiveBlockCount) {
          KJ_LOG(ERROR, "storage object had inconsistent transitive block count",
              deltaBlocks, xattr.transitiveBlockCount);
          deltaBlocks = -xattr.transitiveBlockCount;
        }

        xattr.accountedBlockCount = newBlockCount;
        xattr.transitiveBlockCount += deltaBlocks;
        txn.updateObject(id, xattr, newData.fd);

        factory->modifyTransitiveSize(xattr.owner, deltaBlocks, txn);

        // Update currentData to reflect the transaction before closing it out.
        currentData = kj::mv(newData);
        commitedSetSeqnum = seqnum;

        return txn.commit();
      } else {
        // Save the adoptions for a later transaction that actually links us into the tree.
        newData.transitiveAdoptions = adoptions.releaseAsArray();

        // We don't need to think about the objects we disowned, as all of them had to have been
        // uncommitted anyway, since we are uncommitted and an uncommited object cannot be the
        // parent of a committed object.

        // Update currentData to reflect changes.
        currentData = kj::mv(newData);
        commitedSetSeqnum = seqnum;

        // We don't bother counting child size until we're committed to disk.
        xattr.accountedBlockCount = newBlockCount;
        xattr.transitiveBlockCount = newBlockCount;

        return kj::READY_NOW;
      }

      // TODO(leak): Call drop() on all references from the old object. These don't have to
      //   succeed.
      // TODO(perf): Detect which external references from the old capability are being re-saved
      //   in the new capability to avoid a redundant save/drop pair.
    });
  }

  template <typename Context>
  void getStoredObject(Context context) {
    // Reads the object as a StoredObject and restores it, filling in `value`.

    KJ_IF_MAYBE(c, cachedValue) {
      // A set() is in progress. Return a copy of the cached value.
      auto payload = c->get()->getRoot<StoredObject>().getPayload();
      auto size = payload.targetSize();
      size.wordCount += capnp::sizeInWords<
          capnp::FromBuilder<kj::Decay<decltype(context.getResults())>>>();
      size.capCount += 1;  // for `setter`
      context.initResults(size).setValue(payload);
      return;
    }

    auto& data = KJ_ASSERT_NONNULL(currentData, "can't read from uninitialized storage object");

    KJ_SYSCALL(lseek(data.fd, data.storedChildIdsWords * sizeof(capnp::word), SEEK_SET));

    capnp::StreamFdMessageReader reader(data.fd.get());
    auto root = reader.getRoot<StoredObject>();
    capnp::ReaderCapabilityTable capTable(KJ_MAP(cap, root.getCapTable()) {
      return restoreCap(cap);
    });

    auto payload = capTable.imbue(root.getPayload());
    auto size = payload.targetSize();
    size.wordCount += capnp::sizeInWords<
        capnp::FromBuilder<kj::Decay<decltype(context.getResults())>>>();
    size.capCount += 1;  // for `setter`
    context.initResults(size).setValue(payload);
  }

  void updateSize(uint64_t blocks) {
    // Indicate that the size of the object (in blocks) is now `size`. If this is a change from the
    // accounted size, arrange to update the accounted size for this object and all parents.

    KJ_ASSERT(blocks <= uint32_t(kj::maxValue), "file too big");

    if (state == COMMITTED) {
      if (blocks != xattr.accountedBlockCount) {
        int64_t delta = blocks - xattr.accountedBlockCount;
        Journal::Transaction txn(journal);
        xattr.accountedBlockCount = blocks;
        factory->modifyTransitiveSize(id, delta, txn);
        txn.commit();
      }
    } else {
      // We don't bother counting child size until we're committed to disk.
      xattr.accountedBlockCount = blocks;
      xattr.transitiveBlockCount = blocks;
    }
  }

  kj::Promise<void> setReadOnly() {
    if (xattr.readOnly) {
      return kj::READY_NOW;
    } else if (state != COMMITTED) {
      xattr.readOnly = true;
      return kj::READY_NOW;
    } else {
      Journal::Transaction txn(journal);
      xattr.readOnly = true;
      txn.updateObjectXattr(id, xattr);
      return txn.commit();
    }
  }

  int openRaw() {
    // Directly get the underlying file descriptor. Used for types that aren't in StoredObject
    // format and do not have child capabilities.

    KJ_IF_MAYBE(d, currentData) {
      return d->fd;
    } else {
      // First time. Create new file.
      CurrentData data;
      data.fd = journal.createTempFile();
      data.storedChildIdsWords = 0;
      data.storedObjectWords = 0;
      int result = data.fd;
      currentData = kj::mv(data);
      return result;
    }
  }

  uint64_t getStorageUsageImpl() {
    return xattr.transitiveBlockCount * Volume::BLOCK_SIZE;
  }

private:
  Journal& journal;
  kj::Own<ObjectFactory> factory;
  ObjectKey key;
  ObjectId id;
  Xattr xattr;

  uint64_t nextSetSeqnum = 1;
  // Each time setStoredObject() is called, it takes a sequence number.

  uint64_t commitedSetSeqnum = 0;
  // Each time setStoredObject() is ready to commit to disk, it first checks this value to make
  // sure no future sets have already completed. If not, it updates this sequence number.

  kj::Maybe<kj::Own<RefcountedMallocMessageBuilder>> cachedValue;
  // A cached live copy (with live capabilities) of the last-set value. If non-null, this
  // corresponds to set sequence number (nextSetSeqnum - 1). getStoredObject() will return this
  // value if available so that it can be consistent with the most-recent set() even if that set()
  // hasn't hit disk yet.
  //
  // TODO(perf): We currently only keep the value cached while set() is in progress. We could keep
  //   it for some defined time after that, possibly improving performance. Alternatively, if we
  //   don't do that, we don't really need it to be refcounted because the set() task owns the
  //   other reference anyway, so it can make sure the object stays live.

  enum {
    ORPHAN,
    // Object is newly-created and not linked into anything.

    CLAIMED,
    // Object has been claimed by an owner, but that owner could still back out. The object still
    // exists only as a temporary file, not linked into storage.

    COMMITTED
    // Object has an owner and is on-disk.
  } state;

  struct CurrentData {
    kj::AutoCloseFd fd;

    // Below this point are fields which are only relevant to StoredObject format. Otherwise, they
    // are empty/zero.

    kj::Array<ObjectId> children;

    uint32_t storedChildIdsWords;
    // Size (in words) of the StoredChildIds part of the file.

    uint32_t storedObjectWords;
    // Size (in words) of the StoredObject part of the file.

    kj::Array<AdoptionIntent> transitiveAdoptions;
    // Objects which this one will adopt if this object is itself adopted.
  };

  kj::Maybe<CurrentData> currentData;
  // Null if no data has yet been written.

  struct SavedChild {
    ObjectBase& object;
    capnp::Capability::Client client;
  };

  kj::Promise<kj::Maybe<SavedChild>> saveCap(
      kj::Own<capnp::ClientHook> cap, StoredObject::CapDescriptor::Builder descriptor) {
    auto client = capnp::Capability::Client(kj::mv(cap));

    // First see if it's an OwnedStorage.
    auto promise = factory->getLiveObject(client);
    return promise.then([KJ_MVCAP(client),descriptor](
          kj::Maybe<FilesystemStorage::ObjectBase&> object) mutable
       -> kj::Promise<kj::Maybe<SavedChild>> {
      KJ_IF_MAYBE(o, object) {
        o->key.copyTo(descriptor.initChild());
        return kj::Maybe<SavedChild>(SavedChild { *o, kj::mv(client) });
      } else {
        // Not OwnedStorage. Do a regular save().
        auto req = client.castAs<StandardPersistent>().saveRequest(capnp::MessageSize {16, 0});
        req.getSealFor().setStorage();

        return req.send().then([descriptor](auto&& response) mutable {
          descriptor.setExternal(response.getSturdyRef());
          return kj::Maybe<SavedChild>(nullptr);
        });
      }
    });
  }

  kj::Maybe<kj::Own<capnp::ClientHook>> restoreCap(
      StoredObject::CapDescriptor::Reader descriptor) {
    switch (descriptor.which()) {
      case StoredObject::CapDescriptor::NONE:
        return nullptr;
      case StoredObject::CapDescriptor::CHILD: {
        kj::Own<capnp::ClientHook> result;
        KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
          auto obj = factory->openObject(descriptor.getChild());
          result = capnp::ClientHook::from(kj::mv(obj.client));
        })) {
          result = capnp::newBrokenCap(kj::mv(*exception));
        }
        return kj::mv(result);
      }
      case StoredObject::CapDescriptor::EXTERNAL: {
        auto req = factory->restoreRequest();
        req.setSturdyRef(descriptor.getExternal());
        capnp::Capability::Client cap(req.send().getCap());
        return capnp::ClientHook::from(kj::mv(cap));
      }
    }
    return capnp::newBrokenCap(KJ_EXCEPTION(FAILED, "unknown cap descriptor type on disk"));
  }
};

// =======================================================================================

class FilesystemStorage::AssignableImpl: public OwnedAssignable<>::Server, public ObjectBase {
public:
  static constexpr Type TYPE = Type::ASSIGNABLE;
  using ObjectBase::ObjectBase;

  using ObjectBase::setStoredObject;
  // Make public for Assignable so that StorageFactory can call this to initialize it.

  kj::Promise<void> getStorageUsage(GetStorageUsageContext context) override {
    context.getResults().setTotalBytes(getStorageUsageImpl());
    return kj::READY_NOW;
  }

  kj::Promise<void> get(GetContext context) override {
    context.releaseParams();
    getStoredObject(context);
    context.getResults().setSetter(kj::heap<SetterImpl>(*this, thisCap(), version));
    return kj::READY_NOW;
  }

  // TODO(soon): Implement asGetter().

  kj::Promise<void> asSetter(AsSetterContext context) override {
    context.releaseParams();
    context.getResults(capnp::MessageSize { 4, 1 })
        .setSetter(kj::heap<SetterImpl>(*this, thisCap()));
    return kj::READY_NOW;
  }

private:
  uint version = 1;

  class SetterImpl: public sandstorm::Assignable<>::Setter::Server {
  public:
    SetterImpl(AssignableImpl& object, capnp::Capability::Client client, uint expectedVersion = 0)
        : object(object), client(client), expectedVersion(expectedVersion) {}

    kj::Promise<void> set(SetContext context) override {
      if (expectedVersion > 0) {
        if (object.version != expectedVersion) {
          return KJ_EXCEPTION(DISCONNECTED, "Assignable modified concurrently");
        }
        ++expectedVersion;
      }

      context.allowCancellation();
      // If a save() call never returns we don't want this call context to be stuck here. So, only
      // keep trying for as long as the caller hasn't canceled.

      auto promise = object.setStoredObject(context.getParams().getValue());
      ++object.version;
      context.releaseParams();
      return kj::mv(promise);
    }

  private:
    AssignableImpl& object;
    capnp::Capability::Client client;  // prevent GC
    uint expectedVersion;
  };
};

constexpr FilesystemStorage::Type FilesystemStorage::AssignableImpl::TYPE;

// =======================================================================================

class FilesystemStorage::BlobImpl: public OwnedBlob::Server, public ObjectBase {
public:
  static constexpr Type TYPE = Type::BLOB;
  using ObjectBase::ObjectBase;

  void init(capnp::Data::Reader data) {
    int fd = openRaw();
    pwriteAll(fd, data.begin(), data.size(), 0);
    updateSize((data.size() + Volume::BLOCK_SIZE - 1) / Volume::BLOCK_SIZE);
    setReadOnly();
  }

  sandstorm::ByteStream::Client init() {
    openRaw();
    auto result = kj::heap<Initializer>(*this, thisCap());
    return kj::mv(result);
  }

  kj::Promise<void> getStorageUsage(GetStorageUsageContext context) override {
    context.getResults().setTotalBytes(getStorageUsageImpl());
    return kj::READY_NOW;
  }

  kj::Promise<void> getSize(GetSizeContext context) override {
    context.releaseParams();
    auto& xattr = getXattrRef();
    if (xattr.readOnly) {
      int fd = openRaw();
      context.getResults(capnp::MessageSize {4, 0}).setSize(getFileSize(fd));
      return kj::READY_NOW;
    } else KJ_IF_MAYBE(i, currentInitializer) {
      return i->getSize().then([context](size_t s) mutable {
        context.getResults(capnp::MessageSize {4, 0}).setSize(s);
      });
    } else {
      return KJ_EXCEPTION(FAILED, "blob was not fully uploaded");
    }
  }

  kj::Promise<void> writeTo(WriteToContext context) override {
    auto params = context.getParams();
    auto target = params.getStream();
    auto offset = params.getStartAtOffset();
    context.releaseParams();

    int fd = openRaw();
    uint64_t currentSize = getFileSize(fd);

    kj::Maybe<uint64_t> expectedSize;
    auto& xattr = getXattrRef();
    if (xattr.readOnly) {
      expectedSize = currentSize;
    } else KJ_IF_MAYBE(i, currentInitializer) {
      expectedSize = i->getSizeIfKnown();
    }

    KJ_IF_MAYBE(s, expectedSize) {
      // We know the expected size, so send the expected size hint.
      KJ_REQUIRE(offset <= *s, "starting offset out-of-range");

      auto req = target.expectSizeRequest();
      req.setSize(*s - offset);
      auto sizeHintPromise = req.send()
          .then([](auto&&) -> kj::Promise<void> {
        // Allow write() loop to complete.
        return kj::NEVER_DONE;
      }, [](kj::Exception&& e) -> kj::Promise<void> {
        if (e.getType() == kj::Exception::Type::UNIMPLEMENTED) {
          // Don't care if it's unimplemented. Allow write() loop to complete.
          return kj::NEVER_DONE;
        } else {
          return kj::mv(e);
        }
      });

      return sizeHintPromise.exclusiveJoin(writeLoop(offset, kj::mv(target)));
    } else {
      return writeLoop(offset, kj::mv(target));
    }
  }

private:
  class Initializer: public sandstorm::ByteStream::Server {
  public:
    Initializer(BlobImpl& object, capnp::Capability::Client client)
        : object(object), client(kj::mv(client)) {
      KJ_REQUIRE(object.currentInitializer == nullptr);
      object.currentInitializer = *this;
    }
    ~Initializer() noexcept(false) {
      object.currentInitializer = nullptr;
    }

    kj::Promise<uint64_t> getSize() {
      KJ_IF_MAYBE(s, expectedSize) {
        return *s;
      } else {
        KJ_IF_MAYBE(e, eventualSize) {
          return e->promise.addBranch();
        } else {
          return eventualSize.emplace().promise.addBranch();
        }
      }
    }

    kj::Maybe<uint64_t> getSizeIfKnown() {
      return expectedSize;
    }

    kj::Promise<void> onNextData() {
      if (isDone) return kj::READY_NOW;
      KJ_IF_MAYBE(n, nextData) {
        return n->promise.addBranch();
      } else {
        return nextData.emplace().promise.addBranch();
      }
    }

    kj::Promise<void> write(WriteContext context) override {
      KJ_REQUIRE(!isDone, "can't call write() after done()");

      auto data = context.getParams().getData();
      uint64_t newOffset = currentOffset + data.size();
      KJ_IF_MAYBE(s, expectedSize) {
        if (newOffset > *s) {
          currentOffset = *s + 1;  // don't accept any more input
          KJ_REQUIRE(currentOffset + data.size() <= *s, "written data exceeded expected size");
        }
      }

      pwriteAll(object.openRaw(), data.begin(), data.size(), currentOffset);

      // Update accounting for every megabyte uploaded.
      if ((currentOffset >> 20) != (newOffset >> 20)) {
        object.updateSize((newOffset + Volume::BLOCK_SIZE - 1) / Volume::BLOCK_SIZE);
      }

      currentOffset = newOffset;

      KJ_IF_MAYBE(n, nextData) {
        n->fulfiller->fulfill();
        nextData = nullptr;
      }

      return kj::READY_NOW;
    }

    kj::Promise<void> done(DoneContext context) override {
      KJ_REQUIRE(!isDone, "can't call done() twice");
      isDone = true;

      KJ_IF_MAYBE(e, expectedSize) {
        KJ_REQUIRE(currentOffset == *e,
            "actual bytes written to stream didn't match hint given with expectedSize()");
      } else {
        expectedSize = currentOffset;
        KJ_IF_MAYBE(e, eventualSize) {
          e->fulfiller->fulfill(kj::cp(currentOffset));
          eventualSize = nullptr;
        }
      }

      KJ_IF_MAYBE(n, nextData) {
        n->fulfiller->fulfill();
        nextData = nullptr;
      }

      int fd = object.openRaw();
      KJ_SYSCALL(fdatasync(fd));
      object.updateSize((currentOffset + Volume::BLOCK_SIZE - 1) / Volume::BLOCK_SIZE);
      return object.setReadOnly();
    }

    kj::Promise<void> expectSize(ExpectSizeContext context) override {
      KJ_REQUIRE(!isDone, "can't call expectSize() after done()");

      uint64_t newExpectedSize = context.getParams().getSize() + currentOffset;
      context.releaseParams();

      KJ_IF_MAYBE(oldExpectedSize, expectedSize) {
        KJ_ASSERT(newExpectedSize == *oldExpectedSize,
            "expectSize() called twice with mismatching inputs");
      } else {
        expectedSize = newExpectedSize;
        KJ_IF_MAYBE(e, eventualSize) {
          e->fulfiller->fulfill(kj::cp(newExpectedSize));
          eventualSize = nullptr;
        }
      }

      return kj::READY_NOW;
    }

  private:
    BlobImpl& object;
    capnp::Capability::Client client;  // prevent GC
    uint64_t currentOffset = 0;
    kj::Maybe<uint64_t> expectedSize;
    bool isDone = false;

    template <typename T>
    struct ForkedPromiseAndFulfiller {
      kj::ForkedPromise<T> promise;
      kj::Own<kj::PromiseFulfiller<T>> fulfiller;

      ForkedPromiseAndFulfiller(kj::PromiseFulfillerPair<T> paf = kj::newPromiseAndFulfiller<T>())
          : promise(paf.promise.fork()), fulfiller(kj::mv(paf.fulfiller)) {}
    };
    kj::Maybe<ForkedPromiseAndFulfiller<uint64_t>> eventualSize;  // fulfilled when size is known
    kj::Maybe<ForkedPromiseAndFulfiller<void>> nextData;  // fulfilled when data is received

    void expectAdditionalBytes(uint64_t amount) {
      uint64_t total = currentOffset + amount;
      KJ_IF_MAYBE(f, eventualSize) {
        f->fulfiller->fulfill(kj::cp(total));
        eventualSize = nullptr;
      }
      expectedSize = total;
    }
  };

  kj::Maybe<Initializer&> currentInitializer;

  kj::Promise<void> writeLoop(uint64_t offset, sandstorm::ByteStream::Client target) {
    int fd = openRaw();

    auto req = target.writeRequest(capnp::MessageSize { 2052, 0 });
    auto orphan =
        capnp::Orphanage::getForMessageContaining(sandstorm::ByteStream::WriteParams::Builder(req))
        .newOrphan<capnp::Data>(8192);
    auto buffer = orphan.get();
    ssize_t n;
    KJ_SYSCALL(n = pread(fd, buffer.begin(), buffer.size(), offset));
    if (n > 0) {
      if (n < buffer.size()) {
        orphan.truncate(n);
      }
      req.adoptData(kj::mv(orphan));
      offset += n;
      // TODO(perf): flow control / parallel writes
      return req.send().then([this,offset,KJ_MVCAP(target)](auto&&) mutable {
        return writeLoop(offset, kj::mv(target));
      });
    } else if (getXattrRef().readOnly) {
      // EOF, and file is finalized.
      return target.doneRequest().send().then([](auto&&) {});
    } else KJ_IF_MAYBE(i, currentInitializer) {
      // Still uploading. Wait for more data to be available.
      //
      // Note that we don't set up to directly copy data from the initializer capability to the
      // output stream because if the output stream backs up we don't want to buffer data
      // in-memory. Doing so could lead to DoS, etc.
      return i->onNextData().then([this,offset,KJ_MVCAP(target)]() mutable {
        return writeLoop(offset, kj::mv(target));
      });
    } else {
      // Blob is incomplete and no longer being initialized.
      return KJ_EXCEPTION(FAILED, "blob was not fully uploaded");
    }
  }
};

constexpr FilesystemStorage::Type FilesystemStorage::BlobImpl::TYPE;

// =======================================================================================

class FilesystemStorage::VolumeImpl: public OwnedVolume::Server, public ObjectBase {
public:
  static constexpr Type TYPE = Type::VOLUME;
  using ObjectBase::ObjectBase;

  void init() {
    openRaw();
  }

  kj::Promise<void> getStorageUsage(GetStorageUsageContext context) override {
    context.getResults().setTotalBytes(getStorageUsageImpl());
    return kj::READY_NOW;
  }

  kj::Promise<void> read(ReadContext context) override {
    auto params = context.getParams();
    uint64_t blockNum = params.getBlockNum();
    uint32_t count = params.getCount();
    context.releaseParams();

    KJ_REQUIRE(blockNum + count < (1ull << 32), "volume read overflow");
    KJ_REQUIRE(count < 2048, "can't read over 8MB from a volume per call");

    uint64_t offset = blockNum * Volume::BLOCK_SIZE;
    uint size = count * Volume::BLOCK_SIZE;

    auto results = context.getResults(capnp::MessageSize {16 + size / sizeof(capnp::word), 0});
    auto data = results.initData(size);

    preadAllOrZero(openRaw(), data.begin(), data.size(), offset);

    return kj::READY_NOW;
  }

  kj::Promise<void> write(WriteContext context) override {
    KJ_REQUIRE(!getXattrRef().readOnly, "attempted to write to a read-only Volume");

    if (snapshotCount > 0) {
      // Wait for snapshot destruction.
      return onZeroSnapshots.addBranch().then([this,context]() mutable {
        return write(context);
      });
    }

    auto params = context.getParams();
    uint64_t blockNum = params.getBlockNum();
    capnp::Data::Reader data = params.getData();

    uint count = data.size() / Volume::BLOCK_SIZE;
    KJ_REQUIRE(data.size() % Volume::BLOCK_SIZE == 0, "non-even number of blocks");
    KJ_REQUIRE(blockNum + count < (1ull << 32), "volume write overflow");

    uint64_t offset = blockNum * Volume::BLOCK_SIZE;

    pwriteAll(openRaw(), data.begin(), data.size(), offset);
    maybeUpdateSize(count);

    return kj::READY_NOW;
  }

  kj::Promise<void> zero(ZeroContext context) override {
    KJ_REQUIRE(!getXattrRef().readOnly, "attempted to write to a read-only Volume");

    if (snapshotCount > 0) {
      // Wait for snapshot destruction.
      return onZeroSnapshots.addBranch().then([this,context]() mutable {
        return zero(context);
      });
    }

    auto params = context.getParams();
    uint64_t blockNum = params.getBlockNum();
    uint32_t count = params.getCount();
    context.releaseParams();

    KJ_REQUIRE(blockNum + count < (1ull << 32), "volume write overflow");

    uint64_t offset = blockNum * Volume::BLOCK_SIZE;
    uint size = count * Volume::BLOCK_SIZE;

    int fd = openRaw();
    KJ_SYSCALL(fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size),
               offset, size);

    maybeUpdateSize(count);

    return kj::READY_NOW;
  }

  kj::Promise<void> sync(SyncContext context) override {
    int fd = openRaw();
    KJ_SYSCALL(fdatasync(fd));
    return kj::READY_NOW;
  }

//  kj::Promise<void> asBlob(AsBlobContext context) override {
    // TODO(someday)
//  }

  kj::Promise<void> getExclusive(GetExclusiveContext context) override {
    context.getResults(capnp::MessageSize {4, 1}).setExclusive(
        capnp::Capability::Client(kj::heap<ExclusiveWrapper>(*this)).castAs<Volume>());
    return kj::READY_NOW;
  }

  kj::Promise<void> freeze(FreezeContext context) override {
    return setReadOnly();
  }

  kj::Promise<void> pause(PauseContext context) override {
    context.getResults(capnp::MessageSize {4, 1}).setSnapshot(
        capnp::Capability::Client(kj::heap<SnapshotWrapper>(*this)).castAs<Volume>());
    return kj::READY_NOW;
  }

private:
  class ExclusiveWrapper: public capnp::Capability::Server {
  public:
    explicit ExclusiveWrapper(VolumeImpl& inner)
        : inner(inner), innerCap(inner.thisCap()),
          exclusiveNumber(++inner.currentExclusiveNumber) {
      // All pause() snapshots are disabled by a new getExclusive().
      if (inner.snapshotCount > 0) {
        inner.snapshotCount = 0;
        inner.onZeroSnapshotsFulfiller->fulfill();
      }
    }

    capnp::Capability::Server::DispatchCallResult dispatchCall(
        uint64_t interfaceId, uint16_t methodId,
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
      if (inner.currentExclusiveNumber != exclusiveNumber) {
        return { KJ_EXCEPTION(DISCONNECTED, "Volume revoked due to concurrent write"), false };
      }

      if (interfaceId != capnp::typeId<Volume>()) {
        return { KJ_EXCEPTION(UNIMPLEMENTED, "actual interface: blackrock::Volume",
                              interfaceId, methodId), false };
      }

      return inner.dispatchCall(interfaceId, methodId, context);
    }

  private:
    VolumeImpl& inner;
    capnp::Capability::Client innerCap;  // prevent gc
    uint32_t exclusiveNumber;
  };

  class SnapshotWrapper: public Volume::Server {
  public:
    explicit SnapshotWrapper(VolumeImpl& inner)
        : inner(inner), innerCap(inner.thisCap()),
          exclusiveNumber(inner.currentExclusiveNumber) {
      if (inner.snapshotCount++ == 0) {
        auto paf = kj::newPromiseAndFulfiller<void>();
        inner.onZeroSnapshots = paf.promise.fork();
        inner.onZeroSnapshotsFulfiller = kj::mv(paf.fulfiller);
      }
    }

    ~SnapshotWrapper() noexcept(false) {
      if (inner.currentExclusiveNumber == exclusiveNumber) {
        KJ_ASSERT(inner.snapshotCount > 0) { return; }
        if (--inner.snapshotCount == 0) {
          inner.onZeroSnapshotsFulfiller->fulfill();
        }
      }
    }

    kj::Promise<void> read(ReadContext context) override {
      if (inner.currentExclusiveNumber != exclusiveNumber) {
        return KJ_EXCEPTION(DISCONNECTED,
            "snapshot Volume revoked due to concurrent getExclusive()");
      }

      return inner.read(context);
    }

  private:
    VolumeImpl& inner;
    capnp::Capability::Client innerCap;  // prevent gc
    uint32_t exclusiveNumber;
  };

  uint32_t counter = 0;
  uint32_t currentExclusiveNumber = 0;
  uint32_t snapshotCount = 0;
  kj::ForkedPromise<void> onZeroSnapshots = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> onZeroSnapshotsFulfiller;

  void maybeUpdateSize(uint32_t count) {
    // Periodically update our accounting of the volume size. Called every time some blocks are
    // modified. `count` is the number of blocks modified. We don't bother updating accounting for
    // every single block write, since that would be inefficient.

    counter += count;
    if (counter > 128) {
      updateSize(getFileBlockCount(openRaw()));
      counter = 0;
    }
  }
};

constexpr FilesystemStorage::Type FilesystemStorage::VolumeImpl::TYPE;

// =======================================================================================

class FilesystemStorage::StorageFactoryImpl: public StorageFactory::Server {
public:
  StorageFactoryImpl(ObjectFactory& factory, capnp::Capability::Client storage)
      : factory(factory), storage(kj::mv(storage)) {}

  kj::Promise<void> newBlob(NewBlobContext context) override {
    auto result = factory.newObject<BlobImpl>();
    result.object.init(context.getParams().getContent());
    context.getResults(capnp::MessageSize { 4, 1 }).setBlob(kj::mv(result.client));
    return kj::READY_NOW;
  }

  kj::Promise<void> uploadBlob(UploadBlobContext context) override {
    auto factoryResult = factory.newObject<BlobImpl>();
    auto results = context.getResults(capnp::MessageSize { 4, 2 });
    results.setBlob(kj::mv(factoryResult.client));
    results.setStream(factoryResult.object.init());
    return kj::READY_NOW;
  }

  kj::Promise<void> newVolume(NewVolumeContext context) override {
    auto result = factory.newObject<VolumeImpl>();
    result.object.init();
    context.getResults(capnp::MessageSize { 4, 1 }).setVolume(kj::mv(result.client));
    return kj::READY_NOW;
  }

  kj::Promise<void> newAssignable(NewAssignableContext context) override {
    auto result = factory.newObject<AssignableImpl>();
    auto promise = result.object.setStoredObject(context.getParams().getInitialValue());
    context.getResults(capnp::MessageSize { 4, 1 }).setAssignable(kj::mv(result.client));
    return kj::mv(promise);
  }

private:
  ObjectFactory& factory;
  capnp::Capability::Client storage;  // ensures storage is not destroyed while factory exists
};

// =======================================================================================
// finish implementing ObjectFactory

FilesystemStorage::ObjectFactory::ObjectFactory(Journal& journal, kj::Timer& timer,
                                                Restorer<SturdyRef>::Client&& restorer)
    : journal(journal), timer(timer), restorer(kj::mv(restorer)) {}

template <typename T>
auto FilesystemStorage::ObjectFactory::newObject() -> ClientObjectPair<typename T::Serves, T> {
  return registerObject<T>(kj::heap<T>(journal, kj::addRef(*this), T::TYPE));
}

auto FilesystemStorage::ObjectFactory::openObject(ObjectKey key)
    -> ClientObjectPair<capnp::Capability, ObjectBase> {
  ObjectId id = key;
  auto iter = objectCache.find(id);
  if (iter != objectCache.end()) {
    ObjectBase& object = *iter->second;
    return { object.self(), object };
  }

  // Not in cache. Create it.
  Xattr xattr;
  auto fd = KJ_ASSERT_NONNULL(journal.openObject(id, xattr), "object not found");

  switch (xattr.type) {
#define HANDLE_TYPE(tag, type) \
    case Type::tag: \
      return registerObject(kj::heap<type>(journal, kj::addRef(*this), key, id, xattr, kj::mv(fd)))
    HANDLE_TYPE(BLOB, BlobImpl);
    HANDLE_TYPE(VOLUME, VolumeImpl);
//    HANDLE_TYPE(IMMUTABLE, ImmutableImpl);
    HANDLE_TYPE(ASSIGNABLE, AssignableImpl);
//    HANDLE_TYPE(COLLECTION, CollectionImpl);
//    HANDLE_TYPE(OPAQUE, OpaqueImpl);
#undef HANDLE_TYPE
//    case Type::REFERENCE:
    default:
      KJ_UNIMPLEMENTED("unimplemented storage object type seen on disk", (uint)xattr.type);
  }
}

auto FilesystemStorage::ObjectFactory::getLiveObject(capnp::Capability::Client& client)
    -> kj::Promise<kj::Maybe<FilesystemStorage::ObjectBase&>> {
  return serverSet.getLocalServer(client).then([](auto&& maybeServer) {
    return maybeServer.map([](auto& server) -> ObjectBase& {
      return dynamic_cast<ObjectBase&>(server);
    });
  });
}

void FilesystemStorage::ObjectFactory::destroyed(ObjectBase& object) {
  objectCache.erase(object.getId());
}

void FilesystemStorage::ObjectFactory::modifyTransitiveSize(
    ObjectId id, int64_t deltaBlocks, Journal::Transaction& txn) {
  if (id == nullptr) {
    // Root. Skip.
    return;
  }

  Xattr scratchXattr;
  Xattr* xattr;

  auto iter = objectCache.find(id);
  if (iter == objectCache.end()) {
    // Object not loaded. Edit directly.
    if (journal.openObject(id, scratchXattr) == nullptr) {
      // Apparently the object has been deleted. There's no use trying to modify it or its parents.
      return;
    }
    xattr = &scratchXattr;
  } else {
    xattr = &iter->second->getXattrRef();
  }

  if (deltaBlocks < 0 && -deltaBlocks > xattr->transitiveBlockCount) {
    KJ_LOG(ERROR, "storage object had inconsistent transitive block count",
        deltaBlocks, xattr->transitiveBlockCount);
    deltaBlocks = -xattr->transitiveBlockCount;
  }

  xattr->transitiveBlockCount += deltaBlocks;
  txn.updateObjectXattr(id, *xattr);

  modifyTransitiveSize(xattr->owner, deltaBlocks, txn);
}

void FilesystemStorage::ObjectFactory::disowned(ObjectId id) {
  auto iter = objectCache.find(id);
  if (iter != objectCache.end()) {
    iter->second->getXattrRef().owner = nullptr;
  }
}

template <typename T>
auto FilesystemStorage::ObjectFactory::registerObject(kj::Own<T> object)
    -> ClientObjectPair<typename T::Serves, T> {
  T& ref = *object;

  ObjectBase& base = ref;
  KJ_ASSERT(objectCache.insert(std::make_pair(base.getId(), &base)).second, "duplicate object");

  return { serverSet.add(kj::mv(object)).template castAs<typename T::Serves>(), ref };
}

// =======================================================================================

static kj::AutoCloseFd openOrCreateDirectory(int parentFd, kj::StringPtr name) {
  mkdirat(parentFd, name.cStr(), 0777);
  auto f = sandstorm::raiiOpenAt(parentFd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  return f;
}

FilesystemStorage::FilesystemStorage(
    int directoryFd, kj::UnixEventPort& eventPort, kj::Timer& timer,
    Restorer<SturdyRef>::Client&& restorer)
    : mainDirFd(openOrCreateDirectory(directoryFd, "main")),
      stagingDirFd(openOrCreateDirectory(directoryFd, "staging")),
      deathRowFd(openOrCreateDirectory(directoryFd, "death-row")),
      rootsFd(openOrCreateDirectory(directoryFd, "roots")),
      deathRow(kj::heap<DeathRow>(*this)),
      journal(kj::heap<Journal>(*this, eventPort,
          sandstorm::raiiOpenAt(directoryFd, "journal", O_RDWR | O_CREAT | O_CLOEXEC))),
      factory(kj::refcounted<ObjectFactory>(*journal, timer, kj::mv(restorer))) {}

FilesystemStorage::~FilesystemStorage() noexcept(false) {}

kj::Promise<void> FilesystemStorage::set(SetContext context) {
  auto params = context.getParams();
  auto object = params.getObject();
  auto name = kj::heapString(params.getName());
  context.releaseParams();
  return setImpl(kj::mv(name), kj::mv(object));
}

kj::Promise<void> FilesystemStorage::setImpl(kj::String name, OwnedStorage<>::Client object) {
  KJ_ASSERT(name.size() > 0, "invalid storage root name", name);
  KJ_ASSERT(('a' <= name[0] && name[0] <= 'z') ||
            ('A' <= name[0] && name[0] <= 'Z') ||
            ('0' <= name[0] && name[0] <= '9') ||
             name[0] == '_', "invalid storage root name", name);

  for (char c: name) {
    KJ_ASSERT(c > ' ' && c <= '~' && c != '/', "invalid storage root name", name);
  }

  auto promise = factory->getLiveObject(object);
  return promise
      .then([this,KJ_MVCAP(object),KJ_MVCAP(name)](kj::Maybe<ObjectBase&>&& unwrapped) mutable {
    ObjectBase& base = KJ_ASSERT_NONNULL(unwrapped,
        "tried to set non-OwnedStorage object as storage root");
    ObjectBase::AdoptionIntent adoption(base, kj::mv(object));

    capnp::MallocMessageBuilder rootMessage(64);
    base.getKey().copyTo(rootMessage.getRoot<StoredRoot>().initKey());
    capnp::writeMessageToFd(
        sandstorm::raiiOpenAt(rootsFd, name, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC),
        rootMessage);

    Journal::Transaction txn(*journal);
    adoption.prepCommit(nullptr);
    adoption.commit(txn);
    return txn.commit();
  });
}

kj::Promise<void> FilesystemStorage::get(GetContext context) {
  capnp::StreamFdMessageReader message(sandstorm::raiiOpenAt(
      rootsFd, context.getParams().getName(), O_RDONLY | O_CLOEXEC));
  ObjectKey key(message.getRoot<StoredRoot>().getKey());
  context.getResults().setObject(factory->openObject(key).client.castAs<OwnedStorage<>>());
  return kj::READY_NOW;
}

kj::Promise<void> FilesystemStorage::tryGet(TryGetContext context) {
  KJ_IF_MAYBE(fd, sandstorm::raiiOpenAtIfExists(
      rootsFd, context.getParams().getName(), O_RDONLY | O_CLOEXEC)) {
    capnp::StreamFdMessageReader message(kj::mv(*fd));
    ObjectKey key(message.getRoot<StoredRoot>().getKey());
    context.getResults().setObject(factory->openObject(key).client.castAs<OwnedStorage<>>());
  }
  return kj::READY_NOW;
}

kj::Promise<void> FilesystemStorage::getOrCreateAssignable(GetOrCreateAssignableContext context) {
  auto params = context.getParams();
  KJ_IF_MAYBE(fd, sandstorm::raiiOpenAtIfExists(
      rootsFd, params.getName(), O_RDONLY | O_CLOEXEC)) {
    capnp::StreamFdMessageReader message(kj::mv(*fd));
    ObjectKey key(message.getRoot<StoredRoot>().getKey());
    context.getResults().setObject(factory->openObject(key).client.castAs<OwnedAssignable<>>());
    return kj::READY_NOW;
  } else {
    auto name = kj::heapString(params.getName());
    auto result = factory->newObject<AssignableImpl>();
    auto object = result.client.castAs<OwnedStorage<>>();
    context.getResults(capnp::MessageSize {4, 1}).setObject(kj::mv(result.client));
    return result.object.setStoredObject(params.getDefaultValue())
        .then([this,KJ_MVCAP(name),KJ_MVCAP(object)]() mutable {
      return setImpl(kj::mv(name), kj::mv(object));
    });
  }
}

kj::Promise<void> FilesystemStorage::remove(RemoveContext context) {
  kj::StringPtr name = context.getParams().getName();
  KJ_IF_MAYBE(file, sandstorm::raiiOpenAtIfExists(
      rootsFd, name, O_RDONLY | O_CLOEXEC)) {
    capnp::StreamFdMessageReader message(kj::mv(*file));
    ObjectKey key(message.getRoot<StoredRoot>().getKey());
    Journal::Transaction txn(*journal);
    txn.moveToDeathRow(key);
    return txn.commit().then([this,name]() {
      while (unlinkat(rootsFd, name.cStr(), 0) < 0) {
        int error = errno;
        if (error == ENOENT) {
          // fine
          break;
        } else if (error != EINTR) {
          KJ_FAIL_SYSCALL("unlinkat(roots, name)", errno, name);
        }
      }
    });
  }
  return kj::READY_NOW;
}

kj::Promise<void> FilesystemStorage::getFactory(GetFactoryContext context) {
  context.getResults().setFactory(kj::heap<StorageFactoryImpl>(*factory, thisCap()));
  return kj::READY_NOW;
}

kj::Maybe<kj::AutoCloseFd> FilesystemStorage::openObject(ObjectId id) {
  return sandstorm::raiiOpenAtIfExists(mainDirFd, id.filename('o').begin(), O_RDWR | O_CLOEXEC);
}

kj::Maybe<kj::AutoCloseFd> FilesystemStorage::openStaging(uint64_t number) {
  auto name = hex64(number);
  return sandstorm::raiiOpenAtIfExists(stagingDirFd, fixedStr(name), O_RDWR | O_CLOEXEC);
}

kj::AutoCloseFd FilesystemStorage::createObject(ObjectId id) {
  return sandstorm::raiiOpenAt(mainDirFd, id.filename('o').begin(),
                               O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC);
}

kj::AutoCloseFd FilesystemStorage::createTempFile() {
  return sandstorm::raiiOpenAt(mainDirFd, ".", O_RDWR | O_TMPFILE | O_CLOEXEC);
}

void FilesystemStorage::linkTempIntoStaging(uint64_t number, int fd, const Xattr& xattr) {
  KJ_SYSCALL(fsetxattr(fd, Xattr::NAME, &xattr, sizeof(xattr), 0));
  KJ_SYSCALL(linkat(AT_FDCWD, kj::str("/proc/self/fd/", fd).cStr(), stagingDirFd,
                    hex64(number).begin(), AT_SYMLINK_FOLLOW));
}

void FilesystemStorage::deleteStaging(uint64_t number) {
  KJ_SYSCALL(unlinkat(stagingDirFd, hex64(number).begin(), 0));
}

void FilesystemStorage::deleteAllStaging() {
  for (auto& file: sandstorm::listDirectoryFd(stagingDirFd)) {
    KJ_SYSCALL(unlinkat(stagingDirFd, file.cStr(), 0));
  }
}

void FilesystemStorage::createFromStagingIfExists(
    uint64_t stagingId, ObjectId finalId, const Xattr& attributes) {
  auto stagingName = hex64(stagingId);
  auto finalName = finalId.filename('o');

  // Verify that file doesn't already exist in main.
  //
  // TODO(cleanup): Once we can assume kernel 3.15, we can do this atomically with renameat2().
  if (faccessat(mainDirFd, finalName.begin(), F_OK, 0) == 0) {
    // Hmm, the target file already exists. This is OK *if* the source file doesn't exist, which
    // indicates that we're replaying a transaction that already happened.
    KJ_ASSERT(faccessat(stagingDirFd, stagingName.begin(), F_OK, 0) != 0,
        "can't create storage object: an object with that ID already exists",
        stagingName.begin(), finalName.begin());
    return;
  }

  if (attributes.owner != nullptr) {
    // Verify that owner exists. If the owner was moved directly to death row, then we need to
    // move directly to death row as well, because the death row thread may have already deleted
    // the owner and failed to find this child.
    auto ownerName = attributes.owner.filename('o');
  retryAccess:
    if (faccessat(mainDirFd, ownerName.begin(), F_OK, 0) != 0) {
      int error = errno;
      if (error == EINTR) {
        goto retryAccess;
      } else if (error == ENOENT) {
        // Owner no longer exists, so we should just delete. Note that any children of this object
        // which we attempt to create later will find that this object doesn't exist and therefore
        // will delete themselves as well, so there's no need to move to death row.
      retryUnlink:
        if (unlinkat(stagingDirFd, stagingName.begin(), 0) != 0) {
          int error = errno;
          if (error == EINTR) {
            goto retryUnlink;
          } else if (error == ENOENT) {
            // acceptable; file already deleted by someone else
          } else {
            KJ_FAIL_SYSCALL("unlinkat(stagingDirFd, stagingName)", error, stagingName);
          }
        }
        return;
      } else {
        KJ_FAIL_SYSCALL("faccessat", error, ownerName.begin());
      }
    }
  }

retry:
  if (renameat(stagingDirFd, stagingName.begin(), mainDirFd, finalName.begin()) < 0) {
    int error = errno;
    switch (error) {
      case EINTR:
        goto retry;
      case ENOENT:
        // Acceptable;
        break;
      default:
        KJ_FAIL_SYSCALL("renameat(staging -> final)", error,
                        fixedStr(stagingName), fixedStr(finalName));
    }
  }
}

void FilesystemStorage::replaceFromStagingIfExists(
    uint64_t stagingId, ObjectId finalId, const Xattr& attributes) {
  auto stagingName = hex64(stagingId);
  auto finalName = finalId.filename('o');

  // First check that the old file still exists, since we're updating. If it doesn't, it was
  // probably deleted, and the new copy should also be immediately deleted.
  //
  // Note that since all modifications are done by the journal thread we can assume no race between
  // faccessat() and renameat(), but if races were possible we could use renameat2() (new feature
  // in Linux 3.15).
retryAccess:
  if (faccessat(mainDirFd, finalName.begin(), F_OK, 0) != 0) {
    int error = errno;
    if (error == EINTR) {
      goto retryAccess;
    } else if (error == ENOENT) {
      // Old file no longer exists. Delete the replacement immediately. No need for death row since
      // no new children can be created while the parent doesn't exist anyway.
    retryUnlink:
      if (unlinkat(stagingDirFd, stagingName.begin(), 0) != 0) {
        int error = errno;
        if (error == EINTR) {
          goto retryUnlink;
        } else if (error == ENOENT) {
          // acceptable; file already deleted by someone else
        } else {
          KJ_FAIL_SYSCALL("unlinkat(stagingDirFd, stagingName)", error, stagingName);
        }
      }
      return;
    } else {
      KJ_FAIL_SYSCALL("faccessat", error, finalName.begin());
    }
  }

retry:
  if (renameat(stagingDirFd, stagingName.begin(), mainDirFd, finalName.begin()) < 0) {
    int error = errno;
    switch (error) {
      case EINTR:
        goto retry;
      case ENOENT:
        // Acceptable;
        break;
      default:
        KJ_FAIL_SYSCALL("renameat(staging -> final)", error,
                        fixedStr(stagingName), fixedStr(finalName));
    }
  }
}

void FilesystemStorage::setAttributesIfExists(ObjectId objectId, const Xattr& attributes) {
  // Sadly, there is no setxattrat(). But we can use /proc/self/fd to emulate it.
  auto name = objectId.filename('o');
  auto hackname = kj::str("/proc/self/fd/", mainDirFd, "/", fixedStr(name));
retry:
  if (setxattr(hackname.cStr(), Xattr::NAME, &attributes, sizeof(attributes), 0) < 0) {
    int error = errno;
    switch (error) {
      case EINTR:
        goto retry;
      case ENOENT:
        // Acceptable;
        break;
      default:
        KJ_FAIL_SYSCALL("setxattr(name, ...)", error, fixedStr(name));
    }
  }
}

void FilesystemStorage::moveToDeathRowIfExists(ObjectId id, bool notify) {
  auto name = id.filename('o');

retry:
  if (renameat(mainDirFd, name.begin(), deathRowFd, name.begin()) == 0) {
    if (notify) deathRow->notifyNewInmates();
  } else {
    int error = errno;
    switch (error) {
      case EINTR:
        goto retry;
      case ENOENT:
        // Acceptable;
        break;
      default:
        KJ_FAIL_SYSCALL("renameat(move to death row)", error, fixedStr(name));
    }
  }
}

void FilesystemStorage::sync() {
  static bool noSyncfs = false;

retry:
  if (noSyncfs) {
    ::sync();  // apparently does not return errors
  } else if (syncfs(mainDirFd) < 0) {
    int error = errno;
    if (error == EINTR) {
      goto retry;
    } else if (error == ENOSYS) {
      // syncfs() is not implemented under Valgrind. Fall back to sync().
      noSyncfs = true;
      KJ_LOG(WARNING, "syncfs() syscall not implemented, falling back to sync(); "
                      "if you're valgrinding, ignore this, otherwise, please investigate");
      goto retry;
    } else {
      KJ_FAIL_SYSCALL("syncfs", error);
    }
  }
}

bool FilesystemStorage::isStoredObjectType(Type type) {
  switch (type) {
    case Type::BLOB:
    case Type::VOLUME:
      return false;

    case Type::IMMUTABLE:
    case Type::ASSIGNABLE:
    case Type::COLLECTION:
    case Type::OPAQUE:
      return true;

    case Type::REFERENCE:
      return false;
  }

  KJ_FAIL_ASSERT("unknown object type on disk", (uint)type);
}

}  // namespace blackrock
