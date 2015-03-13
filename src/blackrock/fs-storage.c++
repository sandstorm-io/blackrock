// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

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

namespace blackrock {

namespace {

void readAll(int fd, void* data, size_t size);
void writeAll(int fd, const void* data, size_t size);
void preadAll(int fd, void* data, size_t size, off_t offset);
void pwriteAll(int fd, const void* data, size_t size, off_t offset);

size_t getFileSize(int fd) {
  struct stat stats;
  KJ_SYSCALL(fstat(fd, &stats));
  return stats.st_size;
}

kj::AutoCloseFd newEventFd(uint value, int flags) {
  int fd;
  KJ_SYSCALL(fd = eventfd(0, flags));
  return kj::AutoCloseFd(fd);
}

static constexpr uint64_t EVENTFD_MAX = (uint64_t)-2;

}  // namespace

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
  BLOB,
  MUTABLE_BLOB,
  IMMUTABLE,
  ASSIGNABLE,
  COLLECTION,
  VOLUME,
  ZONE,
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

  unsigned refcount :24;

  uint32_t reserved;
  // Must be zero.

  uint64_t accountedSize;
  // The size (in bytes) of this object the last time it was accounted into the zone's size.
  // The actual on-disk size may have changed in the meantime.

  ObjectId zone;
  // The zone in which this object lives. 0 = this is the root zone, which should never be deleted.
};

class FilesystemStorage::Journal {
  struct Entry;
public:
  explicit Journal(FilesystemStorage& storage, kj::UnixEventPort& unixEventPort,
                   kj::AutoCloseFd journalFd)
      : storage(storage),
        journalFd(kj::mv(journalFd)),
        journalEnd(getFileSize(this->journalFd)),
        journalSynced(journalEnd),
        journalExecuted(journalEnd),
        journalReadyEventFd(newEventFd(0, EFD_CLOEXEC)),
        journalProcessedEventFd(newEventFd(0, EFD_CLOEXEC | EFD_NONBLOCK)),
        journalProcessedEventFdObserver(unixEventPort, journalProcessedEventFd,
            kj::UnixEventPort::FdObserver::OBSERVE_READ),
        processingThread([this]() { doProcessingThread(); }) {
    doRecovery();
  }

  ~Journal() noexcept(false) {
    // Write the maximum possible value to the eventfd. This write will actually block until the
    // eventfd reaches 0, which is nice because it means the processing thread will be able to
    // receive the previous event and process it before it receives this one, resulting in clean
    // shutdown.
    uint64_t stop = EVENTFD_MAX;
    writeAll(journalReadyEventFd, &stop, sizeof(stop));

    // Now the destructor of the thread will wait for the thread to exit.
  }

  kj::Maybe<kj::AutoCloseFd> openObject(ObjectId id, Xattr& xattr) {
    // Obtain a file descriptor and current attributes for the given object, as if all transactions
    // had already completed. `xattr` is filled in with the attributes.

    auto iter = cache.find(id);
    if (iter == cache.end()) {
      auto result = storage.openObject(id);
      KJ_IF_MAYBE(r, result) {
        memset(&xattr, 0, sizeof(xattr));
        KJ_SYSCALL(fgetxattr(*r, Xattr::NAME, &xattr, sizeof(xattr)));
      }
      return result;
    } else {
      xattr = iter->second.xattr;
      if (iter->second.stagingId != 0) {
        KJ_IF_MAYBE(fd, storage.openStaging(iter->second.stagingId)) {
          return kj::mv(*fd);
        }
      }
      return KJ_ASSERT_NONNULL(storage.openObject(id),
          "object is in cache but file not found on disk?");
    }
  }

  void deleteObject(ObjectId id) {
    // Immediately delete the given object from disk. The object must already have had its refcount
    // reduced to zero.

    auto iter = cache.find(id);
    if (iter != cache.end()) {
      KJ_REQUIRE(iter->second.xattr.refcount == 0,
          "request to delete object which still has references");
      if (iter->second.stagingId != 0) {
        // If the staging file still exists, then there is an unfulfilled journal entry moving it
        // into place. But, if the journal playback doesn't find the file, it will safely ignore
        // that operation. Meanwhile, normal journal playback never deletes files -- only recovery
        // playback does, when at the end of recovery some file has zero refcount. So, we really
        // do need to delete the staging file now, otherwise it will never be deleted. We also
        // need to make sure to do this *before* deleting the main object file to avoid any race
        // condition with the journal playback thread.
        storage.deleteStaging(iter->second.stagingId);
      }
      cache.erase(iter);
    }

    storage.deleteObject(id);
  }

  class Transaction {
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

    void updateObject(ObjectId id, const Xattr& attributes, int tmpFd) {
      // Replace the object on disk with the file referenced by `tmpFd` with the given attributes.

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Link temp file into staging.
      uint32_t stagingId = journal.nextStagingId++;
      if (journal.nextStagingId > MAX_STAGING_ID) {
        journal.nextStagingId = 1;
      }
      journal.storage.linkTempIntoStaging(stagingId, tmpFd);

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.stagingId = stagingId;
      entry.objectId = id;
      entry.newXattr = attributes;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
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
      entry.objectId = id;
      entry.newXattr = attributes;

      // Update cache.
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.stagingId = 0;
      cache.xattr = attributes;
      journal.cacheDropQueue.push({cache.lastUpdate, entry.objectId});
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
      uint64_t entryCount = entries.size();
      writeAll(journal.journalReadyEventFd, &entryCount, sizeof(entryCount));

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
  };

private:
  struct Entry {
    // In order to implement atomic transactions, we organize disk changes into a stream of
    // idempotent modifications. Each change is appended to the journal before being actually
    // performed, so that on system failure we can replay the journal to get up-to-date.

    uint32_t txSize;
    // Number of entries remaining in this transaction, including this one. Do not start applying
    // operations unless the full transaction is available and all `txSize` values are correct.
    // If recovering from a failure, ignore an incomplete transaction at the end of the journal.

    unsigned type :4;
    // Transaction type. Currently must always be zero.

    unsigned stagingId :28;
    // If non-zero, identifies a staging file which should be rename()ed to replace this object.
    // The xattrs should be written to the file just before it is renamed. If no such staging file
    // exists, this operation probably already occurred; ignore it.
    //
    // If zero, then an existing file should be modified in-place. If there is no existing file
    // matching the ID, we are probably replaying an operation that was already completed, and some
    // later operation probably deletes this object; ignore the op.

    ObjectId objectId;
    // ID of the object to update.

    Xattr newXattr;
    // Updated Xattr structure to write into the file.
    //
    // If `refcount` is becoming zero, and does not come back up from zero before the journal
    // has been fully played back, the file should be deleted.

    uint64_t reserved;
    // Must be zero. Reserved for possible future expansion of Xattr.
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
    uint64_t lastUpdate;
    // Offset in the journal of the last update to this object. We may discard the cache entry
    // once the journal has been committed past this point.

    uint32_t stagingId;
    // ID of staging file which is the object's current content. If this file no longer exists,
    // then it has been moved to the file's final location.

    Xattr xattr;
    // Attributes as of the last update.
  };
  std::unordered_map<ObjectId, CacheEntry, ObjectId::Hash> cache;
  // Cache of attribute changes that are in the journal but haven't been written to disk yet.

  static constexpr uint32_t MAX_STAGING_ID = (1 << 28) - 1;
  uint32_t nextStagingId = 1;
  // Counter to use to generate staging file names. The names are 7-digit zero-padded hex. The
  // counter wraps around at 2^28, but skips the value zero since it is used to indicate
  // "no staging".

  bool txInProgress = false;
  // True if a `Transaction` exists which has not been committed.

  struct SyncQueueEntry {
    uint64_t offset;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
  };
  std::queue<SyncQueueEntry> syncQueue;

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
    off_t position;
    KJ_SYSCALL(position = lseek(journalFd, 0, SEEK_DATA));

    if (journalEnd > position) {
      // Recover from previous journal failure.

      // Read all entries.
      auto entries = kj::heapArray<Entry>((journalEnd - position) / sizeof(Entry));
      preadAll(journalFd, entries.begin(), entries.asBytes().size(), position);

      // Process valid entries and discard any incomplete transaction.
      for (auto& entry: validateEntries(entries, true)) {
        executeEntry(entry);
      }
    }
  }

  void doProcessingThread() {
    // This thread reads the journal, makes sure things are synced to disk, and actually executes
    // the transactions.

    // Get the current position from journalSynced rather than journalEnd since journalEnd could
    // possibly have changed already, but journalSynced can't change until we signal back to the
    // main thread.
    uint64_t position = journalSynced;

    for (;;) {
      // Wait for some data to read.
      uint64_t count;
      readAll(journalReadyEventFd, &count, sizeof(count));

      KJ_ASSERT(count > 0);

      if (count == EVENTFD_MAX) {
        // Clean shutdown requested.
        break;
      }

      // Read the entries.
      auto entries = kj::heapArray<Entry>(count);
      preadAll(journalFd, entries.begin(), entries.asBytes().size(), position);

      // Make sure this data is synced.
      KJ_SYSCALL(fdatasync(journalFd));

      // Post back to main thread that sync is finished through these bytes.
      uint64_t byteCount = entries.asBytes().size();
      writeAll(journalProcessedEventFd, &byteCount, sizeof(byteCount));

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
        KJ_SYSCALL(fallocate(journalFd, FALLOC_FL_PUNCH_HOLE, holeStart, holeEnd - holeStart));
      }
    }

    // On clean shutdown, the journal is empty and we can discard it all.
    KJ_ASSERT(getFileSize(journalFd) == position, "journal not empty after clean shutdown");
    KJ_SYSCALL(ftruncate(journalFd, 0));
  }

  kj::ArrayPtr<const Entry> validateEntries(
      kj::ArrayPtr<const Entry> entries, bool discardIncompleteTrailing) {
    uint64_t expected = 0;
    const Entry* end;
    for (auto& entry: entries) {
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

      KJ_ASSERT(entry.type == 0 && entry.reserved == 0,
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
    if (entry.stagingId != 0) {
      storage.finalizeStagingIfExists(entry.stagingId, entry.objectId, entry.newXattr);
    } else {
      storage.setAttributesIfExists(entry.objectId, entry.newXattr);
    }
  }
};

// =======================================================================================

class FilesystemStorage::ObjectBase {
  // Base class which all persistent storage objects implement. Often accessed by first
  // unwrapping a Capability::Client into a native object and then doing dynamic_cast.

public:
  ObjectBase(Journal& journal, ObjectId zone, Type type);
  // Create a new object. A key will be generated.

  ObjectBase(Journal& journal, ObjectId zone, const ObjectKey& key,
             const ObjectId& id, const Xattr& xattr);
  // Construct an ObjectBase around an existing on-disk object.

  bool isInZone(ObjectId zone);
  // Returns true if this object's parent zone is `zone`.

  ObjectKey addReference(Journal::Transaction& transaction);
  // Add a new reference to this object (from a sibling in the same zone).

  void dropReference(Journal::Transaction& transaction);
  // Add an operation to the transaction which decrements this object's refcount. If it reaches
  // zero, recursively reduces the refcount on all children as well, but holds live refs to them
  // for possible later revival.

protected:
  kj::Promise<void> setStoredObject(capnp::AnyPointer::Reader value);
  // Overwrites the object with the given value saved as a StoredObject.

  kj::Promise<void> getStoredObject(capnp::AnyPointer::Builder value);
  // Reads the object as a StoredObject and restores it, filling in `value`.

  typedef capnp::Persistent<SturdyRef, SturdyRef::Owner> StandardPersistent;
  typedef capnp::CallContext<StandardPersistent::SaveParams, StandardPersistent::SaveResults>
       StandardSaveContext;

  kj::Promise<void> saveImpl(StandardSaveContext context);
  // Implements the save() RPC method. Creates a new ref, and adds the ref to this object's
  // backreferences.

  void updateSize(uint64_t size);
  // Indicate that the size of the object (in bytes) is now `size`. If this is a change from the
  // accounted size, arrange to update the accounted size for this object and all parent zones.

  int openCurrent(bool readonly);
  // Open the file representing the object's current content. Returns a file descriptor. All calls
  // made to `openCurrent()` on a particular object must have the same parameter value.

  kj::AutoCloseFd openReplacement(int flags);
  // Open a new temporary file which will later be passed to replace() to replace the existing
  // content.

  void replace(kj::AutoCloseFd fd, uint64_t newSize, kj::ArrayPtr<ObjectId> oldRefs,
               kj::ArrayPtr<capnp::Capability> newRefs);
  // Replace the object's content with an FD created using openReplacement().

  uint64_t getAccountedSize();
  // Get the accounted size of the object which, if the subclass is careful, may be consistent with
  // the current size.

  kj::Maybe<kj::Own<capnp::ClientHook>> restoreRef(StoredObject::CapDescriptor::Reader reader);
  kj::Promise<void> saveRef(StoredObject::Builder builder, capnp::Capability::Client);

private:
  Journal& journal;
  ObjectId zone;
  ObjectKey key;
  ObjectId id;
  Xattr xattr;
  kj::AutoCloseFd fd;

  kj::Array<capnp::Capability::Client> tenuousRefs;
  // If this object's refcount has reached zero, `tenuousRefs` is a list of live capabilities
  // representing all outgoing strong references form this object, to prevent them from going away
  // in the meantime.
};

// =======================================================================================

class FilesystemStorage::AssignableImpl: public PersistentAssignable<>::Server, public ObjectBase {
public:
  using ObjectBase::ObjectBase;

  using ObjectBase::getStoredObject;  // make public for Assignable
  using ObjectBase::setStoredObject;  // make public for Assignable

  kj::Promise<void> get(GetContext context) override {
    context.releaseParams();

    int fd = openCurrent(true);
    auto words = kj::heapArray<capnp::word>(getAccountedSize() / sizeof(capnp::word));
    auto bytes = words.asBytes();

    off_t offset = 0;
    while (bytes.size() > 0) {
      ssize_t n = 0;
      KJ_SYSCALL(n = pread(fd, bytes.begin(), bytes.size(), offset));
      KJ_ASSERT(n > 0, "file on disk shorter than expected?");
      bytes = bytes.slice(n, bytes.size());
      offset += n;
    }

    capnp::FlatArrayMessageReader reader(words);
    auto root = reader.getRoot<StoredObject>();
    reader.initCapTable(KJ_MAP(cap, root.getCapTable()) {
      return restoreRef(cap);
    });

    auto payload = root.getPayload();
    auto size = payload.targetSize();
    size.wordCount += capnp::sizeInWords<GetResults>();
    context.initResults(size).setValue(payload);
    return kj::READY_NOW;
  }

  kj::Promise<void> save(SaveContext context) override {
    return saveImpl(context);
  }
};

// =======================================================================================

class FilesystemStorage::VolumeImpl: public PersistentVolume::Server, public ObjectBase {
public:
  using ObjectBase::ObjectBase;

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

    int fd = openCurrent(false);
    while (data.size() > 0) {
      ssize_t n;
      KJ_SYSCALL(n = pread(fd, data.begin(), data.size(), offset));
      data = data.slice(n, data.size());
      offset += n;
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> write(WriteContext context) override {
    auto params = context.getParams();
    uint64_t blockNum = params.getBlockNum();
    capnp::Data::Reader data = params.getData();

    uint count = data.size() / Volume::BLOCK_SIZE;
    KJ_REQUIRE(data.size() % Volume::BLOCK_SIZE == 0, "non-even number of blocks");
    KJ_REQUIRE(blockNum + count < (1ull << 32), "volume write overflow");

    uint64_t offset = blockNum * Volume::BLOCK_SIZE;

    int fd = openCurrent(false);
    while (data.size() > 0) {
      ssize_t n;
      KJ_SYSCALL(n = pwrite(fd, data.begin(), data.size(), offset));
      data = data.slice(n, data.size());
      offset += n;
    }

    maybeUpdateSize(count);

    return kj::READY_NOW;
  }

  kj::Promise<void> zero(ZeroContext context) override {
    auto params = context.getParams();
    uint64_t blockNum = params.getBlockNum();
    uint32_t count = params.getCount();
    context.releaseParams();

    KJ_REQUIRE(blockNum + count < (1ull << 32), "volume write overflow");
    KJ_REQUIRE(count < 2048, "can't read over 8MB from a volume per call");

    uint64_t offset = blockNum * Volume::BLOCK_SIZE;
    uint size = count * Volume::BLOCK_SIZE;

    int fd = openCurrent(false);
    KJ_SYSCALL(fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size));

    maybeUpdateSize(count);

    return kj::READY_NOW;
  }

  kj::Promise<void> sync(SyncContext context) override {
    int fd = openCurrent(false);
    KJ_SYSCALL(fdatasync(fd));
    return kj::READY_NOW;
  }

  kj::Promise<void> getBlockCount(GetBlockCountContext context) override {
    struct stat stats;
    int fd = openCurrent(false);
    KJ_SYSCALL(fstat(fd, &stats));
    uint64_t size = stats.st_blocks * 512u;
    context.getResults().setCount(size / Volume::BLOCK_SIZE);
    updateSize(size);
    return kj::READY_NOW;
  }

  kj::Promise<void> watchBlockCount(WatchBlockCountContext context) override {
    KJ_UNIMPLEMENTED("Volume.watchBlockCount()");
  }

private:
  uint32_t counter = 0;

  void maybeUpdateSize(uint32_t count) {
    // Periodically update our accounting of the volume size. Called every time some blocks are
    // modified. `count` is the number of blocks modified. We don't bother updating accounting for
    // every single block write, since that would be inefficient.

    counter += count;
    if (counter > 128) {
      struct stat stats;
      KJ_SYSCALL(fstat(openCurrent(false), &stats));
      uint64_t size = stats.st_blocks * 512u;
      updateSize(size);
      counter = 0;
    }
  }
};

// =======================================================================================

class FilesystemStorage::StorageFactoryImpl: public StorageFactory::Server {
public:
  explicit StorageFactoryImpl(Journal& journal, ObjectId zone)
      : journal(journal), zone(zone) {}

#error "TODO: if the same object is opened twice, return the same cap"

  kj::Promise<void> newAssignable(NewAssignableContext context) override {
    auto result = kj::heap<AssignableImpl>(journal, zone, Type::VOLUME);
    result->setStoredObject(context.getParams().getInitialValue());
    context.getResults().setAssignable(kj::mv(result));
    return kj::READY_NOW;
  }

  kj::Promise<void> newVolume(NewVolumeContext context) override {
    context.getResults().setVolume(kj::heap<VolumeImpl>(journal, zone, Type::VOLUME));
    return kj::READY_NOW;
  }

private:
  Journal& journal;
  ObjectId zone;
};

}  // namespace blackrock
