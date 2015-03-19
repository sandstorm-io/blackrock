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
#include <unordered_map>
#include <unordered_set>
#include <capnp/persistent.capnp.h>

namespace blackrock {

namespace {

void readAll(int fd, void* data, size_t size);
void writeAll(int fd, const void* data, size_t size);
void preadAll(int fd, void* data, size_t size, off_t offset);
void pwriteAll(int fd, const void* data, size_t size, off_t offset);

uint64_t getFileSize(int fd) {
  struct stat stats;
  KJ_SYSCALL(fstat(fd, &stats));
  return stats.st_size;
}

uint64_t getFilePosition(int fd) {
  off_t offset;
  KJ_SYSCALL(offset = lseek(fd, 0, SEEK_CUR));
  return offset;
}

kj::AutoCloseFd newEventFd(uint value, int flags) {
  int fd;
  KJ_SYSCALL(fd = eventfd(0, flags));
  return kj::AutoCloseFd(fd);
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

  return result;
}

static constexpr uint64_t EVENTFD_MAX = (uint64_t)-2;

typedef capnp::Persistent<SturdyRef, SturdyRef::Owner> StandardPersistent;
typedef capnp::CallContext<StandardPersistent::SaveParams, StandardPersistent::SaveResults>
     StandardSaveContext;

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

  byte reserved[3];
  // Must be zero.

  uint32_t accountedBlockCount;
  // The number of 4k blocks consumed by this object the last time we considered it for
  // accounting/quota purposes. The on-disk size could have changed in the meantime.

  uint64_t transitiveBlockCount;
  // The number of 4k blocks in this object and all child objects.

  ObjectId owner;
  // What object owns this one?
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

  kj::AutoCloseFd openObject(ObjectId id, Xattr& xattr) {
    // Obtain a file descriptor and current attributes for the given object, as if all transactions
    // had already completed. `xattr` is filled in with the attributes.

    auto iter = cache.find(id);
    if (iter == cache.end()) {
      auto result = KJ_ASSERT_NONNULL(storage.openObject(id), "object not found");
      memset(&xattr, 0, sizeof(xattr));
      KJ_SYSCALL(fgetxattr(result, Xattr::NAME, &xattr, sizeof(xattr)));
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

  kj::AutoCloseFd createTempFile() {
    return storage.createTempFile();
  }

  template <typename T>
  typename T::Serves::Client registerObject(kj::Own<T> object);
  void unregisterObject(ObjectBase& object);
  kj::Maybe<ObjectBase&> getLiveObject(ObjectId id);
  kj::Promise<kj::Maybe<ObjectBase&>> getLiveObject(capnp::Capability::Client& client);
  // Interfaces for use only by StorageFactoryImpl and RestorerImpl.

  struct ClientObjectPair {
    capnp::Capability::Client client;
    ObjectBase* object;
  };

  ClientObjectPair restoreObject(ObjectKey key);

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
      uint64_t stagingId = journal.nextStagingId++;
      journal.storage.linkTempIntoStaging(stagingId, tmpFd);

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

    void moveToDeathRow(ObjectId id) {
      // Delete an object (recursively, if it has children).

      KJ_REQUIRE(journal.txInProgress, "transaction already committed");

      // Add the operation to the transaction.
      entries.add();
      Entry& entry = entries.back();
      memset(&entry, 0, sizeof(entry));
      entry.type = Entry::Type::MOVE_TO_DEATH_ROW;
      entry.objectId = id;

      // Update cache.
      journal.cache.erase(id);
      CacheEntry& cache = journal.cache[id];
      cache.lastUpdate = journal.journalEnd + entries.size() * sizeof(Entry);
      cache.location = CacheEntry::Location::DELETED;
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

    enum class Type :uint8_t {
      UPDATE_OBJECT,
      // Replace the object with a staging file identified by `stagingId`, first setting the xattrs
      // on the staging file, then rename()ing it into place. If no such staging file exists, this
      // operation probably already occurred; ignore it.

      UPDATE_XATTR,
      // Update the xattrs on an existing file.

      MOVE_TO_DEATH_ROW
      // Move this object's file from main storage to death row.
    };

    Type type;
    // Transaction type.

    byte reserved[3];

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
  };

  static_assert(sizeof(Entry) == 64,
      "journal entry size changed; please keep power-of-two and consider migration issues");
  // We want the entry size to be a power of two so that they are page-aligned.

  FilesystemStorage& storage;

  capnp::CapabilityServerSet<capnp::Capability> serverSet;
  // Lets us map our own capabilities -- when they come back from the caller -- back to the
  // underlying objects.

  std::unordered_map<ObjectId, ObjectBase*, ObjectId::Hash> objectCache;
  // Maps object IDs to live objects representing them, if any.

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
      NORMAL,
      // The file -- if it exists -- is in its normal location.

      STAGING,
      // The file *may* still be in staging, under `stagingId`. If `stagingId` no longer exists,
      // then the file is actually now live.

      DELETED
      // The file has been deleted, but may still be present on disk.
    };

    Location location = Location::NORMAL;

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

    storage.deleteAllStaging();
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
      case Entry::Type::UPDATE_OBJECT:
        storage.finalizeStagingIfExists(entry.stagingId, entry.objectId, entry.xattr);
        break;
      case Entry::Type::UPDATE_XATTR:
        storage.setAttributesIfExists(entry.objectId, entry.xattr);
        break;
      case Entry::Type::MOVE_TO_DEATH_ROW:
        storage.moveToDeathRow(entry.objectId);
        break;
    }
  }
};

// =======================================================================================

class FilesystemStorage::ObjectBase {
  // Base class which all persistent storage objects implement. Often accessed by first
  // unwrapping a Capability::Client into a native object and then doing dynamic_cast.

public:
  ObjectBase(Journal& journal, Type type)
      : journal(journal), key(ObjectKey::generate()), id(key), state(ORPHAN) {
    // Create a new object. A key will be generated.

    memset(&xattr, 0, sizeof(xattr));
    xattr.type = type;
  }

  ObjectBase(Journal& journal, const ObjectKey& key, const ObjectId& id, const Xattr& xattr,
             kj::AutoCloseFd fd)
      : journal(journal), key(key), id(id), xattr(xattr), state(COMMITTED) {
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
  }

  ~ObjectBase() noexcept(false) {
    journal.unregisterObject(*this);

    // Note: If the object hasn't been committed yet, then our FD is an unlinked temp file and
    // closing it will delete the data from disk, so we don't have to worry about it here. If the
    // file has been linked to disk, then either it's in staging as part of a not-yet-committed
    // transaction, or it's all the way in main storage already.
  }

  inline const ObjectId& getId() const { return id; }

  inline capnp::Capability::Client getClient() {
    return KJ_ASSERT_NONNULL(weak->get());
  }

  inline void setWeak(kj::Own<capnp::WeakCapability<capnp::Capability>> weak) {
    KJ_IREQUIRE(this->weak.get() == nullptr);
    this->weak = kj::mv(weak);
  }

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

    void commit(ObjectId owner, Journal::Transaction& transaction) {
      // Add an operation to the transaction which officially adopts this object.

      KJ_ASSERT(!committed);
      committed = true;
      object.state = COMMITTED;

      object.xattr.owner = owner;

      auto& data = KJ_ASSERT_NONNULL(object.currentData);

      // Currently, only adopting of newly-created objects is allowed, so we know data.fd is a
      // temp file, and we should call updateObject() here. Later, when we support ownership
      // transfers, this may not be true.
      transaction.updateObject(object.id, object.xattr, data.fd);

      for (auto& adoption: data.transitiveAdoptions) {
        adoption.commit(object.id, transaction);
      }
      data.transitiveAdoptions = nullptr;
    }

  private:
    ObjectBase& object;
    capnp::Capability::Client cap;  // Make sure `object` can't be deleted.
    bool committed;
  };

protected:
  kj::Promise<void> setStoredObject(capnp::AnyPointer::Reader value) {
    // Overwrites the object with the given value saved as a StoredObject.

    KJ_ASSERT(value.targetSize().wordCount < (1u << 17),
        "Stored Cap'n Proto objects must be less than 1MB. Use Volume or Blob for bulk data.");

    // Start constructing the message to write to disk, copying over the input.
    auto message = kj::heap<capnp::MallocMessageBuilder>();
    auto root = message->getRoot<StoredObject>();
    root.getPayload().set(value);

    // Arrange to save each capability ot the cap table.
    auto capTableIn = message->getCapTable();
    auto capTableOut = root.initCapTable(capTableIn.size());
    auto promises = kj::heapArrayBuilder<kj::Promise<kj::Maybe<SavedChild>>>(capTableIn.size());
    for (auto i: kj::indices(capTableIn)) {
      KJ_IF_MAYBE(cap, capTableIn[i]) {
        promises.add(saveCap(kj::mv(*cap), capTableOut[i]));
      } else {
        promises.add(kj::READY_NOW);
      }
    }

    #error "TODO: don't re-save external refs that haven't changed? drop the ones that have?"

    // Wait for all the saves to complete.
    return kj::joinPromises(promises.finish())
        .then([KJ_MVCAP(message),this](kj::Array<kj::Maybe<SavedChild>> results) mutable
                                    -> kj::Promise<void> {
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
              // We had this child before, so don't disown it.
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
      capnp::writeMessageToFd(newData.fd, *message);
      newData.storedObjectWords = getFilePosition(newData.fd) / sizeof(capnp::word) -
                                  newData.storedChildIdsWords;

      if (state == COMMITTED) {
        // This object is already in the tree, so any other objects it adopted are now becoming
        // part of the tree. It's time to commit a transaction adding them.

        // Create the transaction.
        Journal::Transaction txn(journal);

        txn.updateObject(id, xattr, newData.fd);
        for (auto& adoption: adoptions) {
          adoption.commit(id, txn);
        }
        for (auto& disown: disowned) {
          txn.moveToDeathRow(disown);
        }

        // Update currentData to reflect the transaction before closing it out.
        currentData = kj::mv(newData);

        return txn.commit();
      } else {
        // Save the adoptions for a later transaction that actually links us into the tree.
        newData.transitiveAdoptions = adoptions.releaseAsArray();

        // We don't need to think about the objects we disowned, as all of them had to have been
        // uncommitted anyway, since we are uncommitted and an uncommited object cannot be the
        // parent of a committed object.

        // Update currentData to reflect changes.
        currentData = kj::mv(newData);

        return kj::READY_NOW;
      }
    });
  }

  template <typename Context>
  void getStoredObject(Context context) {
    // Reads the object as a StoredObject and restores it, filling in `value`.

    auto& data = KJ_ASSERT_NONNULL(currentData, "can't read from uninitialized storage object");

    KJ_SYSCALL(lseek(data.fd, data.storedChildIdsWords * sizeof(capnp::word), SEEK_SET));

    capnp::StreamFdMessageReader reader(data.fd.get());
    auto root = reader.getRoot<StoredObject>();
    reader.initCapTable(KJ_MAP(cap, root.getCapTable()) {
      return restoreCap(cap);
    });

    auto payload = root.getPayload();
    auto size = payload.targetSize();
    size.wordCount += capnp::sizeInWords<kj::Decay<decltype(context.getResults())>>();
    size.capCount += 1;  // for `setter`
    context.initResults(size).setValue(payload);
  }

  void updateSize(uint64_t size) {
    // Indicate that the size of the object (in bytes) is now `size`. If this is a change from the
    // accounted size, arrange to update the accounted size for this object and all parent zones.

    uint64_t blocks = size / Volume::BLOCK_SIZE;
    KJ_ASSERT(blocks <= uint32_t(kj::maxValue), "file too big");

    if (blocks != xattr.accountedBlockCount) {
      #error "TODO: update all parent sizes"
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

  capnp::Capability::Client self() {
    return KJ_ASSERT_NONNULL(weak->get());
  }

private:
  Journal& journal;
  ObjectKey key;
  ObjectId id;
  Xattr xattr;
  kj::Own<capnp::WeakCapability<capnp::Capability>> weak;

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
    auto promise = journal.getLiveObject(client);
    return promise.then([KJ_MVCAP(client),descriptor](
          kj::Maybe<FilesystemStorage::ObjectBase&> object) mutable
       -> kj::Promise<kj::Maybe<SavedChild>> {
      KJ_IF_MAYBE(o, object) {
        o->key.copyTo(descriptor.getChild());
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

  kj::Maybe<kj::Own<capnp::ClientHook>> restoreCap(StoredObject::CapDescriptor::Reader descriptor);
};

template <typename T>
typename T::Serves::Client FilesystemStorage::Journal::registerObject(kj::Own<T> object) {
  ObjectBase& base = *object;
  KJ_ASSERT(objectCache.insert(std::make_pair(base.getId(), &base)).second, "duplicate object");

  auto clientAndWeak = serverSet.addWeak(kj::mv(object));
  base.setWeak(kj::mv(clientAndWeak.weak));
  return kj::mv(clientAndWeak.client).template castAs<typename T::Serves>();
}

void FilesystemStorage::Journal::unregisterObject(ObjectBase& object) {
  objectCache.erase(object.getId());
}

kj::Maybe<FilesystemStorage::ObjectBase&> FilesystemStorage::Journal::getLiveObject(ObjectId id) {
  auto iter = objectCache.find(id);
  if (iter == objectCache.end()) {
    return nullptr;
  } else {
    return *iter->second;
  }
}

kj::Promise<kj::Maybe<FilesystemStorage::ObjectBase&>> FilesystemStorage::Journal::getLiveObject(
    capnp::Capability::Client& client) {
  return serverSet.getLocalServer(client).then([](auto&& maybeServer) {
    return maybeServer.map([](auto& server) -> ObjectBase& {
      return dynamic_cast<ObjectBase&>(server);
    });
  });
}

// =======================================================================================

class FilesystemStorage::AssignableImpl: public OwnedAssignable<>::Server, public ObjectBase {
public:
  using ObjectBase::ObjectBase;

  using ObjectBase::setStoredObject;
  // Make public for Assignable so that StorageFactory can call this to initialize it.

  kj::Promise<void> get(GetContext context) override {
    #error "we should probably wait for the set queue to be consistent... but what about DoS?"
    context.releaseParams();
    getStoredObject(context);
    context.getResults().setSetter(kj::heap<SetterImpl>(*this, self(), version));
  }

private:
  uint version = 1;

  kj::Promise<void> setQueue = kj::READY_NOW;
  // For now, we serialize set() calls. Arguably, it would be reasonable to cancel unfinised set()s
  // when a new one comes in, but that could leave dangling (un-drop()ed) refs.
  //
  // TODO(perf): We could allow concurrent set()s, but require that they complete in the order they
  //   were made. That is, `set()` has no need to wait for the previous `set()` to complete before
  //   it starts calling `save()` on its refs -- it only needs to wait before committing the final
  //   result.

  class SetterImpl: public sandstorm::Assignable<>::Setter::Server {
  public:
    SetterImpl(AssignableImpl& object, capnp::Capability::Client client, uint expectedVersion = 0)
        : object(object), client(client), expectedVersion(expectedVersion) {}

    kj::Promise<void> set(SetContext context) override {
      auto fork = object.setQueue.then([this,context]() mutable -> kj::Promise<void> {
        if (expectedVersion > 0) {
          if (object.version != expectedVersion) {
            return KJ_EXCEPTION(DISCONNECTED, "Assignable modified concurrently");
          }
          ++expectedVersion;
        }

        auto promise = object.setStoredObject(context.getParams().getValue());
        ++object.version;
        context.releaseParams();
        return kj::mv(promise);
      }).fork();
      object.setQueue = fork.addBranch().catch_([](auto&& e) {});
      return fork.addBranch();
    }

  private:
    AssignableImpl& object;
    capnp::Capability::Client client;  // prevent GC
    uint expectedVersion;
  };
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

  kj::Promise<void> newAssignable(NewAssignableContext context) override {
    auto result = kj::heap<AssignableImpl>(journal, zone, Type::VOLUME);
    result->setStoredObject(context.getParams().getInitialValue());
    context.getResults().setAssignable(journal.registerObject(kj::mv(result)));
    return kj::READY_NOW;
  }

  kj::Promise<void> newVolume(NewVolumeContext context) override {
    context.getResults().setVolume(journal.registerObject(
        kj::heap<VolumeImpl>(journal, zone, Type::VOLUME)));
    return kj::READY_NOW;
  }

private:
  Journal& journal;
  ObjectId zone;
};

}  // namespace blackrock
