// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_JOURNAL_LAYER_H_
#define BLACKROCK_STORAGE_JOURNAL_LAYER_H_

#include "basics.h"
#include "blob-layer.h"
#include <unordered_map>
#include <map>

namespace blackrock {
namespace storage {

class JournalLayer {
  // The journal layer supports the same basic operations as the blob layer, but also supports
  // combining write operations into transactions.

public:
  ~JournalLayer() noexcept(false);

  class RecoverableTemporary;
  class Object;
  class Transaction;
  class Recovery;

  kj::Promise<kj::Maybe<kj::Own<Object>>> openObject(ObjectId id);
  // Like BlobLayer::openObject(). As with that function, it is an error to openObject() the same
  // ID a second time without having dropped the first reference.

  kj::Own<BlobLayer::Temporary> newDetachedTemporary();
  // Like BlobLayer::newTemporary(). At the journal layer, we have different types for detached
  // temporaries (which have no ID assigned) and "recoverable" temporaries (which have a recovery
  // ID assigned).

  class Transaction {
  public:
    explicit Transaction(JournalLayer& journal);
    ~Transaction() noexcept(false);

    kj::Own<BlobLayer::Object> wrap(Object& object);
    kj::Own<BlobLayer::Temporary> wrap(RecoverableTemporary& content);
    // Wraps the given object in a wrapper whose write methods do not apply immediately but rather
    // are added to the transaction.
    //
    // Not all operations can be performed transactionally. In particular, direct reads and writes
    // on object contents are not transactional. Therefore, attempts to call getContent() on a
    // wrapped Object or use methods of Content on a wrapped Temporary will throw exceptions.
    //
    // Note that wrapping an object makes it part of the transaction even if none of its methods
    // are called, meaning that other transactions attempting to operate on the same object will
    // be considered to conflict with this transaction.

    kj::Own<Object> createObject(
        ObjectId id, Xattr xattr, kj::Own<BlobLayer::Temporary> content);
    // Create a new object as part of the transaction. The given `content` must NOT have a
    // recovery ID already set.

    kj::Own<RecoverableTemporary> createRecoverableTemporary(
        RecoveryId id, TemporaryXattr xattr, kj::Own<BlobLayer::Temporary> content);
    // Create a new recoverable temporary as part of the transaction. The given `content` must NOT
    // have a recovery ID already set.

    kj::Promise<void> commit(
        kj::Maybe<kj::Own<RecoverableTemporary>> tempToConsume = nullptr);
    // Commit the transaction to the journal. Once the promise resolves, it is guaranteed that
    // the transaction will occur in full. If a machine failure occurs before the promise resolves,
    // then the transaction may or may not have been committed, but either it will take effect in
    // full or not at all.
    //
    // `commit()` never throws an exception, because callers need to be assured that if they get
    // to the point of calling commit(), it will succeed. If something goes wrong durring
    // `commit()`, the entire process will be aborted.
    //
    // If `tempToConsume` is provided, it is a temporary file that should be consumed (deleted) as
    // part of this transaction. Typically this is a temporary whose presence indicates the need
    // to perform the transaction in the first place, especially backburner tasks.

    kj::Promise<void> saveInto(BlobLayer::Content& content);
    // Serialize this transaction into the given `content`.

  private:
    class LockedObject;
    class LockedTemporary;

    JournalLayer& journal;
    kj::Vector<kj::Own<LockedObject>> objects;
    kj::Vector<kj::Own<LockedTemporary>> temporaries;
  };

  class Object: private kj::Refcounted {
    // Like BlobLayer::Object, but read-only; to modify, use a transaction.

  public:
    ~Object() noexcept(false);

    Xattr getXattr() { return cachedXattr; }
    uint64_t getGeneration() { return generation; }
    BlobLayer::Content& getContent();

  private:
    JournalLayer& journal;
    ObjectId id;
    kj::Own<BlobLayer::Object> inner;
    uint64_t generation = 0;

    Xattr cachedXattr;
    kj::Maybe<BlobLayer::Content&> cachedContent;
    bool locked = false;

    Object(JournalLayer& journal, ObjectId id, kj::Own<BlobLayer::Object>&& innerParam);
    Object(JournalLayer& journal, ObjectId id, const Xattr& xattr, BlobLayer::Content& content);

    void update(Xattr newXattr, kj::Maybe<BlobLayer::Content&> newContent, uint changeCount);

    friend class Transaction;
    template <typename T, typename... Params>
    friend kj::Own<T> kj::refcounted(Params&&... params);
    template <typename T>
    friend kj::Own<T> kj::addRef(T& object);
    friend class kj::Refcounted;
  };

  class RecoverableTemporary: private kj::Refcounted {
    // Like BlobLayer::Temporary, but read-only; to modify, use a transaction.
    //
    // This type is *only* used to represent recoverable temporaries that already have a
    // RecoveryId assigned.

  public:
    ~RecoverableTemporary() noexcept(false);

    TemporaryXattr getXattr() { return cachedXattr; }
    uint64_t getGeneration() { return generation; }
    BlobLayer::Content& getContent();

  private:
    JournalLayer& journal;
    RecoveryId id;
    kj::Own<BlobLayer::Temporary> inner;
    uint64_t generation = 0;

    TemporaryXattr cachedXattr;
    kj::Maybe<BlobLayer::Content&> cachedContent;
    bool locked = false;

    RecoverableTemporary(JournalLayer& journal, RecoveryId id,
                         kj::Own<BlobLayer::Temporary>&& inner);
    RecoverableTemporary(JournalLayer& journal, RecoveryId id,
                         const TemporaryXattr& xattr, BlobLayer::Content& content);

    void update(TemporaryXattr newXattr, kj::Maybe<BlobLayer::Content&> newContent,
                uint changeCount);

    friend class Transaction;
    template <typename T, typename... Params>
    friend kj::Own<T> kj::refcounted(Params&&... params);
    template <typename T>
    friend kj::Own<T> kj::addRef(T& object);
    friend class kj::Refcounted;
  };

  class RecoveredTemporary {
    // Like BlobLayer::RecoveredTemporary.

  public:
    const RecoveryId& getOldId() { return oldId; }
    const TemporaryXattr& getTemporaryXattr() { return xattr; }
    BlobLayer::Content& getContent() { return inner->getContent(); }
    kj::Own<RecoverableTemporary> keepAs(RecoveryId newId);

    void setXattr(TemporaryXattr xattr);
    void overwrite(TemporaryXattr xattr, kj::Own<BlobLayer::RecoveredTemporary> replacement);

  private:
    JournalLayer& journal;
    RecoveryId oldId;
    TemporaryXattr xattr;
    kj::Own<BlobLayer::RecoveredTemporary> inner;

    RecoveredTemporary(JournalLayer& journal, kj::Own<BlobLayer::RecoveredTemporary> inner);
    RecoveredTemporary(JournalLayer& journal, RecoveryId oldId, TemporaryXattr xattr,
       kj::Own<BlobLayer::RecoveredTemporary> inner);

    friend class JournalLayer::Recovery;
    template <typename T, typename... Params>
    friend kj::Own<T> kj::heap(Params&&... params);
  };

private:
  BlobLayer* blobLayer;
  kj::Own<BlobLayer::Temporary> journalFile;
  uint64_t journalPosition = 0;
  uint64_t stagingIdCounter = 1;
  kj::Promise<void> writeQueue = nullptr;
  std::unordered_map<ObjectId, Object*, ObjectId::Hash> openObjects;
};

class JournalLayer::Recovery: private JournalLayer {
public:
  explicit Recovery(BlobLayer::Recovery& blobLayer);

  kj::Maybe<kj::Own<Object>> getObject(ObjectId id);
  // Get an object for recovery purposes. Returns null if it doesn't exist.

  kj::Array<kj::Own<RecoveredTemporary>> recoverTemporaries(RecoveryType type);
  // Like BlobLayer::Recovery::recoverTemporaries(), but reflects the state of the temporaries
  // after the journal has been replayed.

  void commitSavedTransaction(BlobLayer::Content& content);
  // Read a transaction from the given `content` and commit it now.

  JournalLayer& finish();
  // Finish recovery and transition into normal operation. No other methods on Recovery may be
  // called after this point.

private:
  BlobLayer::Recovery& blobLayerRecovery;
  std::map<uint64_t, kj::Own<BlobLayer::RecoveredTemporary>> recoveredStaging;
  std::map<RecoveryId, kj::Own<JournalLayer::RecoveredTemporary>> recoveredTemporaries;
  bool finished = false;

  void replayEntry(BlobLayer::Recovery& recovery, const JournalEntry& entry);
};

}  // namespace storage
}  // namespace blackrock

#endif // BLACKROCK_STORAGE_JOURNAL_LAYER_H_
