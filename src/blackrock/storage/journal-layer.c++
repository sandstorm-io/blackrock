// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "journal-layer.h"
#include <kj/debug.h>
#include <unistd.h>
#include <kj/function.h>
#include <sandstorm/util.h>

namespace blackrock {
namespace storage {

JournalLayer::Object::Object(
    JournalLayer& journal, ObjectId id, kj::Own<BlobLayer::Object>&& innerParam)
    : journal(journal), id(id), inner(kj::mv(innerParam)) {
  KJ_REQUIRE(journal.openObjects.insert(std::make_pair(id, this)).second);
}
JournalLayer::Object::~Object() noexcept(false) {
  journal.openObjects.erase(id);
}

JournalLayer::RecoverableTemporary::RecoverableTemporary(
    JournalLayer& journal, RecoveryId id, kj::Own<BlobLayer::Temporary>&& inner)
    : journal(journal), id(id), inner(kj::mv(inner)) {}
JournalLayer::RecoverableTemporary::~RecoverableTemporary() noexcept(false) {}

// =======================================================================================

class JournalLayer::Transaction::FutureObject final: public BlobLayer::Object {
public:
  void overwrite(Xattr xattr, kj::Own<BlobLayer::Temporary> content) override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }

  Xattr getXattr() override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }

  void setXattr(Xattr xattr) override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }

  void remove() override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }

  uint64_t getGeneration() override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }

  BlobLayer::Content& getContent() override {
    KJ_FAIL_REQUIRE("object does not exist yet");
  }
};

class JournalLayer::Transaction::FutureTemporary final: public BlobLayer::Temporary {
public:
  void setRecoveryId(RecoveryId id) override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }

  void setRecoveryId(RecoveryId id, TemporaryXattr xattr) override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }

  void overwrite(TemporaryXattr xattr, kj::Own<Temporary> replacement) override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }

  TemporaryXattr getXattr() override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }

  void setXattr(TemporaryXattr xattr) override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }

  BlobLayer::Content& getContent() override {
    KJ_FAIL_REQUIRE("temporary does not exist yet");
  }
};

// =======================================================================================

class JournalLayer::Transaction::LockedObject final
    : public BlobLayer::Object, public kj::Refcounted {
public:
  explicit LockedObject(kj::Own<JournalLayer::Object> objectParam)
      : object(kj::mv(objectParam)), changeCount(0), created(false) {
    if (object->locked) {
      kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "transaction aborted due to conflict"));
    }
    object->locked = true;
  }

  LockedObject(kj::Own<JournalLayer::Object> objectParam, Xattr initialXattr,
               kj::Own<BlobLayer::Temporary> initialContent)
      : object(kj::mv(objectParam)), changeCount(1), created(true),
        newXattr(initialXattr), newContent(kj::mv(initialContent)) {
    if (object->locked) {
      kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "transaction aborted due to conflict"));
    }
    object->locked = true;
  }

  ~LockedObject() noexcept(false) {
    object->locked = false;
  }

  kj::Maybe<JournalEntry> getJournalEntry(uint64_t stagingId) {
    // Write a journal entry for this object's changes. If a temporary needs to be staged, do so
    // and assign it `stagingId`.

    if (changeCount == 0 || (created && removed)) return nullptr;

    JournalEntry entry;
    memset(&entry, 0, sizeof(entry));

    KJ_IF_MAYBE(c, newContent) {
      c->get()->setRecoveryId(RecoveryId(RecoveryType::STAGING, stagingId));
      entry.stagingId = stagingId;
    }

    entry.object.id = object->id;
    KJ_IF_MAYBE(x, newXattr) {
      entry.object.xattr = *x;
    } else {
      KJ_ASSERT(removed);
    }

    if (created) {
      entry.type = JournalEntry::Type::CREATE_OBJECT;
    } else if (removed) {
      entry.type = JournalEntry::Type::DELETE_OBJECT;
    } else if (newContent == nullptr) {
      entry.type = JournalEntry::Type::UPDATE_XATTR;
    } else {
      entry.type = JournalEntry::Type::UPDATE_OBJECT;
    }

    return entry;
  }

  void execute(BlobLayer& blobLayer) {
    // Execute the changes made to this object, storing (but not necessarily syncing) them to disk.
    // This is called after the journal entry has been synced to disk.
    //
    // No other methods of LockedObject will be called after commit().

    if (created) {
      object->inner = blobLayer.createObject(object->id, KJ_ASSERT_NONNULL(newXattr),
          kj::mv(KJ_ASSERT_NONNULL(newContent)));
    } else KJ_IF_MAYBE(c, newContent) {
      object->inner->overwrite(KJ_ASSERT_NONNULL(newXattr), kj::mv(*c));
    } else KJ_IF_MAYBE(x, newXattr) {
      object->inner->setXattr(*x);
    }

    if (removed) {
      object->inner->remove();
    }

    object->changeCount += changeCount;
  }

  void overwrite(Xattr xattr, kj::Own<BlobLayer::Temporary> content) override {
    ++changeCount;
    newXattr = xattr;
    newContent = kj::mv(content);
  }

  Xattr getXattr() override {
    KJ_IF_MAYBE(x, newXattr) {
      return *x;
    } else {
      return object->getXattr();
    }
  }

  void setXattr(Xattr xattr) override {
    ++changeCount;
    newXattr = xattr;
  }

  void remove() override {
    ++changeCount;
    removed = true;
  }

  uint64_t getGeneration() override {
    return object->getGeneration() + changeCount;
  }

  BlobLayer::Content& getContent() override {
    KJ_IF_MAYBE(c, newContent) {
      return c->get()->getContent();
    } else {
      return object->getContent();
    }
  }

private:
  kj::Own<JournalLayer::Object> object;

  uint changeCount;
  bool created;
  bool removed = false;
  kj::Maybe<Xattr> newXattr;
  kj::Maybe<kj::Own<BlobLayer::Temporary>> newContent;
};

class JournalLayer::Transaction::LockedTemporary final
    : public BlobLayer::Temporary, public kj::Refcounted {
public:
  explicit LockedTemporary(kj::Own<JournalLayer::RecoverableTemporary> objectParam)
      : object(kj::mv(objectParam)), changeCount(0), created(false) {
    if (object->locked) {
      kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "transaction aborted due to conflict"));
    }
    object->locked = true;
  }

  LockedTemporary(kj::Own<JournalLayer::RecoverableTemporary> objectParam,
                  TemporaryXattr initialXattr,
                  kj::Own<BlobLayer::Temporary> initialContent)
      : object(kj::mv(objectParam)), changeCount(1), created(true),
        newXattr(initialXattr), newContent(kj::mv(initialContent)) {
    if (object->locked) {
      kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "transaction aborted due to conflict"));
    }
    object->locked = true;
  }

  ~LockedTemporary() noexcept(false) {
    object->locked = false;
  }

  void remove() {
    ++changeCount;
    removed = true;
  }

  kj::Maybe<JournalEntry> getJournalEntry(uint64_t stagingId) {
    // Write a journal entry for this object's changes. If a temporary needs to be staged, do so
    // and assign it `stagingId`.

    if (changeCount == 0 || (created && removed)) return nullptr;

    JournalEntry entry;
    memset(&entry, 0, sizeof(entry));

    KJ_IF_MAYBE(c, newContent) {
      c->get()->setRecoveryId(RecoveryId(RecoveryType::STAGING, stagingId));
      entry.stagingId = stagingId;
    }

    entry.temporary.id = object->id;
    KJ_IF_MAYBE(x, newXattr) {
      entry.temporary.xattr = *x;
    } else {
      KJ_ASSERT(removed);
    }

    if (created) {
      entry.type = JournalEntry::Type::CREATE_TEMPORARY;
    } else if (removed) {
      entry.type = JournalEntry::Type::DELETE_TEMPORARY;
    } else if (newContent == nullptr) {
      entry.type = JournalEntry::Type::UPDATE_TEMPORARY_XATTR;
    } else {
      entry.type = JournalEntry::Type::UPDATE_TEMPORARY;
    }

    return entry;
  }

  void execute(BlobLayer& blobLayer) {
    // Execute the changes made to this object, storing (but not necessarily syncing) them to disk.
    // This is called after the journal entry has been synced to disk.
    //
    // No other methods of LockedObject will be called after commit().

    if (created) {
      auto content = kj::mv(KJ_ASSERT_NONNULL(newContent));
      content->setRecoveryId(object->id, KJ_ASSERT_NONNULL(newXattr));
      object->inner = kj::mv(content);
    } else KJ_IF_MAYBE(c, newContent) {
      object->inner->overwrite(KJ_ASSERT_NONNULL(newXattr), kj::mv(*c));
    } else KJ_IF_MAYBE(x, newXattr) {
      object->inner->setXattr(*x);
    }

    if (removed) {
      // Force immediate deletion. We know that no one has a reference to the object since
      // transactionally deleting temporaries requires giving up your reference.
      object->inner = nullptr;
    }

    object->changeCount += changeCount;
  }

  void setRecoveryId(RecoveryId id) override {
    KJ_UNIMPLEMENTED("please use Transaction::createRecoverableTemporary");
  }

  void setRecoveryId(RecoveryId id, TemporaryXattr xattr) override {
    KJ_UNIMPLEMENTED("please use Transaction::createRecoverableTemporary");
  }

  void overwrite(TemporaryXattr xattr, kj::Own<Temporary> replacement) override {
    ++changeCount;
    newXattr = xattr;
    newContent = kj::mv(replacement);
  }

  TemporaryXattr getXattr() override {
    KJ_IF_MAYBE(x, newXattr) {
      return *x;
    } else {
      return object->getXattr();
    }
  }

  void setXattr(TemporaryXattr xattr) override {
    ++changeCount;
    newXattr = xattr;
  }

  BlobLayer::Content& getContent() override {
    KJ_IF_MAYBE(c, newContent) {
      return c->get()->getContent();
    } else {
      return object->getContent();
    }
  }

private:
  kj::Own<JournalLayer::RecoverableTemporary> object;

  uint changeCount;
  bool created;
  bool removed = false;
  kj::Maybe<TemporaryXattr> newXattr;
  kj::Maybe<kj::Own<BlobLayer::Temporary>> newContent;
};

// =======================================================================================

JournalLayer::Transaction::Transaction(JournalLayer &journal): journal(journal) {}
JournalLayer::Transaction::~Transaction() noexcept(false) {}

kj::Own<BlobLayer::Object> JournalLayer::Transaction::wrap(Object& object) {
  KJ_REQUIRE(!committed);

  auto result = kj::refcounted<LockedObject>(kj::addRef(object));
  objects.add(kj::addRef(*result));
  return kj::mv(result);
}

kj::Own<BlobLayer::Temporary> JournalLayer::Transaction::wrap(RecoverableTemporary& object) {
  KJ_REQUIRE(!committed);

  auto result = kj::refcounted<LockedTemporary>(kj::addRef(object));
  temporaries.add(kj::addRef(*result));
  return kj::mv(result);
}

kj::Own<JournalLayer::Object> JournalLayer::Transaction::createObject(
    ObjectId id, Xattr xattr, kj::Own<BlobLayer::Temporary> content) {
  KJ_REQUIRE(!committed);

  auto result = kj::refcounted<JournalLayer::Object>(journal, id, kj::heap<FutureObject>());
  objects.add(kj::refcounted<LockedObject>(kj::addRef(*result), xattr, kj::mv(content)));
  return kj::mv(result);
}

kj::Own<JournalLayer::RecoverableTemporary> JournalLayer::Transaction::createRecoverableTemporary(
    RecoveryId id, TemporaryXattr xattr, kj::Own<BlobLayer::Temporary> content) {
  KJ_REQUIRE(!committed);

  auto result = kj::refcounted<JournalLayer::RecoverableTemporary>(
      journal, id, kj::heap<FutureTemporary>());
  temporaries.add(kj::refcounted<LockedTemporary>(kj::addRef(*result), xattr, kj::mv(content)));
  return kj::mv(result);
}

kj::Promise<void> JournalLayer::Transaction::commit(
    kj::Maybe<kj::Own<RecoverableTemporary>> tempToConsume) {
  KJ_REQUIRE(!committed);
  committed = true;

  kj::Promise<void> result = nullptr;

  KJ_IF_MAYBE(t, tempToConsume) {
    auto wrapper = kj::refcounted<LockedTemporary>(kj::mv(*t));
    wrapper->remove();
    temporaries.add(kj::mv(wrapper));
  }

  KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
    kj::Vector<JournalEntry> entries(objects.size() + temporaries.size());

    // Build the transaction.
    for (auto& object: objects) {
      KJ_IF_MAYBE(entry, object->getJournalEntry(journal.stagingIdCounter++)) {
        entries.add(*entry);
      }
    }
    for (auto& temp: temporaries) {
      KJ_IF_MAYBE(entry, temp->getJournalEntry(journal.stagingIdCounter++)) {
        entries.add(*entry);
      }
    }

    for (auto i: kj::indices(entries)) {
      entries[i].txSize = entries.size() - i;
    }

    // Write to the journal.
    auto& journalContent = journal.journalFile->getContent();
    journalContent.write(journal.journalPosition, entries.asPtr().asBytes());
    uint64_t oldPosition = journal.journalPosition;
    uint64_t newPosition = oldPosition + entries.asPtr().asBytes().size();
    journal.journalPosition = newPosition;
    auto& journalRef = journal;

    // Sync the journal, then execute the transaction (only to kernel cache).
    auto fork = journalContent.sync().then([this]() mutable {
      for (auto& object: objects) {
        object->execute(*journal.blobLayer);
      }
      for (auto& temp: temporaries) {
        temp->execute(*journal.blobLayer);
      }
    }).fork();

    // The transaction is committed at this point.
    result = fork.addBranch();

    // Schedule journal cleanup to happen later, making sure of course that we remove journal
    // entries strictly in the order that we created them.
    auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
    promises.add(fork.addBranch());
    promises.add(kj::mv(journal.writeQueue));
    journal.writeQueue = kj::joinPromises(promises.finish()).then([]() {
      // We have to sync() to make sure all the effects of the transaction have hit disk.
      // TODO(now): Offload sync to another thread. It doesn't even have to sync frequently; every
      //   30 seconds would be fine.
      sync();
    }).then([oldPosition,newPosition,&journalRef]() mutable {
      // We can now safely punch out our journal entry, as it has been completely synced to disk.

      // Round down to nearest block, since holes can only be punched at block boundaries. It's
      // OK if some of a journal entry gets left around for a while.
      oldPosition &= ~(BLOCK_SIZE - 1);
      newPosition &= ~(BLOCK_SIZE - 1);
      uint64_t delta = newPosition - oldPosition;

      // Punch dat hole.
      if (delta > 0) {
        journalRef.journalFile->getContent().zero(oldPosition, delta);
      }
    }, [](kj::Exception&& exception) {
      // It would appear that we failed to actually execute the transaction after writing it to
      // the journal and confirming commit to the client. We should abort now and hope that things
      // get fixed up during recovery.
      KJ_DEFER(abort());
      KJ_LOG(FATAL, "exception during journal execution; aborting", exception);
    });
  })) {
    KJ_DEFER(abort());
    KJ_LOG(FATAL, "exception during journal commit; aborting", *exception);
  }

  return result;
}

// =======================================================================================

kj::Own<JournalLayer::RecoverableTemporary>
JournalLayer::RecoveredTemporary::keepAs(RecoveryId newId) {
  return kj::refcounted<RecoverableTemporary>(journal, newId, inner->keepAs(newId, xattr));
}

void JournalLayer::RecoveredTemporary::setXattr(TemporaryXattr xattr) {
  this->xattr = xattr;
}

void JournalLayer::RecoveredTemporary::overwrite(
    TemporaryXattr xattr, kj::Own<BlobLayer::RecoveredTemporary> replacement) {
  this->xattr = xattr;
  inner = kj::mv(replacement);
}

JournalLayer::RecoveredTemporary::RecoveredTemporary(
    JournalLayer& journal, kj::Own<BlobLayer::RecoveredTemporary> inner)
    : journal(journal), oldId(inner->getOldId()), xattr(inner->getTemporaryXattr()),
      inner(kj::mv(inner)) {}

JournalLayer::RecoveredTemporary::RecoveredTemporary(
    JournalLayer& journal, RecoveryId oldId, TemporaryXattr xattr,
    kj::Own<BlobLayer::RecoveredTemporary> inner)
    : journal(journal), oldId(oldId), xattr(xattr), inner(kj::mv(inner)) {}

// =======================================================================================

JournalLayer::~JournalLayer() noexcept(false) {
  if (openObjects.size() > 0) {
    // This would segfault confusing later, so abort now instead.
    KJ_LOG(FATAL, "Destroyed JournalLayer while objects still exist.");
    abort();
  }
}

kj::Promise<kj::Maybe<kj::Own<JournalLayer::Object>>> JournalLayer::openObject(ObjectId id) {
  // Check the openObjects map to see if this object is already open. Note that the caller is
  // not allowed to call openObject() again without having first dropped the original reference.
  // However, it is possible that the caller has in fact dropped the reference, but that the
  // object is being held open because it is still part of a transaction that has not yet
  // completed. This is why the map lookup is needed here.
  auto iter = openObjects.find(id);
  if (iter != openObjects.end()) {
    return kj::Maybe<kj::Own<JournalLayer::Object>>(kj::addRef(*iter->second));
  }

  return blobLayer->openObject(id).then([this,id](auto&& maybeObject) {
    return kj::mv(maybeObject).map([this,id](kj::Own<BlobLayer::Object>&& object) {
      JournalLayer& super = *this;
      return kj::refcounted<JournalLayer::Object>(super, id, kj::mv(object));
    });
  });
}

kj::Own<BlobLayer::Temporary> JournalLayer::newDetachedTemporary() {
  return blobLayer->newTemporary();
}

// =======================================================================================

JournalLayer::Recovery::Recovery(BlobLayer::Recovery& blobLayer)
    : blobLayerRecovery(blobLayer) {
  for (auto& staging: blobLayer.recoverTemporaries(RecoveryType::STAGING)) {
    uint64_t id = staging->getOldId().id;
    recoveredStaging.insert(std::make_pair(id, kj::mv(staging)));
  }

  for (auto type: ALL_RECOVERY_TYPES) {
    if (type != RecoveryType::STAGING && type != RecoveryType::JOURNAL) {
      for (auto& temp: blobLayer.recoverTemporaries(type)) {
        RecoveryId id = temp->getOldId();
        JournalLayer& super = *this;
        recoveredTemporaries.insert(std::make_pair(id,
            kj::heap<RecoveredTemporary>(super, kj::mv(temp))));
      }
    }
  }

  auto journals = blobLayer.recoverTemporaries(RecoveryType::JOURNAL);
  KJ_ASSERT(journals.size() <= 1);
  if (journals.size() == 1) {
    commitSavedTransaction(journals[0]->getContent());
  }
}

kj::Maybe<kj::Own<JournalLayer::Object>> JournalLayer::Recovery::getObject(ObjectId id) {
  KJ_REQUIRE(!finished, "already called finish()");

  auto iter = openObjects.find(id);
  if (iter != openObjects.end()) {
    return kj::addRef(*iter->second);
  }

  return blobLayerRecovery.getObject(id).map([this,id](kj::Own<BlobLayer::Object>&& object) {
    JournalLayer& super = *this;
    return kj::refcounted<JournalLayer::Object>(super, id, kj::mv(object));
  });
}

kj::Array<kj::Own<JournalLayer::RecoveredTemporary>> JournalLayer::Recovery::recoverTemporaries(
    RecoveryType type) {
  KJ_REQUIRE(!finished, "already called finish()");

  kj::Vector<kj::Own<JournalLayer::RecoveredTemporary>> results;

  auto begin = recoveredTemporaries.lower_bound(RecoveryId(type, 0));
  auto end = recoveredTemporaries.lower_bound(RecoveryId(
      static_cast<RecoveryType>(static_cast<uint>(type) + 1), 0));

  for (auto i = begin; i != end; ++i) {
    results.add(kj::mv(i->second));
  }

  recoveredTemporaries.erase(begin, end);

  return results.releaseAsArray();
}

void JournalLayer::Recovery::commitSavedTransaction(BlobLayer::Content& content) {
  KJ_REQUIRE(!finished, "already called finish()");

  uint64_t start = content.getStart();
  uint64_t end = content.getSize().endMarker;

  auto entries = kj::heapArray<JournalEntry>((end - start) / sizeof(JournalEntry));

  content.read(start, entries.asBytes());

  uint32_t expectedTxSize = 0;  // 0 = expect new transaction; non-zero = expect that txSize next
  JournalEntry* txnStart = entries.begin();
  for (auto& entry: entries) {
    if ((expectedTxSize > 0 && entry.txSize != expectedTxSize) || entry.txSize == 0) {
      // It would seem that the journal is invalid starting here, perhaps because the last
      // transaction had only been partially flushed to disk. In particular it's possible for
      // the file end pointer to be updated before the actual content has been flushed, leaving
      // trailing garbage (usually zeros).
      break;
    }

    expectedTxSize = entry.txSize - 1;

    if (expectedTxSize == 0) {
      // This is the last entry in a transaction!
      for (auto& entryToReplay: kj::arrayPtr(txnStart, &entry + 1)) {
        replayEntry(blobLayerRecovery, entryToReplay);
      }
      txnStart = &entry + 1;
    }
  }
}

JournalLayer& JournalLayer::Recovery::finish() {
  KJ_REQUIRE(!finished, "already called finish()");

  finished = true;
  recoveredStaging.clear();
  recoveredTemporaries.clear();

  // Init JournalLayer members.
  blobLayer = &blobLayerRecovery.finish();
  journalFile = blobLayer->newTemporary();
  journalFile->setRecoveryId(RecoveryId(RecoveryType::JOURNAL, 0));
  writeQueue = kj::READY_NOW;

  return *this;
}

void JournalLayer::Recovery::replayEntry(
    BlobLayer::Recovery& blobLayer, const JournalEntry& entry) {
  kj::Maybe<kj::Own<BlobLayer::RecoveredTemporary>> staging;

  switch (entry.type) {
    case JournalEntry::Type::CREATE_OBJECT:
    case JournalEntry::Type::UPDATE_OBJECT:
    case JournalEntry::Type::CREATE_TEMPORARY:
    case JournalEntry::Type::UPDATE_TEMPORARY: {
      auto iter = recoveredStaging.find(entry.stagingId);
      if (iter != recoveredStaging.end()) {
        staging = kj::mv(iter->second);
        recoveredStaging.erase(iter);
      }
      break;
    }

    case JournalEntry::Type::UPDATE_XATTR:
    case JournalEntry::Type::UPDATE_TEMPORARY_XATTR:
    case JournalEntry::Type::DELETE_OBJECT:
    case JournalEntry::Type::DELETE_TEMPORARY:
      break;
  }

  switch (entry.type) {
    case JournalEntry::Type::CREATE_OBJECT:
      KJ_IF_MAYBE(s, staging) {
        s->get()->keepAs(entry.object.id, entry.object.xattr);
      } else {
        // Staging file is gone, so this object creation must have already happened. No need
        // to `goto updateXattr` as we know there can't be any previous op applying to this object.
      }
      return;

    case JournalEntry::Type::UPDATE_OBJECT:
      KJ_IF_MAYBE(s, staging) {
        s->get()->keepAs(entry.object.id, entry.object.xattr);
      } else {
        // Staging file is missing, probably indicating this op already happend. However, we must
        // still update the xattr as replaying a previous op might have reverted it.
        goto updateXattr;
      }
      return;

    case JournalEntry::Type::UPDATE_XATTR:
    updateXattr:
      KJ_IF_MAYBE(object, blobLayer.getObject(entry.object.id)) {
        object->get()->setXattr(entry.object.xattr);
      }
      return;

    case JournalEntry::Type::DELETE_OBJECT:
      KJ_IF_MAYBE(object, blobLayer.getObject(entry.object.id)) {
        object->get()->remove();
      }
      return;

    case JournalEntry::Type::CREATE_TEMPORARY: {
      KJ_IF_MAYBE(s, staging) {
        if (recoveredTemporaries.count(entry.temporary.id) == 0) {
          JournalLayer& super = *this;
          recoveredTemporaries[entry.temporary.id] =
              kj::heap<JournalLayer::RecoveredTemporary>(super,
                  entry.temporary.id, entry.temporary.xattr, kj::mv(*s));
        }
      } else {
        // Staging file is gone, so this temp creation must have already happened. No need
        // to `goto updateXattr` as we know there can't be any previous op applying to this temp.
      }
      return;
    }

    case JournalEntry::Type::UPDATE_TEMPORARY: {
      KJ_IF_MAYBE(s, staging) {
        auto iter = recoveredTemporaries.find(entry.temporary.id);
        if (iter != recoveredTemporaries.end()) {
          iter->second->overwrite(entry.temporary.xattr, kj::mv(*s));
        }
      } else {
        // Staging file is missing, probably indicating this op already happend. However, we must
        // still update the xattr as replaying a previous op might have reverted it.
        goto updateTemporaryXattr;
      }
      return;
    }

    case JournalEntry::Type::UPDATE_TEMPORARY_XATTR:
    updateTemporaryXattr: {
      auto iter = recoveredTemporaries.find(entry.temporary.id);
      if (iter != recoveredTemporaries.end()) {
        iter->second->setXattr(entry.temporary.xattr);
      }
      return;
    }

    case JournalEntry::Type::DELETE_TEMPORARY:
      recoveredTemporaries.erase(entry.temporary.id);
      return;
  }

  KJ_UNREACHABLE;
}

}  // namespace storage
}  // namespace blackrock
