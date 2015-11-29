// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "mid-level-object.h"
#include <kj/debug.h>
#include <capnp/serialize.h>

namespace blackrock {
namespace storage {

static Xattr cleanXattr() {
  Xattr result;
  memset(&result, 0, sizeof(result));
  return result;
}

static TemporaryXattr temporaryXattr(const ObjectId& id) {
  TemporaryXattr result;
  memset(&result, 0, sizeof(result));
  result.objectState.ojbectId = id;
  return result;
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               kj::Own<JournalLayer::Object> object,
                               uint64_t stateRecoveryId)
    : journal(journal), id(id),
      xattr(object->getXattr()), extendedState(nullptr),
      nextVersion(xattr.version + 1) {
  inner.init<Clean>(Clean { kj::mv(object), stateRecoveryId });
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               kj::Own<JournalLayer::Object> object,
                               kj::Own<JournalLayer::RecoverableTemporary> state,
                               kj::Array<capnp::word> extendedState)
    : journal(journal), id(id),
      xattr(object->getXattr()),
      extendedState(kj::mv(extendedState)),
      nextVersion(xattr.version + 1) {
  inner.init<Durable>(Durable { kj::mv(object), kj::mv(state) });
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               uint64_t stateRecoveryId)
    : journal(journal), id(id),
      xattr(cleanXattr()),
      extendedState(nullptr),
      nextVersion(0) {
  inner.init<Uncreated>(Uncreated { stateRecoveryId });
}

MidLevelObject::~MidLevelObject() noexcept(false) {
  if (inner.is<Durable>()) {
    // We have a state file that is not clean. If we let the destructor run, it will be deleted.
    // We'll possibly lose data. Better to abort.
    KJ_DEFER(abort());
    KJ_LOG(FATAL,
        "tried to destroy MidLevelObject with unclean state; aborting to avoid data loss");
  }
}

ObjectId MidLevelObject::getId() {
  return id;
}

Xattr MidLevelObject::getXattr() {
  return xattr;
}

kj::ArrayPtr<const capnp::word> MidLevelObject::getExtendedState() {
  return extendedState;
}

BlobLayer::Content::Size MidLevelObject::getSize() {
  if (inner.is<Durable>()) {
    return inner.get<Durable>().object->getContent().getSize();
  } else if (inner.is<Temporary>()) {
    return inner.get<Temporary>().object->getContent().getSize();
  } else if (inner.is<Clean>()) {
    return inner.get<Clean>().object->getContent().getSize();
  } else {
    return {0, 0};
  }
}

kj::Promise<void> MidLevelObject::read(uint64_t offset, kj::ArrayPtr<byte> buffer) {
  if (inner.is<Durable>()) {
    return inner.get<Durable>().object->getContent().read(offset, buffer);
  } else if (inner.is<Temporary>()) {
    return inner.get<Temporary>().object->getContent().read(offset, buffer);
  } else if (inner.is<Clean>()) {
    return inner.get<Clean>().object->getContent().read(offset, buffer);
  } else {
    memset(buffer.begin(), 0, buffer.size());
    return kj::READY_NOW;
  }
}

void MidLevelObject::write(uint64_t offset, kj::ArrayPtr<const byte> data) {
  if (inner.is<Durable>()) {
    inner.get<Durable>().object->getContent().write(offset, data);
  } else if (inner.is<Temporary>()) {
    inner.get<Temporary>().object->getContent().write(offset, data);
  } else if (inner.is<Clean>()) {
    inner.get<Clean>().object->getContent().write(offset, data);
  } else {
    KJ_FAIL_REQUIRE("object not yet created");
  }
}

kj::Promise<void> MidLevelObject::sync() {
  xattr.version = nextVersion;
  ++nextVersion;

  if (inner.is<Durable>()) {
    return inner.get<Durable>().object->sync(xattr.version);
  } else if (inner.is<Clean>()) {
    return inner.get<Clean>().object->sync(xattr.version);
  } else if (inner.is<Temporary>()) {
    // irrelevant for temporaries
    return kj::READY_NOW;
  } else {
    // not created; ignore
    return kj::READY_NOW;
  }
}

kj::Promise<void> MidLevelObject::modify(ChangeSet::Reader changes) {
  xattr.version = nextVersion;
  ++nextVersion;

  // Any transaction makes us non-dirty.
  xattr.dirty = false;

  if (inner.is<Uncreated>() == 0) {
    // Not created.
    KJ_REQUIRE(changes.getCreate() != 0, "first modification must create the object");
    xattr.type = static_cast<ObjectType>(changes.getCreate());
  } else {
    KJ_REQUIRE(changes.getCreate() == 0, "object already created");
  }

  if (changes.getShouldBecomeReadOnly()) {
    xattr.readOnly = true;
  }

  if (changes.getShouldMarkForDeletion()) {
    xattr.pendingRemoval = true;
  }

  if (changes.getShouldSetTransitiveBlockCount()) {
    xattr.transitiveBlockCount = changes.getSetTransitiveBlockCount();
    xattr.dirtyTransitiveSize = changes.getShouldMarkTransitiveBlockCountDirty();
  } else if (changes.getShouldMarkTransitiveBlockCountDirty()) {
    xattr.dirtyTransitiveSize = true;
  }

  if (changes.hasSetOwner()) {
    xattr.owner = changes.getSetOwner();
  }

  kj::Maybe<kj::Own<BlobLayer::Temporary>> newContent;
  if (changes.hasSetContent()) {
    auto replacement = journal.newDetachedTemporary();
    replacement->getContent().write(0, changes.getSetContent());
    newContent = kj::mv(replacement);
  }

  if (inner.is<Durable>() || inner.is<Clean>()) {
    JournalLayer::Transaction txn(journal);
    auto wrapped = txn.wrap(inner.is<Durable>()
        ? *inner.get<Durable>().object
        : *inner.get<Clean>().object);
    if (changes.getShouldDelete()) {
      wrapped->remove();
    } else KJ_IF_MAYBE(c, newContent) {
      wrapped->overwrite(xattr, kj::mv(*c));
    } else {
      wrapped->setXattr(xattr);
    }

    if (inner.is<Clean>()) {
      // clean -- create new state
      auto state = txn.createRecoverableTemporary(
          RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Clean>().stateRecoveryId),
          temporaryXattr(id), extendedStateBlob());
      inner.init<Durable>(Durable {
        kj::mv(inner.get<Clean>().object), kj::mv(state)
      });
    } else if (extendedStateChanged) {
      // update extended state
      txn.wrap(*inner.get<Durable>().state)->overwrite(temporaryXattr(id), extendedStateBlob());
    }
    return txn.commit();
  } else {
    if (inner.is<Uncreated>()) {
      if (newContent == nullptr) {
        // Assume empty initial content.
        newContent = journal.newDetachedTemporary();
      }
      uint64_t stateRecoveryId = inner.get<Uncreated>().stateRecoveryId;
      inner.init<Temporary>();
      inner.get<Temporary>().stateRecoveryId = stateRecoveryId;
    }

    KJ_IF_MAYBE(c, newContent) {
      inner.get<Temporary>().object = kj::mv(*c);
    }

    if (changes.hasSetOwner()) {
      // Transition to durable when the owner is set.
      JournalLayer::Transaction txn(journal);
      auto newObject = txn.createObject(id, xattr, kj::mv(inner.get<Temporary>().object));
      auto newState = txn.createRecoverableTemporary(
          RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Temporary>().stateRecoveryId),
          temporaryXattr(id), extendedStateBlob());
      inner.init<Durable>(Durable { kj::mv(newObject), kj::mv(newState) });
      return txn.commit();
    } else {
      return kj::READY_NOW;
    }
  }
}

kj::Own<MidLevelObject::Replacer> MidLevelObject::startReplace() {
  #error todo
}

void MidLevelObject::cleanShutdown() {
  if (inner.is<Uncreated>() || inner.is<Temporary>()) {
    // Not leaving any mess on disk, so ignore.
    return;
  }

  KJ_REQUIRE(!xattr.pendingRemoval && !xattr.dirty && !xattr.dirtyTransitiveSize,
             "can't cleanly shut down when still marked dirty");

  // OK, we can drop the temporary object.
  if (inner.is<Durable>()) {
    auto object = kj::mv(inner.get<Durable>().object);
    auto recoveryId = inner.get<Durable>().state->getId().id;
    inner.init<Clean>(Clean { kj::mv(object), recoveryId });
  }
}

kj::Promise<void> MidLevelObject::setDirty() {
  if (xattr.dirty) {
    // Already dirty.
    return kj::READY_NOW;
  } else if (inner.is<Durable>() || inner.is<Clean>()) {
    xattr.dirty = true;
    JournalLayer::Transaction txn(journal);
    auto wrapped = txn.wrap(inner.is<Durable>()
        ? *inner.get<Durable>().object
        : *inner.get<Clean>().object);
    wrapped->setXattr(xattr);
    return txn.commit();
  } else {
    // Dirtiness is irrelevant for temporaries, since the transaction that commits it will make
    // it clean.
    return kj::READY_NOW;
  }
}

void MidLevelObject::setNextVersion(uint64_t version) {
  nextVersion = version;
}

void MidLevelObject::setNextExtendedState(kj::Array<capnp::word> data) {
  extendedState = kj::mv(data);
  extendedStateChanged = true;
}

kj::Promise<void> MidLevelObject::modifyExtendedState(kj::Array<capnp::word> data) {
  setNextExtendedState(kj::mv(data));

  if (inner.is<Durable>()) {
    JournalLayer::Transaction txn(journal);
    txn.wrap(*inner.get<Durable>().state)->overwrite(temporaryXattr(id), extendedStateBlob());
    return txn.commit();
  } else if (inner.is<Clean>()) {
    // Transition to Durable.
    JournalLayer::Transaction txn(journal);
    auto state = txn.createRecoverableTemporary(
        RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Clean>().stateRecoveryId),
        temporaryXattr(id), extendedStateBlob());
    inner.init<Durable>(Durable {
      kj::mv(inner.get<Clean>().object), kj::mv(state)
    });
    return txn.commit();
  } else if (inner.is<Temporary>()) {
    return kj::READY_NOW;
  } else {
    KJ_FAIL_REQUIRE("object not created");
  }
}

kj::Own<BlobLayer::Temporary> MidLevelObject::extendedStateBlob() {
  auto temp = journal.newDetachedTemporary();
  if (extendedState != nullptr) {
    temp->getContent().write(0, extendedState.asBytes());
  }
  extendedStateChanged = false;
  return temp;
}

} // namespace storage
} // namespace blackrock

