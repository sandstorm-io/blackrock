// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "mid-level-object.h"
#include <kj/debug.h>

namespace blackrock {
namespace storage {

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               capnp::Capability::Client capToHold,
                               kj::Own<JournalLayer::Object> object,
                               kj::Own<JournalLayer::RecoverableTemporary> state)
    : journal(journal), capToHold(kj::mv(capToHold)),
      xattr(object->getXattr()), nextVersion(xattr.version + 1) {
  this->state.init<Durable>(Durable { kj::mv(object), kj::mv(state) });
}
MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id, uint64_t stateRecoveryId,
                               capnp::Capability::Client capToHold,
                               kj::Own<BlobLayer::Temporary> object)
    : journal(journal), capToHold(kj::mv(capToHold)), nextVersion(0) {
  memset(&xattr, 0, sizeof(xattr));
  state.init<Temporary>(Temporary {
      kj::mv(object), journal.newDetachedTemporary(), stateRecoveryId });
}

Xattr MidLevelObject::getXattr() {
  if (state.is<Durable>()) {
    // Do NOT return this->xattr as it may reflect uncommitted changes.
    return state.get<Durable>().object->getXattr();
  } else {
    return xattr;
  }
}

TemporaryXattr MidLevelObject::getState() {
  if (state.is<Durable>()) {
    // Do NOT return this->xattr as it may reflect uncommitted changes.
    return state.get<Durable>().state->getXattr();
  } else {
    return basicState;
  }
}

kj::ArrayPtr<const capnp::word> MidLevelObject::getExtendedState() {
  KJ_REQUIRE(nextState != nullptr, "can't get extended state while it's being modified");
  return extendedState;
}

BlobLayer::Content::Size MidLevelObject::getSize() {
  if (state.is<Durable>()) {
    return state.get<Durable>().object->getContent().getSize();
  } else {
    return state.get<Temporary>().object->getContent().getSize();
  }
}

kj::Promise<void> MidLevelObject::read(uint64_t offset, kj::ArrayPtr<byte> buffer) {
  if (state.is<Durable>()) {
    return state.get<Durable>().object->getContent().read(offset, buffer);
  } else {
    return state.get<Temporary>().object->getContent().read(offset, buffer);
  }
}

void MidLevelObject::write(uint64_t offset, kj::ArrayPtr<const byte> data) {
  if (state.is<Durable>()) {
    state.get<Durable>().object->getContent().write(offset, data);
  } else {
    state.get<Temporary>().object->getContent().write(offset, data);
  }
}

kj::Promise<void> MidLevelObject::sync() {
  xattr.version = nextVersion;
  ++nextVersion;

  if (state.is<Durable>()) {
    return state.get<Durable>().object->sync(xattr.version);
  } else {
    // irrelevant for temporaries
    return kj::READY_NOW;
  }
}

kj::Promise<void> MidLevelObject::modify(ChangeSet::Reader changes) {
  xattr.version = nextVersion;
  ++nextVersion;

  // Any transaction makes us non-dirty.
  xattr.dirty = false;

  if (static_cast<uint>(xattr.type) == 0) {
    // Not created.
    KJ_REQUIRE(changes.getCreate() != 0, "first modification must create the object");
    xattr.type = static_cast<ObjectType>(changes.getCreate());
  } else {
    KJ_REQUIRE(changes.getCreate() == 0, "object already created");
  }

  if (changes.getShouldBecomeReadOnly()) {
    xattr.readOnly = true;
  }

  xattr.transitiveBlockCount += changes.getAdjustTransitiveBlockCount();

  if (changes.hasSetOwner()) {
    xattr.owner = changes.getSetOwner();
  }

  if (changes.getShouldClearBackburner()) {
    #error todo
  }

  if (changes.getBackburnerModifyTransitiveBlockCount() != 0) {
    #error todo
  }

  if (changes.getBackburnerRecursivelyDelete()) {
    #error todo
  }

  if (state.is<Durable>()) {
    JournalLayer::Transaction txn(journal);
    auto wrapped = txn.wrap(*state.get<Durable>().object);
    if (changes.hasSetContent()) {
      auto replacement = journal.newDetachedTemporary();
      replacement->getContent().write(0, changes.getSetContent());
      wrapped->overwrite(xattr, kj::mv(replacement));
    } else {
      wrapped->setXattr(xattr);
    }
    return txn.commit();
  } else if (changes.hasSetOwner()) {
    // Transition to durable when the owner is set.
    JournalLayer::Transaction txn(journal);
    auto newObject = txn.createObject(id, xattr, kj::mv(state.get<Temporary>().object));
    auto newState = txn.createRecoverableTemporary(
        RecoveryId(RecoveryType::OBJECT_STATE, state.get<Temporary>().stateRecoveryId),
        basicState, kj::mv(state.get<Temporary>().state));
    state.init<Durable>(Durable { kj::mv(newObject), kj::mv(newState) });
    return txn.commit();
  } else {
    if (changes.hasSetContent()) {
      auto replacement = journal.newDetachedTemporary();
      replacement->getContent().write(0, changes.getSetContent());
      state.get<Temporary>().object = kj::mv(replacement);
    }
    return kj::READY_NOW;
  }
}

kj::Promise<void> MidLevelObject::setDirty() {
  if (!state.is<Durable>()) {
    // Dirtiness is irrelevant for temporaries, since the transaction that commits it will make
    // it clean.
    return kj::READY_NOW;
  } else if (xattr.dirty) {
    // Already dirty.
    return kj::READY_NOW;
  } else {
    xattr.dirty = true;
    JournalLayer::Transaction txn(journal);
    auto wrapped = txn.wrap(*state.get<Durable>().object);
    wrapped->setXattr(xattr);
    return txn.commit();
  }
}

void MidLevelObject::setNextVersion(uint64_t version) {
  nextVersion = version;
}

void MidLevelObject::setNextExtendedState(kj::Array<capnp::word> data) {
  auto temp = journal.newDetachedTemporary();
  temp->getContent().write(0, data.asBytes());
  nextState = kj::mv(temp);
  extendedState = kj::mv(data);
}

kj::Promise<void> MidLevelObject::modifyExtendedState(kj::Array<capnp::word> data) {
  auto temp = journal.newDetachedTemporary();
  temp->getContent().write(0, data.asBytes());
  nextState = nullptr;
  extendedState = kj::mv(data);

  if (state.is<Durable>()) {
    JournalLayer::Transaction txn(journal);
    txn.wrap(*state.get<Durable>().state)->overwrite(basicState, kj::mv(temp));
    return txn.commit();
  } else {
    state.get<Temporary>().state = kj::mv(temp);
    return kj::READY_NOW;
  }
}

} // namespace storage
} // namespace blackrock

