// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "mid-level-object.h"
#include <kj/debug.h>

namespace blackrock {
namespace storage {

static Xattr cleanXattr() {
  Xattr result;
  memset(&result, 0, sizeof(result));
  return result;
}

static TemporaryXattr cleanBasicState(const ObjectId& id) {
  TemporaryXattr result;
  memset(&result, 0, sizeof(result));
  result.objectState.ojbectId = id;
  return result;
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               kj::Own<JournalLayer::Object> object,
                               uint64_t stateRecoveryId)
    : journal(journal), id(id),
      xattr(object->getXattr()), basicState(cleanBasicState(id)), extendedState(nullptr),
      nextVersion(xattr.version + 1) {
  inner.init<Clean>(Clean { kj::mv(object), stateRecoveryId });
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               kj::Own<JournalLayer::Object> object,
                               kj::Own<JournalLayer::RecoverableTemporary> state,
                               kj::Array<capnp::word> extendedState)
    : journal(journal), id(id),
      xattr(object->getXattr()),
      basicState(state->getXattr()),
      extendedState(kj::mv(extendedState)),
      nextVersion(xattr.version + 1) {
  inner.init<Durable>(Durable { kj::mv(object), kj::mv(state) });
}

MidLevelObject::MidLevelObject(JournalLayer& journal, ObjectId id,
                               uint64_t stateRecoveryId)
    : journal(journal), id(id),
      xattr(cleanXattr()),
      basicState(cleanBasicState(id)),
      extendedState(nullptr),
      nextVersion(0) {
  inner.init<Uncreated>(Uncreated { stateRecoveryId });
}

Xattr MidLevelObject::getXattr() {
  return xattr;
}

TemporaryXattr MidLevelObject::getState() {
  return basicState;
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

  bool stateChanged = nextState != nullptr;

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

  xattr.transitiveBlockCount += changes.getAdjustTransitiveBlockCount();

  if (changes.hasSetOwner()) {
    xattr.owner = changes.getSetOwner();
  }

  if (changes.getShouldClearBackburner()) {
    #error "todo: unregister backburner"
    basicState.objectState.blockCountDelta = 0;
    basicState.objectState.remove = false;
    stateChanged = true;
  }

  if (changes.getBackburnerModifyTransitiveBlockCount() != 0) {
    #error "todo: register backburner"
    basicState.objectState.blockCountDelta = changes.getBackburnerModifyTransitiveBlockCount();
    stateChanged = true;
  }

  if (changes.getBackburnerRecursivelyDelete()) {
    #error "todo: register backburner"
    basicState.objectState.remove = true;
    stateChanged = true;
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
    if (stateChanged) {
      if (inner.is<Durable>()) {
        auto wrapped = txn.wrap(*inner.get<Durable>().state);
        KJ_IF_MAYBE(c, nextState) {
          wrapped->overwrite(basicState, kj::mv(*c));
          nextState = nullptr;
        } else {
          wrapped->setXattr(basicState);
        }
      } else {
        // clean -- create new state
        if (nextState == nullptr) {
          // Assume empty extended state.
          nextState = journal.newDetachedTemporary();
        }

        auto state = txn.createRecoverableTemporary(
            RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Clean>().stateRecoveryId),
            basicState, kj::mv(KJ_ASSERT_NONNULL(nextState)));
        nextState = nullptr;

        inner.init<Durable>(Durable {
          kj::mv(inner.get<Clean>().object), kj::mv(state)
        });
      }
    }
    return txn.commit();
  } else {
    if (inner.is<Uncreated>()) {
      if (newContent == nullptr) {
        // Assume empty initial content.
        newContent = journal.newDetachedTemporary();
      }
      if (nextState == nullptr) {
        // Assume empty extended state.
        nextState = journal.newDetachedTemporary();
      }
      uint64_t stateRecoveryId = inner.get<Uncreated>().stateRecoveryId;
      inner.init<Temporary>();
      inner.get<Temporary>().stateRecoveryId = stateRecoveryId;
    }

    KJ_IF_MAYBE(c, newContent) {
      inner.get<Temporary>().object = kj::mv(*c);
    }
    KJ_IF_MAYBE(c, nextState) {
      inner.get<Temporary>().state = kj::mv(*c);
      nextState = nullptr;
    }

    if (changes.hasSetOwner()) {
      // Transition to durable when the owner is set.
      JournalLayer::Transaction txn(journal);
      auto newObject = txn.createObject(id, xattr, kj::mv(inner.get<Temporary>().object));
      auto newState = txn.createRecoverableTemporary(
          RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Temporary>().stateRecoveryId),
          basicState, kj::mv(inner.get<Temporary>().state));
      inner.init<Durable>(Durable { kj::mv(newObject), kj::mv(newState) });
      return txn.commit();
    } else {
      return kj::READY_NOW;
    }
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

  if (inner.is<Durable>()) {
    JournalLayer::Transaction txn(journal);
    txn.wrap(*inner.get<Durable>().state)->overwrite(basicState, kj::mv(temp));
    return txn.commit();
  } else if (inner.is<Clean>()) {
    JournalLayer::Transaction txn(journal);
    auto state = txn.createRecoverableTemporary(
        RecoveryId(RecoveryType::OBJECT_STATE, inner.get<Clean>().stateRecoveryId),
        basicState, kj::mv(temp));
    inner.init<Durable>(Durable {
      kj::mv(inner.get<Clean>().object), kj::mv(state)
    });
    return txn.commit();
  } else if (inner.is<Temporary>()) {
    inner.get<Temporary>().state = kj::mv(temp);
    return kj::READY_NOW;
  } else {
    KJ_FAIL_REQUIRE("object not created");
  }
}

} // namespace storage
} // namespace blackrock

