// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "mid-level-object.h"
#include <kj/debug.h>

namespace blackrock {
namespace storage {

MidLevelObject::MidLevelObject(JournalLayer& journal, capnp::Capability::Client capToHold,
                               kj::Own<JournalLayer::Object> object)
    : journal(journal), capToHold(kj::mv(capToHold)),
      xattr(object->getXattr()), nextVersion(xattr.version + 1) {
  state.init<Durable>(Durable { kj::mv(object) });
}
MidLevelObject::MidLevelObject(JournalLayer& journal, capnp::Capability::Client capToHold,
                               kj::Own<BlobLayer::Temporary> object)
    : journal(journal), capToHold(kj::mv(capToHold)), nextVersion(0) {
  memset(&xattr, 0, sizeof(xattr));
  state.init<Durable>(Temporary { kj::mv(object) });
}

Xattr MidLevelObject::getXattr() {
  if (state.is<Durable>()) {
    // Do NOT return this->xattr as it may reflect uncommitted changes.
    return state.get<Durable>().object->getXattr();
  } else {
    return xattr;
  }
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

} // namespace storage
} // namespace blackrock

