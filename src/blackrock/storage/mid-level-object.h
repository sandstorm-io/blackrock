// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_
#define BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_

#include "journal-layer.h"
#include <blackrock/storage/basics.capnp.h>
#include <capnp/message.h>

namespace blackrock {
namespace storage {

class MidLevelWriter {
public:
  virtual void write(uint64_t offset, kj::ArrayPtr<const byte> data) = 0;
  virtual kj::Promise<void> sync() = 0;
  // Non-transactional write and sync.

  virtual kj::Promise<void> modify(ChangeSet::Reader changes) = 0;
  // Atomically apply a ChangeSet.

  class Replacer {
  public:
    virtual void write(uint64_t offset, kj::ArrayPtr<const byte> data) = 0;
    virtual kj::Promise<void> finish(uint64_t version, ChangeSet::Reader txn) = 0;
  };

  virtual kj::Own<Replacer> startReplace() = 0;
  // Arrange to replace the whole object with new content. If and when you call Replacer::finish(),
  // the whole object is replaced transactionally.
};

class MidLevelReader {
public:
  virtual Xattr getXattr() = 0;
  virtual TemporaryXattr getState() = 0;
  virtual kj::ArrayPtr<const capnp::word> getExtendedState() = 0;
  virtual BlobLayer::Content::Size getSize() = 0;
  virtual kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) = 0;
};

class MidLevelObject final: public MidLevelReader, public MidLevelWriter {
  // A transactional object. Sits above the journal and blob layers, but below the object graph
  // layer.
  //
  // Split into two interfaces MidLevelReader and MidLevelWriter so that replication can be
  // injected into the write path.

public:
  MidLevelObject(JournalLayer& journal, ObjectId id, capnp::Capability::Client capToHold,
                 kj::Own<JournalLayer::Object> object,
                 kj::Own<JournalLayer::RecoverableTemporary> state);
  MidLevelObject(JournalLayer& journal, ObjectId id, uint64_t stateRecoveryId,
                 capnp::Capability::Client capToHold,
                 kj::Own<BlobLayer::Temporary> stagingContent);

  Xattr getXattr() override;
  TemporaryXattr getState() override;
  kj::ArrayPtr<const capnp::word> getExtendedState() override;
  BlobLayer::Content::Size getSize() override;
  kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) override;

  void write(uint64_t offset, kj::ArrayPtr<const byte> data) override;
  kj::Promise<void> sync() override;
  kj::Promise<void> modify(ChangeSet::Reader changes) override;
  kj::Own<Replacer> startReplace() override;

  // Methods below are only used with replication.

  kj::Promise<void> setDirty();
  // Mark this object "dirty".

  void setNextVersion(uint64_t version);
  // Set the version number which should be reached the next time sync() or modify() is called.
  // By default each call to sync() or modify() will increment the version, but this method
  // allows manual control.

  void setNextExtendedState(kj::Array<capnp::word> data);
  // Set new content for the object's "extended state" to be written out on the next modify().

  kj::Promise<void> modifyExtendedState(kj::Array<capnp::word> data);
  // Commit a transaction now which sets the extended state as specified.

private:
  JournalLayer& journal;
  capnp::Capability::Client capToHold;

  struct Temporary {
    kj::Own<BlobLayer::Temporary> object;
    kj::Own<BlobLayer::Temporary> state;
    uint64_t stateRecoveryId;
  };

  struct Durable {
    kj::Own<JournalLayer::Object> object;
    kj::Own<JournalLayer::RecoverableTemporary> state;
  };

  ObjectId id;
  kj::OneOf<Durable, Temporary> state;
  Xattr xattr;
  TemporaryXattr basicState;
  kj::Array<capnp::word> extendedState;

  uint64_t nextVersion;
  kj::Maybe<kj::Own<BlobLayer::Temporary>> nextState;
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_
