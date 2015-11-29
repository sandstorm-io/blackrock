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
  // Reads a MidLevelObject. Note that the methods here will read uncommitted values; it is the
  // caller's responsibility to wait for any issued writes to complete before returning to the
  // client.

public:
  virtual ObjectId getId() = 0;
  virtual Xattr getXattr() = 0;
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
  MidLevelObject(JournalLayer& journal, ObjectId id,
                 kj::Own<JournalLayer::Object> object, uint64_t stateRecoveryId);
  // Create a MidLevelObject around an object that exists on disk but has clean state. A state
  // file will be written the first time the object is transacted.

  MidLevelObject(JournalLayer& journal, ObjectId id,
                 kj::Own<JournalLayer::Object> object,
                 kj::Own<JournalLayer::RecoverableTemporary> state,
                 kj::Array<capnp::word> extendedState);
  // Create a MidLevelObject around an object that exists on disk with non-clean state.
  // `exnededState` is the body of stateTemp -- the caller is responsible for asynchronously
  // reading it before calling this.

  MidLevelObject(JournalLayer& journal, ObjectId id, uint64_t stateRecoveryId);
  // Create a MidLevelObject for an object that does NOT already exist on-disk.

  ~MidLevelObject() noexcept(false);

  ObjectId getId() override;
  Xattr getXattr() override;
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

  void cleanShutdown();
  // Attempts a clean shutdown of the object, which allows the state file to be deleted.
  //
  // Throws if the object still needs attention and therefore cannot shut down.

  bool exists() { return !inner.is<Uncreated>(); }

  bool isClean() { return !inner.is<Durable>(); }
  // Returns true if the object can be safely deleted.

private:
  JournalLayer& journal;
  ObjectId id;

  struct Uncreated {
    // Object is not created. First ChangeSet must create it.

    uint64_t stateRecoveryId;
  };

  struct Temporary {
    // Object is created but not yet saved to disk.

    kj::Own<BlobLayer::Temporary> object;
    uint64_t stateRecoveryId;
  };

  struct Durable {
    // Object is on-disk but not clean (has state).

    kj::Own<JournalLayer::Object> object;
    kj::Own<JournalLayer::RecoverableTemporary> state;
  };

  struct Clean {
    // Object is on-disk and clean (no state).

    kj::Own<JournalLayer::Object> object;
    uint64_t stateRecoveryId;
  };

  kj::OneOf<Uncreated, Temporary, Durable, Clean> inner;

  Xattr xattr;
  kj::Array<capnp::word> extendedState;
  // Cached state.

  uint64_t nextVersion;
  bool extendedStateChanged;
  // For setNextVersion() / setNextExtendedState().

  kj::Own<BlobLayer::Temporary> extendedStateBlob();
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_
