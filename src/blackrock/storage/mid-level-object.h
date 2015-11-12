// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_
#define BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_

#include "journal-layer.h"
#include <blackrock/storage/basics.capnp.h>

namespace blackrock {
namespace storage {

class MidLevelWriter {
public:
  virtual void write(uint64_t offset, kj::ArrayPtr<const byte> data) = 0;
  virtual kj::Promise<void> sync() = 0;
  virtual kj::Promise<void> modify(ChangeSet::Reader changes) = 0;

  class Replacer {
  public:
    virtual void write(uint64_t offset, kj::ArrayPtr<const byte> data) = 0;
    virtual kj::Promise<void> finish(uint64_t version, ChangeSet::Reader txn) = 0;
  };

  virtual kj::Own<Replacer> startReplace() = 0;
  // Arrange to replace the whole object with new content. If and when you call Replacer::finish(),
  // the whole object is replaced transactionally.

  virtual kj::Promise<void> setDirty() = 0;
  // Mark this object "dirty". Only really matters when using replication.

  virtual void setNextVersion(uint64_t version) = 0;
  // Set the version number which should be reached the next time sync() or modify() is called.
  // By default each call to sync() or modify() will increment the version, but this method
  // allows manual control. Versions only really matter when using replication.
};

class MidLevelReader {
public:
  virtual Xattr getXattr() = 0;
  virtual BlobLayer::Content::Size getSize() = 0;
  virtual kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) = 0;
};

class MidLevelObject final: public MidLevelReader, public MidLevelWriter {
public:
  MidLevelObject(JournalLayer& journal, capnp::Capability::Client capToHold,
                 kj::Own<JournalLayer::Object> object);
  MidLevelObject(JournalLayer& journal, capnp::Capability::Client capToHold,
                 kj::Own<BlobLayer::Temporary> object);

  Xattr getXattr() override;
  BlobLayer::Content::Size getSize() override;
  kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) override;

  void write(uint64_t offset, kj::ArrayPtr<const byte> data) override;
  kj::Promise<void> sync() override;
  kj::Promise<void> modify(ChangeSet::Reader changes) override;
  kj::Own<Replacer> startReplace() override;

  kj::Promise<void> setDirty() override;
  void setNextVersion(uint64_t version) override;

private:
  JournalLayer& journal;
  capnp::Capability::Client capToHold;

  struct Temporary {
    kj::Own<BlobLayer::Temporary> object;
  };

  struct Durable {
    kj::Own<JournalLayer::Object> object;
  };

  kj::OneOf<Durable, Temporary> state;
  Xattr xattr;
  uint64_t nextVersion;
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_MID_LEVEL_OBJECT_H_
