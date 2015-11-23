// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_SIBLING_H_
#define BLACKROCK_STORAGE_SIBLING_H_

#include <blackrock/common.h>
#include <blackrock/storage/sibling.capnp.h>
#include "basics.h"
#include "journal-layer.h"
#include <unordered_map>

namespace blackrock {
namespace storage {

class ObjectDistributor {
public:
  virtual kj::Array<uint> getDistribution(ObjectId object) = 0;
  // Returns the set of siblings which replicate the given object.
};

class SiblingImpl: public Sibling::Server, private kj::TaskSet::ErrorHandler {
public:
  SiblingImpl(kj::Timer& timer, JournalLayer::Recovery& journal, ObjectDistributor& distributor,
              uint id);
  ~SiblingImpl() noexcept(false);

  void setSibling(uint id, Sibling::Client cap);

protected:
  kj::Promise<void> getTime(GetTimeContext context) override;
  kj::Promise<void> getReplica(GetReplicaContext context) override;

private:
  class ReplicaImpl;

  struct SiblingRecord {
    // Work around std::unordered_map disliking non-const copy constructor.

    Sibling::Client cap;

    uint connectionNumber = 0;
    // Incremented every time we lose a connection and have to reconnect.

    SiblingRecord() = default;
    KJ_DISALLOW_COPY(SiblingRecord);
    SiblingRecord(SiblingRecord&&) = default;
    SiblingRecord& operator=(SiblingRecord&&) = default;
    SiblingRecord(Sibling::Client cap, uint connectionNumber)
        : cap(kj::mv(cap)), connectionNumber(connectionNumber) {}
  };

  kj::Timer& timer;
  JournalLayer& journal;
  ObjectDistributor& distributor;

  uint id;
  uint generation;
  uint64_t nextTick = 0;

  kj::Own<JournalLayer::RecoverableTemporary> siblingState;

  uint64_t replicaStateRecoveryIdCounter;

  kj::TaskSet tasks;

  std::unordered_map<uint, SiblingRecord> siblings;
  std::unordered_map<ObjectId, ReplicaImpl*, ObjectId::Hash> replicas;

  struct RecoveredObject {
    ObjectId id;
    kj::Own<JournalLayer::Object> object;
    kj::Own<JournalLayer::RecoverableTemporary> state;
  };

  struct RecoveryResult {
    kj::Array<RecoveredObject> objects;
    kj::Maybe<kj::Own<JournalLayer::RecoverableTemporary>> siblingState;
    uint generation;
  };

  static RecoveryResult recoverObjects(JournalLayer::Recovery& recovery);

  SiblingImpl(kj::Timer& timer, JournalLayer::Recovery& journal, ObjectDistributor& distributor,
              uint id, RecoveryResult recovered);

  Sibling::Client getSibling(uint id);
  // Get the given sibling, reconnecting if necessary.

  StorageConfig::Reader getConfig();

  void getTimeImpl(Sibling::Time::Builder time);

  void taskFailed(kj::Exception&& exception) override;
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_SIBLING_H_
