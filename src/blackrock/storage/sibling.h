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

  BackendSet<Sibling>::Client getBackendSet();

protected:
  kj::Promise<void> getTime(GetTimeContext context) override;
  kj::Promise<void> getReplica(GetReplicaContext context) override;

private:
  class ReplicaImpl;
  class BackendSetImpl;
  class ReconnectingCap;

  struct SiblingRecord {
    Sibling::Client cap;
    kj::Own<kj::PromiseFulfiller<Sibling::Client>> fulfiller;

    uint64_t backendId;
    // Only valid if siblingBackendIds[backendId] points back to this sibling ID.

    SiblingRecord(kj::PromiseFulfillerPair<Sibling::Client> paf =
                  kj::newPromiseAndFulfiller<Sibling::Client>())
        : cap(kj::mv(paf.promise)),
          fulfiller(kj::mv(paf.fulfiller)),
          backendId(kj::maxValue) {}

    void reset() { *this = SiblingRecord(); }
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

  kj::Vector<SiblingRecord> siblings;
  std::unordered_map<uint64_t, uint> siblingBackendIds;
  uint backendSetResetCount = 0;

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
  // Get the given sibling. Method calls on the returned capability will never throw "disconnected"
  // exceptions, but will instead block until the sibling becomes available (possibly forever).
  // Note that capabilities returned through said methods do not have this porperty, since we don't
  // know how to restore access to a disconnected object that isn't the top level.

  StorageConfig::Reader getConfig();

  void getTimeImpl(Sibling::Time::Builder time);

  void taskFailed(kj::Exception&& exception) override;
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_SIBLING_H_
