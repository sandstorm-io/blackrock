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

class SiblingImpl: public Sibling::Server {
public:
  SiblingImpl(JournalLayer& journal, ObjectDistributor& distributor, uint id);
  ~SiblingImpl() noexcept(false);

  void setSibling(uint id, Sibling::Client cap);

protected:
  kj::Promise<void> createObject(CreateObjectContext context) override;
  kj::Promise<void> getReplica(GetReplicaContext context) override;

private:
  class ReplicaImpl;
  class StorageFactoryImpl;
  class WeakLeaderImpl;
  class LeaderImpl;
  class FollowerImpl;
  class TransactionBuilderImpl;
  class ReplicatedStagedTransaction;
  class DistributedStagedTransaction;

  struct SiblingRecord {
    // Work around std::unordered_map disliking non-const copy constructor.

    Sibling::Client cap;

    uint generation;
    // Incremented every time we lose a connection and have to reconnect.

    KJ_DISALLOW_COPY(SiblingRecord);
    SiblingRecord(SiblingRecord&&) = default;
    SiblingRecord& operator=(SiblingRecord&&) = default;
    SiblingRecord(Sibling::Client cap, uint generation)
        : cap(kj::mv(cap)), generation(generation) {}
  };

  JournalLayer& journal;
  ObjectDistributor& distributor;
  uint id;
  std::unordered_map<uint, SiblingRecord> siblings;
  std::unordered_map<ObjectId, ReplicaImpl*, ObjectId::Hash> replicas;

  SiblingRecord getSibling(uint id, uint minGeneration);
  // Get the given sibling. If the current open capability has generation less than
  // `minGeneration`, reconnect to the sibling, because the current generation is known to be
  // disconnected.

  StorageConfig::Reader getConfig();
};

OwnedStorage<>::Client newOwnedStorageImpl(Leader::Client leader);

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_SIBLING_H_
