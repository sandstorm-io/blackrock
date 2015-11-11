// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "sibling.h"
#include <kj/debug.h>
#include <capnp/message.h>

namespace blackrock {
namespace storage {

class SiblingImpl::ReplicaImpl: public Replica::Server {
public:
  ObjectId getId() { return id; }

  struct ReplicaRecord {
    Replica::Client cap;
    uint generation;
  };

  kj::Array<Replica::Client> getOtherReplicas() {
    return KJ_MAP(i, kj::range<uint>(0, otherReplicaIds.size())) {
      return getOtherReplica(i);
    };
  }

  Replica::Client getOtherReplica(uint index, uint minGeneration = 0) {
    KJ_REQUIRE(index < otherReplicaIds.size());
    auto req = sibling.getSibling(otherReplicaIds[index])
        .getReplicaRequest(capnp::MessageSize {8,0});
    id.copyTo(req.initId());
    return req.send().getReplica();
  }

  OwnedStorage<>::Client newOwnedStorage(LeaderImpl& leader, Leader::Client leaderCap,
                                         kj::Maybe<OwnedStorage<>::Server&>& weakRef);

  uint64_t getVersion() {
    return inner->getXattr().version;
  }

  SiblingImpl& getSibling() {
    return sibling;
  }

  JournalLayer::Object& getLocalObject() {
    return *inner;
  }

  JournalLayer& getJournal();

protected:
  struct LeaderStatus {
    Replica::Client replica;
    uint64_t version;
    bool isAvailable;
    bool isLeading;
  };

  kj::Promise<void> getState(GetStateContext context) override;

private:
  Sibling::Client siblingCap;
  SiblingImpl& sibling;
  ObjectId id;
  kj::Array<uint> otherReplicaIds;

  kj::Own<JournalLayer::Object> inner;

  capnp::CapabilityServerSet<capnp::Capability> objects;
  // Allows recognizing our own objects when they come back to us, mainly for implementing
  // TransactionBuilder.

  kj::Maybe<FollowerImpl&> localFollower;
  // If this replica is following a leader, this is the Follower implementation.

  friend class FollowerImpl;
};

class SiblingImpl::StorageFactoryImpl: public StorageFactory::Server {
protected:
  kj::Promise<void> newBlob(NewBlobContext context) override {
  }

  kj::Promise<void> uploadBlob(UploadBlobContext context) override {
  }

  kj::Promise<void> newVolume(NewVolumeContext context) override {
  }

  kj::Promise<void> newImmutable(NewImmutableContext context) override {
  }

  kj::Promise<void> newAssignable(NewAssignableContext context) override {
  }

  kj::Promise<void> newAssignableCollection(NewAssignableCollectionContext context) override {
  }

  kj::Promise<void> newOpaque(NewOpaqueContext context) override {
  }

  kj::Promise<void> newTransaction(NewTransactionContext context) override {
  }

  kj::Promise<void> newImmutableCollection(NewImmutableCollectionContext context) override {
  }

private:
  Replica::Client replicaCap;
  ReplicaImpl& replica;
  ObjectId id;
};

kj::Promise<void> SiblingImpl::ReplicaImpl::getState(GetStateContext context) {
  #error todo
}

} // namespace storage
} // namespace blackrock

