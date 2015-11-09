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

  kj::Array<ReplicaRecord> getOtherReplicas() {
    return KJ_MAP(i, kj::range<uint>(0, otherReplicaIds.size())) {
      return getOtherReplica(i);
    };
  }

  ReplicaRecord getOtherReplica(uint index, uint minGeneration = 0) {
    KJ_REQUIRE(index < otherReplicaIds.size());
    auto siblingRecord = sibling.getSibling(otherReplicaIds[index], minGeneration);
    auto req = siblingRecord.cap.getReplicaRequest(capnp::MessageSize {8,0});
    id.copyTo(req.initId());
    return { req.send().getReplica(), siblingRecord.generation };
  }

  OwnedStorage<>::Client newOwnedStorage(LeaderImpl& leader, Leader::Client leaderCap);

  uint64_t getVersion() {
    return inner->getXattr().getVersion();
  }

  SiblingImpl& getSibling() {
    return sibling;
  }

protected:
  struct LeaderStatus {
    Replica::Client replica;
    uint64_t version;
    bool isAvailable;
    bool isLeading;
  };

  kj::Promise<void> getStatus(GetStatusContext context) override;
  kj::Promise<void> follow(FollowContext context) override;

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

class SiblingImpl::WeakLeaderImpl: public WeakLeader::Server, public kj::Refcounted {
public:
  WeakLeaderImpl(LeaderImpl& leader): leader(leader) {}

protected:
  kj::Promise<void> get(GetContext context) override;

private:
  kj::Maybe<LeaderImpl&> leader;

  friend class LeaderImpl;
};

class SiblingImpl::LeaderImpl: public Leader::Server {
public:
  LeaderImpl(ObjectKey key, SiblingImpl& localSibling, ReplicaImpl& localReplica,
             Replica::Client localReplicaCap)
      : key(key), id(key), localSibling(localSibling), localReplica(localReplica),
        localReplicaCap(kj::mv(localReplicaCap)), weak(kj::refcounted<WeakLeaderImpl>(*this)),
        ready(kj::joinPromises(KJ_MAP(siblingId, localSibling.distributor.getDistribution(id))
                -> kj::Promise<FollowResponseRecord> {
              auto replicaSibling = localSibling.getSibling(siblingId, 0);
              Replica::Client replica = ({
                auto req = replicaSibling.cap.getReplicaRequest(capnp::MessageSize {8, 0});
                id.copyTo(req.initId());
                req.send().getReplica();
              });
              auto req = replica.followRequest(capnp::MessageSize {4, 1});
              // TODO(perf): Use a wrapper around `weak` that can proactively warn us when the
              //   follower disconnects.
              req.setLeader(kj::addRef(*weak));
              uint generation = replicaSibling.generation;
              return req.send().then([generation](auto&& response) {
                return FollowResponseRecord { kj::mv(response), generation };
              });
            }).then([this](kj::Array<FollowResponseRecord>&& followers) {
              // TODO(someday): Don't require *all* replicas to respond; accept a quorum. But note
              //   that starting from a non-unanimous quorum requries more setup work.

              uint64_t maxVersion = 0;
              uint64_t startVersion = 0;
              for (auto& follower: followers) {
                maxVersion = kj::max(maxVersion, follower.response.getVersion());
                startVersion = kj::max(startVersion, follower.response.getStartVersion());
              }
              this->version = kj::max(startVersion, maxVersion);

              bool needRecovery = false;
              for (auto& follower: followers) {
                if (follower.response.getVersion() < maxVersion ||
                    !follower.response.isClean()) {
                  needRecovery = true;
                  break;
                }
              }

              if (needRecovery) {
                // We need to commit a transaction on each replica bringing it up-to-date.
                #error todo
              } else {
                // Everything is clean. Let's get started.
                this->followers = KJ_MAP(follower, followers) {
                  return FollowerRecord {
                    follower.response.getFollower(),
                    follower.generation,
                    kj::READY_NOW
                  };
                };
              }
            }).fork()) {}

  ~LeaderImpl() noexcept(false) {
    weak->leader = nullptr;
  }

  using Leader::Server::thisCap;

  JournalLayer::Object& getLocalObject();

  void write(uint64_t offset, kj::Array<const byte> data) {
    queueOp([this,offset,KJ_MVCAP(data)](FollowerRecord& follower) {
      auto req = follower.cap.writeRequest();
      req.setOffset(offset);
      req.setData(data);

      // We only need to send the writes in order. We don't care when they return; that's what
      // sync() is for.
      tasks.add(req.send().then([](auto&&) {}));
      return kj::READY_NOW;
    });
  }

  kj::Promise<void> sync() {
    uint64_t syncVersion = ++version;
    queueOp([syncVersion](FollowerRecord& follower) {
      auto req = follower.cap.syncRequest();
      req.setVersion(syncVersion);
      return req.send().then([](auto&&) {});
    });

    // TODO(perf): It would be OK to allow additional write()s before sync() has returned, as
    //   long as a replica can't get two syncs ahead. It could even be OK to reorder writes before
    //   a sync, if we adjusted how FollowResults.direct is counted.
    return awaitQuorum();
  }

  class LocalModification {
  public:
    LocalModification(): builder(message.getRoot<ObjectModification>()) {}

    ObjectModification::Builder get() { return builder; }
    ObjectModification::Reader get() const { return builder; }

  private:
    capnp::MallocMessageBuilder message;
    ObjectModification::Builder builder;
  };

  kj::Maybe<kj::Promise<void>> modify(kj::Own<LocalModification> mod, uint64_t againstVersion) {
    if (version != againstVersion) {
      return nullptr;
    }

    ++version;

    queueOp([KJ_MVCAP(mod),againstVersion](FollowerRecord& follower) {
      auto req = follower.cap.stageRequest();
      auto txn = req.initTxn();
      txn.setVersion(againstVersion + 1);
      txn.setFromVersion(againstVersion);
      txn.setModification(mod->get());
      auto promise = req.send();
      follower.staged = promise.getStaged();
      return promise.then([](auto&&) {});
    });
    awaitQuorum();

    queueOp([](FollowerRecord& follower) {
      auto txn = kj::mv(KJ_ASSERT_NONNULL(follower.staged));
      follower.staged = nullptr;
      return txn.commitRequest().send().then([](auto&&){});
    });

    // TODO(perf): In theory the transaction is complete as soon as a quorum has staged it, but
    //   we also want the local copy of the object to reflect the transaction before we report
    //   success, so modify() returns when a quorum have acknowledged commit. We could reduce
    //   latency by either:
    //   1. Consider modify() done when only the local replica acknowledges commit.
    //   2. Don't promise that the transaction is on-disk; requiure the caller to have their own
    //      cache. It may be that this is fine for all callers!
    // TODO(perf): Independently of the above, I'm not sure if a quorum barrier is strictly needed
    //   between commit and the next stage. commit() is really a hint that the transaction is now
    //   reliably stored, therefore can be permanently merged. But the follower can also just
    //   assume that on the next commit.
    return awaitQuorum();
  }

protected:
  kj::Promise<void> getObject(GetObjectContext context) override {
    context.getResults(capnp::MessageSize {4,1}).setCap(kj::heap<TransactionBuilderImpl>(*this));
    return kj::READY_NOW;
  }

  kj::Promise<void> startTransaction(StartTransactionContext context) override {
    context.setResults();
  }

  kj::Promise<void> cleanupTransaction(CleanupTransactionContext context) override {
  }

  kj::Promise<void> getTransactionState(GetTransactionStateContext context) override {
  }

private:
  ObjectKey key;
  ObjectId id;
  SiblingImpl& localSibling;
  ReplicaImpl& localReplica;
  Replica::Client localReplicaCap;
  kj::Own<WeakLeaderImpl> weak;
  kj::TaskSet tasks;

  struct FollowerRecord {
    Follower::Client cap;
    uint generation;
    kj::Promise<void> opQueue;
    kj::Maybe<StagedTransaction::Client> staged;
  };
  struct FollowResponseRecord {
    capnp::Response<Replica::FollowResults> response;
    uint generation;
  };

  kj::Array<FollowerRecord> followers;

  uint64_t version = 0;
  // Version after all queued operations complete.

  kj::ForkedPromise<void> ready;

  template <typename T>
  struct RefcountedWrapper: public kj::Refcounted {
    T value;

    template <typename... Params>
    RefcountedWrapper(Params&&... params)
        : value(kj::fwd<Params>(params)...) {}
  };

  void queueOp(kj::Function<kj::Promise<void>(FollowerRecord&)>&& func) {
    // Perform the giver operation on every connected follower.

    auto rcFunc = kj::refcounted<RefcountedWrapper<
        kj::Function<kj::Promise<void>(FollowerRecord&)>>>(kj::mv(func));

    for (auto& follower: followers) {
      auto f = kj::addRef(*rcFunc);
      follower.opQueue = follower.opQueue.then([KJ_MVCAP(f),&follower]() mutable {
        return f->value(follower);
      });
    }
  }

  kj::Promise<void> awaitQuorum() {
    // Wait for a quorum to have completed the last-queued operation before allowing futher
    // operations to be performed on any node. Also, returns a promise which resolves when both
    // a quorum has been reached and the local replica has completed all operations.

    // TODO(perf): Actually await a quorum rather than unanimity.
    auto forked = kj::joinPromises(KJ_MAP(f, followers) { return kj::mv(f.opQueue); }).fork();

    for (auto& follower: followers) {
      follower.opQueue = forked.addBranch();
    }
    return forked.addBranch();
  }
};

kj::Promise<void> SiblingImpl::WeakLeaderImpl::get(GetContext context) {
  KJ_IF_MAYBE(l, leader) {
    context.getResults(capnp::MessageSize {4,1}).setLeader(l->thisCap());
    return kj::READY_NOW;
  } else {
    return KJ_EXCEPTION(DISCONNECTED, "object leader is gone");
  }
}

class SiblingImpl::FollowerImpl: public Follower::Server {
public:

  Leader::Client getLeader();

protected:
  kj::Promise<void> stage(StageContext context) override {
  }

  kj::Promise<void> free(FreeContext context) override {
  }

private:
  kj::Maybe<Replica&> replica;
  // If null, disconnected.

  Leader::Client leader;
};

class SiblingImpl::TransactionBuilderImpl: public TransactionBuilder::Server {
protected:
  kj::Promise<void> getTransactional(GetTransactionalContext context) override {
  }

  kj::Promise<void> addOps(AddOpsContext context) override {
  }

  kj::Promise<void> stage(StageContext context) override {
  }

};

class SiblingImpl::ReplicatedStagedTransaction: public StagedTransaction::Server {
protected:
  kj::Promise<void> commit(CommitContext context) override {
  }

  kj::Promise<void> abort(AbortContext context) override {
  }

};

class SiblingImpl::DistributedStagedTransaction: public StagedTransaction::Server {
protected:
  kj::Promise<void> commit(CommitContext context) override {
  }

  kj::Promise<void> abort(AbortContext context) override {
  }

};

kj::Promise<void> SiblingImpl::ReplicaImpl::getStatus(GetStatusContext context) {
  #error todo
}

kj::Promise<void> SiblingImpl::ReplicaImpl::follow(FollowContext context) {
  #error todo
}

} // namespace storage
} // namespace blackrock

