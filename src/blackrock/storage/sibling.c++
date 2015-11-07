// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "sibling.h"
#include <kj/debug.h>

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

  kj::Promise<void> getLeader(GetLeaderContext context) override;

  kj::Promise<void> getLeaderStatus(GetLeaderStatusContext context) override {
  }

  kj::Promise<void> follow(FollowContext context) override {
  }

private:
  Sibling::Client siblingCap;
  SiblingImpl& sibling;
  ObjectId id;
  kj::Array<uint> otherReplicaIds;

  kj::Own<JournalLayer::Object> inner;

  capnp::CapabilityServerSet<capnp::Capability> objects;
  // Allows recognizing our own objects when they come back to us, mainly for implementing
  // TransactionBuilder.

  kj::OneOf<capnp::Void, LeaderImpl*, FollowerImpl*> state;
  // Current state.

  friend class LeaderImpl;
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

class SiblingImpl::LeaderImpl: public Leader::Server {
public:
  LeaderImpl(ReplicaImpl& replica)
      : replica(replica) {
    KJ_REQUIRE(replica.state.is<capnp::Void>());
    replica.state.init<LeaderImpl*>(this);
  }

  ~LeaderImpl() noexcept(false) {
    abdicate();
  }

  void abdicate() {
    KJ_IF_MAYBE(r, replica) {
      if (r->state.is<LeaderImpl*>() && r->state.get<LeaderImpl*>() == this) {
        r->state.init<capnp::Void>(capnp::VOID);
      } else {
        KJ_FAIL_REQUIRE("inconsistent state");
      }
      replica = nullptr;
    }
  }

  using Leader::Server::thisCap;

protected:
  kj::Promise<void> getObject(GetObjectContext context) override {
    context.allowCancellation();

    // Check that key matches.
    ObjectKey inputKey = context.getParams().getKey();
    KJ_REQUIRE(ObjectId(inputKey) == localReplica().getId(), "key mismatch");
    key = inputKey;

    if (!electionStarted) {
      // We can't start the election in the constructor because thisCap() doesn't work yet.

      electionStarted = true;
      auto paf = kj::newPromiseAndFulfiller<OwnedStorage<>::Client>();
      electedFulfiller = kj::mv(paf.fulfiller);
      elected = paf.promise.fork();

      auto replicas = localReplica().getOtherReplicas();
      votes = kj::heapArray<bool>(replicas.size());

      uint i = 0;
      followers = KJ_MAP(replica, replicas) {
        uint index = i++;
        votes[index] = false;
        return addFollower(index, kj::mv(replica));
      };

      if (localReplica().getSibling().getConfig().getQuorumSize() == 1) {
        // The leader itself forms a quorum.
        electedFulfiller->fulfill(newOwnedStorageImpl(thisCap()));
      }
    }

    context.getResults(capnp::MessageSize {4, 1}).setCap(elected.addBranch());
    return kj::READY_NOW;
  }

  kj::Promise<void> startTransaction(StartTransactionContext context) override {
  }

  kj::Promise<void> cleanupTransaction(CleanupTransactionContext context) override {
  }

  kj::Promise<void> getTransactionState(GetTransactionStateContext context) override {
  }

private:
  kj::Maybe<ReplicaImpl&> replica;
  // If null, disconnected.

  ObjectKey key;

  bool electionStarted = false;
  uint voteCount = 1;  // vote for self
  kj::Array<Follower::Client> followers;
  kj::Array<bool> votes;
  kj::Own<kj::PromiseFulfiller<OwnedStorage<>::Client>> electedFulfiller;
  kj::ForkedPromise<OwnedStorage<>::Client> elected = nullptr;

  ReplicaImpl& localReplica() {
    KJ_IF_MAYBE(r, replica) {
      return *r;
    } else {
      kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "object leader has been deposed"));
    }
  }

  Follower::Client addFollower(uint index, ReplicaImpl::ReplicaRecord&& followerReplica) {
    auto req = followerReplica.cap.followRequest(capnp::MessageSize {4,1});
    req.setLeader(thisCap());
    uint generation = followerReplica.generation;
    return req.send().then([this,index](auto&& response) {
      auto followerVersion = response.getVersion();
      if (followerVersion > localReplica().getVersion()) {
        // Oh crap, this replica is newer than us. We can't possibly lead.
        // Note: It should be impossible for the election to have completed at this point because
        //   if one replica is at a newer version then a quorum of them would have to be.
        abdicate();
        KJ_LOG(WARNING, "RARE: We tried to be leader but found another node is newer than us.",
                        index, votes);
        auto exception = KJ_EXCEPTION(DISCONNECTED,
            "lost leadership election because a voter had a newer version of the "
            "object than the nominated leader did");
        electedFulfiller->reject(kj::cp(exception));
        kj::throwFatalException(kj::mv(exception));
      }

      #error "todo: replay missing versions"
      #error "todo: decide whether to act on staged transaction"

      if (!votes[index]) {
        votes[index] = true;
        ++voteCount;
        if (voteCount == localReplica().getSibling().getConfig().getQuorumSize()) {
          // CNN can now project that this replica will be elected next leader of the object.
          electedFulfiller->fulfill(newOwnedStorageImpl(thisCap()));
        }
      }

      return response.getFollower();
    }, [this,index,generation](kj::Exception&& e) {
      if (e.getType() == kj::Exception::Type::DISCONNECTED) {
        // Attempt reconnect.
        return addFollower(index, localReplica().getOtherReplica(index, generation + 1));
      }

      kj::throwFatalException(kj::mv(e));
    });
  }
};

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

kj::Promise<void> SiblingImpl::ReplicaImpl::getLeader(GetLeaderContext context) {
  if (state.is<LeaderImpl*>()) {
    context.getResults(capnp::MessageSize {4,1}).setLeader(state.get<LeaderImpl*>()->thisCap());
    return kj::READY_NOW;
  } else if (state.is<FollowerImpl*>()) {
    context.getResults(capnp::MessageSize {4,1}).setLeader(state.get<FollowerImpl*>()->getLeader());
    return kj::READY_NOW;
  } else {
    // Ping all other replicas for leadership status.
    auto promises = KJ_MAP(replicaRecord, getOtherReplicas()) {
      auto replica = kj::mv(replicaRecord.cap);
      auto req = replica.getLeaderStatusRequest();
      return req.send().then([replica](auto&& response) mutable {
        return LeaderStatus {
          replica,
          response.getVersion(),
          true,
          response.getIsLeading()
        };
      }, [replica](kj::Exception&& e) mutable {
        if (e.getType() == kj::Exception::Type::DISCONNECTED ||
            e.getType() == kj::Exception::Type::OVERLOADED) {
          return LeaderStatus { replica, 0, false, false };
        } else {
          kj::throwFatalException(kj::mv(e));
        }
      });
    };

    // TODO(now): Only wait for a quorum, plus maybe some timeout thereafter, so that a dead node
    //   doesn't block progress. Maybe getLeaderStatus() should return the replica's current leader
    //   capability...
    return kj::joinPromises(kj::mv(promises))
        .then([this,context](kj::Array<LeaderStatus> results) mutable
           -> kj::Promise<void> {
      if (state.is<LeaderImpl*>() || state.is<FollowerImpl*>()) {
        // Oh, things resolved while we were waiting. Never mind.
        return getLeader(context);
      }

      uint64_t version = getVersion();

      LeaderStatus newestReplica = {thisCap(), version, true, false};
      // The replica that has the highest version number, among all replicas. Prefer ourselves,
      // otherwise prefer the lowest-numbered replica as a tiebreaker.
      //
      // TODO(perf): Prefer the least-loaded replica as a tiebreaker.

      LeaderStatus newestLeader = {nullptr, newestReplica.version - 1, false, false};
      // The replica that has the highest version number and claims to be a leader, among all
      // replicas at least as new as us.

      uint availableCount = 1;
      for (auto& result: results) {
        availableCount += result.isAvailable;
        if (result.isLeading && result.version > newestLeader.version) {
          newestLeader = result;
        }
        if (result.version > newestReplica.version) {
          newestReplica = kj::mv(result);
        }
      }

      // If some other replica claimed to be leading and has a sufficiently new version, defer
      // to it.
      if (newestLeader.isLeading) {
        return context.tailCall(newestLeader.replica.getLeaderRequest(capnp::MessageSize {2,0}));
      }

      // If some other replica claimed to be a newer version, defer to it.
      if (newestReplica.version > version) {
        return context.tailCall(newestReplica.replica.getLeaderRequest(capnp::MessageSize {2,0}));
      }

      // There is no leader, and we have the newest version, so become the leader ourselves.
      context.getResults(capnp::MessageSize {4,1}).setLeader(kj::heap<LeaderImpl>(*this));
      return kj::READY_NOW;
    });
  }
}

} // namespace storage
} // namespace blackrock

