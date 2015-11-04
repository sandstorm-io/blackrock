// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "sibling.h"
#include <kj/debug.h>

namespace blackrock {
namespace storage {

class SiblingImpl::ReplicaImpl: public Replica::Server {
protected:
  struct LeaderStatus {
    Replica::Client replica;
    uint64_t version;
    bool isAvailable;
    bool isLeading;
  };

  kj::Promise<void> getObject(GetObjectContext context) override {
    if (state.is<LeaderImpl*>()) {
      return state.get<LeaderImpl*>()->getObject();
    } else if (state.is<FollowerImpl*>()) {
      return state.get<FollowerImpl*>()->getObject();
    } else {
      // Call election.
      ObjectKey objectKey = context.getParams().getKey();

      auto replicas = getOtherReplicas();

      if (replicas.size() == 0) {
        // I'm the only replica.
        #error todo
      }

      auto promises = KJ_MAP(replica, replicas) {
        auto req = replica.getLeaderStatusRequest();
        objectKey.copyTo(req.initKey());
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

      return kj::joinPromises(kj::mv(promises))
          .then([this,context,objectKey](kj::Array<LeaderStatus> results) mutable {
        if (state.is<LeaderImpl*>() || state.is<FollowerImpl*>()) {
          // Oh, things resolved while we were waiting. Never mind.
          return getObject(context);
        }

        LeaderStatus newestReplica = {thisCap(), getVersion(), true, false};
        // The replica that has the highest version number, among all replicas. Prefer ourselves,
        // otherwise prefer the lowest-numbered replica as a tiebreaker.

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

        // If a suitable leader was found, follow it.
        KJ_IF_MAYBE(leader, newestLeader.capIfLeader) {
          auto req = newestLeader.replica.getObjectRequest(capnp::MessageSize {4, 1});
          objectKey.copyTo(req.initKey());
          return context.tailCall(kj::mv(req));
        }



        uint64_t version = getVersion();

        kj::Vector<kj::Promise<void>> promises;

        uint leaderCount = 0;
        for (auto& result: results) {
          if (result.hasCapIfLeader()) {
            if
          }
        }

      });
    }
  }

  kj::Promise<void> startTransaction(StartTransactionContext context) override {
  }

  kj::Promise<void> cleanupTransaction(CleanupTransactionContext context) override {
  }

  kj::Promise<void> getTransactionState(GetTransactionStateContext context) override {
  }

  kj::Promise<void> getLeaderStatus(GetLeaderStatusContext context) override {
  }

  kj::Promise<void> lead(LeadContext context) override {
  }

  kj::Promise<void> abdicate(AbdicateContext context) override {
  }

  kj::Promise<void> follow(FollowContext context) override {
  }

private:
  Sibling::Client siblingCap;
  SiblingImpl& sibling;
  ObjectId id;
  kj::Array<uint> distribution;

  kj::Own<JournalLayer::Object> inner;

  capnp::CapabilityServerSet<capnp::Capability> objects;
  // Allows recognizing our own objects when they come back to us, mainly for implementing
  // TransactionBuilder.

  kj::OneOf<capnp::Void, LeaderImpl*, FollowerImpl*> state;
  // Current state.

  kj::Array<Replica::Client> getOtherReplicas() {
    kj::Vector<Replica::Client> result;
    for (uint siblingId: distribution) {
      if (siblingId != sibling.id) {
        auto iter = sibling.siblings.find(siblingId);
        KJ_ASSERT(iter != sibling.siblings.end());
        result.add(iter->second);
      }
    }

    KJ_ASSERT(result.size() == distribution.size() - 1);
    return result.releaseAsArray();
  }

  uint64_t getVersion() {
    return inner->getXattr().getVersion();
  }
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
protected:
  kj::Promise<void> getObject(GetObjectContext context) override {
  }

private:
  kj::Maybe<Replica&> replica;
  // If null, disconnected.
};

class SiblingImpl::FollowerImpl: public Follower::Server {
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

} // namespace storage
} // namespace blackrock

