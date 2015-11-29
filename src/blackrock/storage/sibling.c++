// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "sibling.h"
#include <kj/debug.h>
#include <capnp/message.h>
#include "mid-level-object.h"
#include "follower.h"
#include "leader.h"
#include <capnp/serialize.h>

namespace blackrock {
namespace storage {

static kj::Promise<kj::Array<capnp::word>> readAll(BlobLayer::Content& content) {
  auto buffer = kj::heapArray<capnp::word>(content.getSize().endMarker / sizeof(capnp::word));
  auto promise = content.read(0, buffer.asBytes());
  return promise.then([KJ_MVCAP(buffer)]() mutable {
    return kj::mv(buffer);
  });
}

class SiblingImpl::ReplicaImpl final: public Replica::Server {
public:
  ReplicaImpl(SiblingImpl& sibling, ObjectId id, uint64_t stateRecoveryId)
      : sibling(sibling),
        siblingCap(sibling.thisCap()),
        id(id),
        ready(sibling.journal.openObject(id)
            .then([this,stateRecoveryId](kj::Maybe<kj::Own<JournalLayer::Object>> inner) {
          KJ_IF_MAYBE(o, inner) {
            // Exists, clean.
            objectWhenReady.emplace(this->sibling.journal, this->id,
                                    kj::mv(*o), stateRecoveryId);
          } else {
            // Doesn't exist.
            objectWhenReady.emplace(this->sibling.journal, this->id, stateRecoveryId);
          }
        }).fork()) {
    // Open a clean object, or create a new object.

    KJ_ASSERT(sibling.replicas.insert(std::make_pair(id, this)).second);
  }

  ReplicaImpl(SiblingImpl& sibling, RecoveredObject&& obj)
      : sibling(sibling),
        siblingCap(sibling.thisCap()),
        id(obj.id),
        ready(readAll(obj.state->getContent())
            .then([this,KJ_MVCAP(obj)](auto&& extendedState) mutable {
          objectWhenReady.emplace(this->sibling.journal, obj.id,
                                  kj::mv(obj.object), kj::mv(obj.state),
                                  kj::mv(extendedState));
        }).fork()) {
    // Recover an unclean object (one which still has a temporary state file).

    KJ_ASSERT(sibling.replicas.insert(std::make_pair(id, this)).second);
  }

  ~ReplicaImpl() noexcept(false) {
    KJ_ASSERT(sibling.replicas.erase(id) == 1) { break; }
  }

  void cleanup(uint retryCount = 0) {
    // Schedules background tasks which attempt to clean-shutdown the object, such that all
    // replicas can delete their object states.

    KJ_IF_MAYBE(object, objectWhenReady) {
      if (!object->isClean()) {
        // Not clean. Elect a leader -- requiring unanimity -- and then immediately drop it. If
        // successful, the leader will complete all pending cleanup.
        KJ_LOG(WARNING, "object not cleanly shut down", id);
        auto self = thisCap();
        sibling.tasks.add(getLeaderImpl(*object, true).whenResolved()
            .catch_([this,KJ_MVCAP(self),retryCount](kj::Exception&& exception) mutable {
          // Awkwardly, leader election failed. This is unusual; if replicas are unavailable we
          // should have been blocked waiting for the election.
          KJ_LOG(ERROR, "failed to elect leader for replica cleanup, retrying later",
                        id, exception);

          return sibling.timer.afterDelay((2 << retryCount) * kj::SECONDS)
              .then([this,KJ_MVCAP(self),retryCount]() { cleanup(retryCount + 1); });
        }));
      }
    } else {
      // Wait until ready, then try again.
      auto self = thisCap();
      sibling.tasks.add(ready.addBranch().then([this,KJ_MVCAP(self)]() { cleanup(); }));
    }
  }

  using Replica::Server::thisCap;

protected:
  kj::Promise<void> getState(GetStateContext context) override {
    KJ_IF_MAYBE(object, objectWhenReady) {
      context.releaseParams();
      auto results = context.getResults();
      sibling.getTimeImpl(results.initTime());
      if (object->exists()) {
        auto state = results.initDataState().initExists();
        auto xattr = object->getXattr();
        auto extendedState = object->getExtendedState();

        state.setType(static_cast<uint8_t>(xattr.type));
        state.setIsReadOnly(xattr.readOnly);
        state.setIsDirty(xattr.dirty);
        state.setIsTransitiveSizeDirty(xattr.dirtyTransitiveSize);
        state.setIsPendingRemoval(xattr.pendingRemoval);
        state.setVersion(xattr.version);
        state.setTransitiveBlockCount(xattr.transitiveBlockCount);
        if (xattr.owner != nullptr) {
          xattr.owner.copyTo(state.initOwner());
        }

        if (extendedState != nullptr) {
          capnp::FlatArrayMessageReader reader(extendedState);
          state.setExtended(reader.getRoot<ReplicaState::Extended>());
        }
      } else {
        results.initDataState().setDoesntExist();
      }

      KJ_IF_MAYBE(v, voter) {
        KJ_IF_MAYBE(l, v->getLeader()) {
          auto following = results.initFollowState().initFollowing();
          following.setLeader(kj::mv(l->cap));
          following.setTerm(l->term);
        } else {
          results.initFollowState().setIdle(v->thisCap());
        }
      } else {
        results.initFollowState().setIdle(kj::heap<VoterImpl>(*object,
            kj::heap<Cleaner>(*this, thisCap()), voter));
      }

      return kj::READY_NOW;
    } else {
      // Not ready.
      return ready.addBranch().then([this,context]() mutable {
        return getState(context);
      });
    }
  }

  kj::Promise<void> getLeader(GetLeaderContext context) override {
    KJ_IF_MAYBE(object, objectWhenReady) {
      context.releaseParams();
      context.getResults(capnp::MessageSize {4, 1}).setLeader(getLeaderImpl(*object, false));
      return kj::READY_NOW;
    } else {
      // Not ready.
      return ready.addBranch().then([this,context]() mutable {
        return getLeader(context);
      });
    }
  }

private:
  class Election;

  SiblingImpl& sibling;
  Sibling::Client siblingCap;
  ObjectId id;
  kj::Maybe<MidLevelObject> objectWhenReady;
  kj::Maybe<VoterImpl&> voter;
  kj::ForkedPromise<void> ready;
  kj::Maybe<Election&> currentElection;
  uint cleanerCount = 0;

  Leader::Client getLeaderImpl(MidLevelObject& object, bool forCleanup) {
    // Check if we're locally aware of a leader.
    KJ_IF_MAYBE(v, voter) {
      KJ_IF_MAYBE(l, v->getLeader()) {
        return l->cap.getRequest(capnp::MessageSize {4, 0}).send().getLeader();
      }
    }

    // No leader here. Start an election.

    // For cleanup, we need unanimity. Otherwise we need a regular quorum to elect a leader.
    auto distribution = sibling.distributor.getDistribution(id);
    uint quorumSize = forCleanup ? distribution.size() :
        sibling.getConfig().getElectionQuorumSize();

    KJ_IF_MAYBE(e, currentElection) {
      // Oh, we're already holding an election. No use starting another one.
      if (e->getQuorumSize() > quorumSize) {
        // It looks like the in-progress election was called looking for a larger quorum. Probably,
        // the reason we caught it in-progress is because it is blocked waiting for some replica to
        // become available. Let's cancel it and start our own.
        e->cancel();
      } else {
        return e->wait();
      }
    }

    auto election = kj::refcounted<Election>(*this, object, thisCap(), quorumSize);

    // Note that we intentionally call ourselves in addition to other replicas.
    for (uint siblingId: distribution) {
      auto req = sibling.getSibling(siblingId).getReplicaRequest(capnp::MessageSize {8,0});
      id.copyTo(req.initId());
      election->addReplica(req.send().getReplica());
    }
    return election->wait();
  }

  class Election final: public kj::Refcounted, private kj::TaskSet::ErrorHandler {
  public:
    Election(ReplicaImpl& replica, MidLevelObject& object, Replica::Client replicaCap,
             uint quorumSize,
             kj::PromiseFulfillerPair<Leader::Client> leaderPaf =
                 kj::newPromiseAndFulfiller<Leader::Client>())
        : replica(replica), object(object), replicaCap(kj::mv(replicaCap)), quorumSize(quorumSize),
          leaderFulfiller(kj::mv(leaderPaf.fulfiller)), leaderPromise(leaderPaf.promise.fork()),
          tasks(*this) {
      KJ_REQUIRE(replica.currentElection == nullptr);
      replica.currentElection = *this;
    }

    ~Election() noexcept(false) {
      cancel();
    }

    void addReplica(Replica::Client otherReplica) {
      ++totalReplicas;

      tasks.add(otherReplica.getStateRequest(capnp::MessageSize {4,0}).send()
         .then([this](auto&& response) {
        voters.add(kj::mv(response));

        if (--waitingCount == 0) {
          finalize();
        } else if (voters.size() == quorumSize) {
          // We have all the voters we need, but we're still waiting on some other replicas. Larger
          // quorums are better, so give the other replicas a chance to reply.
          auto& timer = replica.sibling.timer;
          tasks.add(timer
              .afterDelay(replica.sibling.getConfig().getStragglerTimeoutMs() * kj::MILLISECONDS)
              .then([this]() { finalize(); }));
        }
      }, [this](kj::Exception&& e) {
        if (sampleException == nullptr) sampleException = kj::mv(e);
        if (--waitingCount == 0) {
          finalize();
        }
      }));
    }

    kj::Promise<Leader::Client> wait() {
      return leaderPromise.addBranch().attach(kj::addRef(*this));
    }

    uint getQuorumSize() { return quorumSize; }

    void cancel() {
      if (!canceled) {
        replica.currentElection = nullptr;
        leaderFulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "election canceled"));
        canceled = true;
      }
    }

  private:
    ReplicaImpl& replica;
    MidLevelObject& object;
    Replica::Client replicaCap;
    uint quorumSize;
    uint waitingCount = 0;
    uint totalReplicas = 0;
    bool canceled = false;

    kj::Vector<capnp::Response<Replica::GetStateResults>> voters;
    kj::Maybe<kj::Exception> sampleException;

    kj::Own<kj::PromiseFulfiller<Leader::Client>> leaderFulfiller;
    kj::ForkedPromise<Leader::Client> leaderPromise;

    kj::TaskSet tasks;

    void finalize() {
      // Create the leader and fulfill the wait() promise.

      // Use isWaiting() to verify we didn't already finalize.
      if (leaderFulfiller->isWaiting()) {
        if (voters.size() < quorumSize) {
          // We didn't get a quorum. Some replicas must have thrown exceptions.
          leaderFulfiller->reject(kj::mv(KJ_ASSERT_NONNULL(sampleException)));
        } else {
          bool isUnanimous = voters.size() == totalReplicas;
          leaderFulfiller->fulfill(kj::heap<LeaderImpl>(
              replica.sibling.timer, replica.sibling.id,
              replica.sibling.getConfig().getWriteQuorumSize(),
              object, replicaCap, voters.releaseAsArray(), isUnanimous));
        }
      }
    }

    void taskFailed(kj::Exception&& exception) override {
      leaderFulfiller->reject(kj::mv(exception));
    }
  };

  class Cleaner final: public capnp::Capability::Server {
    // Hacky capability that calls cleanup() on the replica when dropped. The only reason this
    // implements Capability is so that it can be passed as `capToHold` to e.g. VoterImpl. The only
    // reason we don't pass the ReplicaImpl itself as the capability is because we need to get
    // notification that the cap is being dropped without ReplicaImpl's own destructor being
    // called.

  public:
    Cleaner(ReplicaImpl& replica, Replica::Client replicaCap)
        : replica(replica), replicaCap(kj::mv(replicaCap)) {
      ++replica.cleanerCount;
    }

    ~Cleaner() noexcept(false) {
      if (--replica.cleanerCount == 0) {
        replica.cleanup();
      }
    }

    kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
        capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
      KJ_UNIMPLEMENTED("no methods");
    }

  private:
    ReplicaImpl& replica;
    Replica::Client replicaCap;
  };
};

// =======================================================================================

class SiblingImpl::BackendSetImpl: public BackendSet<Sibling>::Server {
public:
  BackendSetImpl(SiblingImpl& sibling, Sibling::Client siblingCap)
      : sibling(sibling), siblingCap(kj::mv(siblingCap)) {}

protected:
  kj::Promise<void> reset(ResetContext context) override {
    ++sibling.backendSetResetCount;

    // Clear all existing siblings.
    for (auto& other: sibling.siblings) {
      other.reset();
    }
    sibling.siblingBackendIds.clear();

    for (auto backend: context.getParams().getBackends()) {
      addImpl(backend.getId(), backend.getShardId(), backend.getBackend());
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> add(AddContext context) override {
    auto params = context.getParams();
    addImpl(params.getId(), params.getShardId(), params.getBackend());
    return kj::READY_NOW;
  }

  kj::Promise<void> remove(RemoveContext context) override {
    auto backendId = context.getParams().getId();
    auto iter = sibling.siblingBackendIds.find(backendId);
    KJ_REQUIRE(iter != sibling.siblingBackendIds.end());

    if (backendId == sibling.siblings[iter->second].backendId) {
      sibling.siblings[iter->second].reset();
    }
    sibling.siblingBackendIds.erase(iter);

    return kj::READY_NOW;
  }

private:
  SiblingImpl& sibling;
  Sibling::Client siblingCap;

  void addImpl(uint64_t backendId, uint shardId, Sibling::Client cap) {
    KJ_REQUIRE(sibling.siblingBackendIds.insert(std::make_pair(backendId, shardId)).second);

    if (shardId >= sibling.siblings.size()) {
      sibling.siblings.resize(shardId + 1);
    }

    sibling.siblings[shardId].fulfiller->fulfill(kj::cp(cap));
    sibling.siblings[shardId].cap = kj::mv(cap);
  }
};

// =======================================================================================

class SiblingImpl::ReconnectingCap: public Sibling::Server {
public:
  ReconnectingCap(SiblingImpl& localSibling, Sibling::Client localSiblingCap, uint shardId)
      : localSibling(localSibling), localSiblingCap(kj::mv(localSiblingCap)), shardId(shardId) {}

  kj::Promise<void> dispatchCall(
      uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) {
    context.allowCancellation();

    auto& other = localSibling.siblings[shardId];
    auto backendId = other.backendId;
    auto resetCount = localSibling.backendSetResetCount;

    auto params = context.getParams();
    auto req = other.cap.typelessRequest(interfaceId, methodId, params.targetSize());
    req.set(params);

    return req.send().then([this,context](auto&& response) mutable -> kj::Promise<void> {
      context.initResults(response.targetSize()).set(response);
      return kj::READY_NOW;
    }, [this,interfaceId,methodId,context,backendId,resetCount]
        (kj::Exception&& exception) mutable -> kj::Promise<void> {
      if (exception.getType() != kj::Exception::Type::DISCONNECTED) {
        return kj::mv(exception);
      }

      // If the target sibling hasn't been reset since we started the call, reset it.
      auto& other = localSibling.siblings[shardId];
      if (localSibling.backendSetResetCount == resetCount &&
          other.backendId == backendId) {
        other.reset();
      }

      // Now we can try again.
      return dispatchCall(interfaceId, methodId, context);
    });
  }

private:
  SiblingImpl& localSibling;
  Sibling::Client localSiblingCap;
  uint shardId;
};

// =======================================================================================

SiblingImpl::SiblingImpl(kj::Timer& timer, JournalLayer::Recovery& journal,
                         ObjectDistributor& distributor, uint id)
    : SiblingImpl(timer, journal, distributor, id, recoverObjects(journal)) {}

SiblingImpl::SiblingImpl(kj::Timer& timer, JournalLayer::Recovery& journalRecovery,
                         ObjectDistributor& distributor, uint id, RecoveryResult recovered)
    : timer(timer), journal(journalRecovery.finish()), distributor(distributor),
      id(id), generation(recovered.generation),
      replicaStateRecoveryIdCounter(recovered.objects.size()), tasks(*this) {
  for (auto& obj: recovered.objects) {
    auto replica = kj::heap<ReplicaImpl>(*this, kj::mv(obj));
    auto& replicaRef = *replica;
    Replica::Client client = kj::mv(replica);  // so thisCap() works in cleanup()
    replicaRef.cleanup();
  }

  KJ_IF_MAYBE(s, recovered.siblingState) {
    siblingState = kj::mv(*s);
  } else {
    JournalLayer::Transaction txn(journal);

    TemporaryXattr xattr;
    memset(&xattr, 0, sizeof(xattr));
    siblingState = txn.createRecoverableTemporary(
        RecoveryId(RecoveryType::SIBLING_STATE, 0), xattr, journal.newDetachedTemporary());

    tasks.add(txn.commit());
  }
}
SiblingImpl::~SiblingImpl() noexcept(false) {
  if (!replicas.empty()) {
    // The replicas have pointers back to SiblingImpl so we'll probably crash if they still exist.
    // Abort instead.
    KJ_DEFER(abort());
    KJ_LOG(FATAL, "can't destroy SiblingImpl while replicas exist");
  }
}

BackendSet<Sibling>::Client SiblingImpl::getBackendSet() {
  return kj::heap<BackendSetImpl>(*this, thisCap());
}

kj::Promise<void> SiblingImpl::getTime(GetTimeContext context) {
  getTimeImpl(context.getResults(capnp::MessageSize {8,0}).initTime());
  return kj::READY_NOW;
}

kj::Promise<void> SiblingImpl::getReplica(GetReplicaContext context) {
  ObjectId objectId = context.getParams().getId();
  auto iter = replicas.find(objectId);
  if (iter == replicas.end()) {
    context.getResults(capnp::MessageSize {4,1}).setReplica(
        kj::heap<ReplicaImpl>(*this, objectId, replicaStateRecoveryIdCounter));
  } else {
    context.getResults(capnp::MessageSize {4,1}).setReplica(iter->second->thisCap());
  }
  return kj::READY_NOW;
}

SiblingImpl::RecoveryResult SiblingImpl::recoverObjects(JournalLayer::Recovery& recovery) {
  RecoveryResult result;

  // Recover object states.
  uint64_t recoveryIdCounter = 0;
  result.objects = KJ_MAP(recovered, recovery.recoverTemporaries(RecoveryType::OBJECT_STATE)) {
    TemporaryXattr xattr = recovered->getTemporaryXattr();
    return RecoveredObject {
      xattr.objectState.ojbectId,
      KJ_ASSERT_NONNULL(recovery.getObject(xattr.objectState.ojbectId),
                        "missing object associated with this object state!"),
      recovered->keepAs(RecoveryId(RecoveryType::OBJECT_STATE, recoveryIdCounter++))
    };
  };

  // Recover sibling state. The recovery ID indicates the generation.
  auto siblingStates = recovery.recoverTemporaries(RecoveryType::SIBLING_STATE);
  KJ_ASSERT(siblingStates.size() <= 1, "multiple sibling states recovered");

  if (siblingStates.size() == 1) {
    auto siblingState = kj::mv(siblingStates.front());
    result.generation = siblingState->getOldId().id + 1;
    result.siblingState = siblingState->keepAs(
        RecoveryId(RecoveryType::SIBLING_STATE, result.generation));
  } else {
    KJ_ASSERT(result.objects.size() == 0, "recovered object states but no sibling state");
    result.generation = 0;
  }

  return result;
}

Sibling::Client SiblingImpl::getSibling(uint id) {
  if (id == this->id) return thisCap();
  if (id >= siblings.size()) {
    siblings.resize(id + 1);
  }
  return kj::heap<ReconnectingCap>(*this, thisCap(), id);
}

StorageConfig::Reader SiblingImpl::getConfig() {
  #error todo
}

void SiblingImpl::getTimeImpl(Sibling::Time::Builder time) {
  time.setSiblingId(id);
  time.setGeneration(generation);
  time.setTick(nextTick++);
}

void SiblingImpl::taskFailed(kj::Exception&& exception) {
  #error todo
}

} // namespace storage
} // namespace blackrock

