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

  void cleanup() {
    #error todo
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
        auto basicState = object->getState();
        auto extendedState = object->getExtendedState();

        state.setType(static_cast<uint8_t>(xattr.type));
        state.setIsReadOnly(xattr.readOnly);
        state.setIsDirty(xattr.dirty);
        state.setVersion(xattr.version);
        state.setTransitiveBlockCount(xattr.transitiveBlockCount);
        if (xattr.owner != nullptr) {
          xattr.owner.copyTo(state.initOwner());
        }

        state.setPendingRemoval(basicState.objectState.remove);
        state.setPendingSizeUpdate(basicState.objectState.blockCountDelta);

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
        results.initFollowState().setIdle(kj::heap<VoterImpl>(*object, thisCap(), voter));
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

      // Check if we're locally aware of a leader.
      KJ_IF_MAYBE(v, voter) {
        KJ_IF_MAYBE(l, v->getLeader()) {
          context.getResults(capnp::MessageSize {4, 1})
              .setLeader(l->cap.getRequest(capnp::MessageSize {4, 0}).send().getLeader());
          return kj::READY_NOW;
        }
      }

      // No leader here. Start an election.
      //
      // Note that we intentionally call ourselves in addition to other replicas.
      auto election = kj::heap<Election>(*this, *object, thisCap());
      for (uint siblingId: sibling.distributor.getDistribution(id)) {
        auto req = sibling.getSibling(siblingId).getReplicaRequest(capnp::MessageSize {8,0});
        id.copyTo(req.initId());
        election->addReplica(req.send().getReplica());
      }

      auto promise = election->wait();
      context.getResults(capnp::MessageSize {4, 1}).setLeader(promise.attach(kj::mv(election)));
      return kj::READY_NOW;
    } else {
      // Not ready.
      return ready.addBranch().then([this,context]() mutable {
        return getLeader(context);
      });
    }
  }

private:
  SiblingImpl& sibling;
  Sibling::Client siblingCap;
  ObjectId id;
  kj::Maybe<MidLevelObject> objectWhenReady;
  kj::Maybe<VoterImpl&> voter;
  kj::ForkedPromise<void> ready;

  class Election final: private kj::TaskSet::ErrorHandler {
  public:
    Election(ReplicaImpl& replica, MidLevelObject& object, Replica::Client replicaCap)
        : replica(replica), object(object), replicaCap(kj::mv(replicaCap)),
          quorumSize(replica.sibling.getConfig().getElectionQuorumSize()),
          tasks(*this) {}

    void addReplica(Replica::Client otherReplica) {
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
      return kj::mv(leaderPaf.promise);
    }

  private:
    ReplicaImpl& replica;
    MidLevelObject& object;
    Replica::Client replicaCap;
    uint quorumSize;
    uint waitingCount = 0;

    kj::Vector<capnp::Response<Replica::GetStateResults>> voters;
    kj::Maybe<kj::Exception> sampleException;

    kj::PromiseFulfillerPair<Leader::Client> leaderPaf =
        kj::newPromiseAndFulfiller<Leader::Client>();

    kj::TaskSet tasks;

    void finalize() {
      // Create the leader and fulfill the wait() promise.

      // Use isWaiting() to verify we didn't already finalize.
      if (leaderPaf.fulfiller->isWaiting()) {
        if (voters.size() < quorumSize) {
          // We didn't get a quorum. Some replicas must have thrown exceptions.
          leaderPaf.fulfiller->reject(kj::mv(KJ_ASSERT_NONNULL(sampleException)));
        } else {
          leaderPaf.fulfiller->fulfill(kj::heap<LeaderImpl>(replica.sibling.id,
              replica.sibling.getConfig().getWriteQuorumSize(),
              object, replicaCap, voters.releaseAsArray()));
        }
      }
    }

    void taskFailed(kj::Exception&& exception) override {
      leaderPaf.fulfiller->reject(kj::mv(exception));
    }
  };
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
    // This will probably crash, so abort.
    KJ_LOG(FATAL, "can't destroy SiblingImpl while replicas exist");
    abort();
  }
}

void SiblingImpl::setSibling(uint id, Sibling::Client cap) {
  #error "todo figure this out"

  auto insertResult = siblings.insert(std::make_pair(id, SiblingRecord(cap, 0)));
  if (!insertResult.second) {
    insertResult.first->second.cap = kj::mv(cap);
    ++insertResult.first->second.connectionNumber;
  }
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
  auto iter = siblings.find(id);
  KJ_REQUIRE(iter != siblings.end());
  return iter->second.cap;
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

