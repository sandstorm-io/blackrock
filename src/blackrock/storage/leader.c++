// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "leader.h"
#include <kj/debug.h>

namespace blackrock {
namespace storage {

class LeaderImpl::WeakLeaderImpl final: public WeakLeader::Server, public kj::Refcounted {
public:
  WeakLeaderImpl(LeaderImpl& leader): leader(leader) {}

protected:
  kj::Promise<void> get(GetContext context) override {
    KJ_IF_MAYBE(l, leader) {
      context.getResults(capnp::MessageSize {4,1}).setLeader(l->thisCap());
      return kj::READY_NOW;
    } else {
      return KJ_EXCEPTION(DISCONNECTED, "object leader is gone");
    }
  }

private:
  kj::Maybe<LeaderImpl&> leader;

  friend class LeaderImpl;
};

class LeaderImpl::StagedTransactionImpl final: public StagedTransaction::Server {
  // Implementation of StagedTransaction that fulfills a promise.

public:
  StagedTransactionImpl(LeaderImpl& leader, kj::Array<StagedTransaction::Client> stagedFollowers)
      : leader(leader), leaderCap(leader.thisCap()), stagedFollowers(kj::mv(stagedFollowers)) {}
  ~StagedTransactionImpl() noexcept(false);

protected:
  kj::Promise<void> commit(CommitContext context) override {
    uint i = 0;
    return leader.allFollowers([&](FollowerRecord& follower) {
      return stagedFollowers[i++].commitRequest().send().then([](auto&&) {});
    });
  }

  kj::Promise<void> abort(AbortContext context) override {
    uint i = 0;
    return leader.allFollowers([&](FollowerRecord& follower) {
      return stagedFollowers[i++].abortRequest().send().then([](auto&&) {});
    });
  }

private:
  LeaderImpl& leader;
  Leader::Client leaderCap;
  kj::Array<StagedTransaction::Client> stagedFollowers;

  struct QuorumWaiter: public kj::Refcounted {
    uint needed;
    uint waiting;
  };
};

class LeaderImpl::TransactionBuilderImpl final: public TransactionBuilder::Server {
public:
  explicit TransactionBuilderImpl(LeaderImpl& leader)
      : leader(leader), leaderCap(leader.thisCap()) {}

protected:
  kj::Promise<void> getTransactional(GetTransactionalContext context) override {
    #error todo
  }

  kj::Promise<void> applyRaw(ApplyRawContext context) override {
    #error "todo: merge supplied transaction into ours"
  }

  kj::Promise<void> stage(StageContext context) override {
    auto params = context.getParams();
    ++leader.version;

    auto stagedFollowers = kj::heapArrayBuilder<StagedTransaction::Client>(leader.followers.size());

    return leader.allFollowers([&](FollowerRecord& follower) {
      auto req = follower.cap.stageDistributedRequest();
      auto txn = req.initTxn();

      txn.setCoordinator(params.getCoordinator());
      txn.setId(params.getId());
      txn.setVersion(leader.version);
      txn.setChanges(changes);

      auto promise = req.send();
      stagedFollowers.add(promise.getStaged());

      // stage() returns when a quorum of followers have accepted, so here we return a promise for
      // the follower accepting.
      //
      // Note that followers will block subsequent writes until the staged transaction goes
      // through, so there's no need for us to block in the leader.
      return promise.then([](auto&&) {});
    });

    context.getResults(capnp::MessageSize {4,1}).setStaged(
        kj::heap<StagedTransactionImpl>(leader, stagedFollowers.finish()));
  }

private:
  LeaderImpl& leader;
  Leader::Client leaderCap;

  capnp::MallocMessageBuilder arena;
  ChangeSet::Builder changes = arena.getRoot<ChangeSet>();
};

LeaderImpl::LeaderImpl(ObjectKey key, uint siblingId, uint quorumSize,
                       MidLevelReader& localObject, capnp::Capability::Client capToHold,
                       kj::Array<capnp::Response<Replica::GetStateResults>> voters)
    : key(key), id(key), siblingId(siblingId), quorumSize(quorumSize), localObject(localObject),
      capToHold(kj::mv(capToHold)), weak(kj::refcounted<WeakLeaderImpl>(*this)),
      termInfo(makeTermInfo(termInfoMessage, voters)),
      tasks(*this),
      ready(kj::joinPromises(KJ_MAP(voter, voters)
            -> kj::Promise<FollowResponseRecord> {
          // Ask the voter to follow us.
          auto req = voter.getFollowState().getIdle().voteRequest();
          req.setLeader(kj::addRef(*weak));
          req.setTerm(termInfo);
          req.setWriteQuorumSize(quorumSize);

          return req.send().then([KJ_MVCAP(voter)](auto&& response) mutable {
            return FollowResponseRecord { response.getFollower(), kj::mv(voter) };
          });
        }).then([this](kj::Array<FollowResponseRecord>&& followers) -> kj::Promise<void> {
          // All votes came back positive!

          this->followers = KJ_MAP(follower, followers) {
            return FollowerRecord {
              kj::mv(follower.cap),
              follower.state.getTime().getSiblingId()
            };
          };

          // Choose best replica.
          Replica::DataState::Reader bestState;
          kj::Vector<uint> goodReplicas(followers.size());
          kj::Vector<uint> badReplicas(followers.size());
          uint64_t maxVersion = 0;

          for (auto i: kj::indices(followers)) {
            auto state = followers[i].state.getDataState();
            if (state.isExists()) {
              maxVersion = kj::max(maxVersion, state.getExists().getVersion());
            }
            switch (compare(state, bestState)) {
              case Comparison::BETTER:
                bestState = state;
                badReplicas.addAll(goodReplicas);
                goodReplicas.resize(0);
                goodReplicas.add(i);
                break;
              case Comparison::SAME:
                goodReplicas.add(i);
                break;
              case Comparison::WORSE:
                badReplicas.add(i);
                break;
            }
          }

          KJ_ASSERT(goodReplicas.size() > 0);

          #error "todo: deal with staged transaction"

          bool dirty = bestState.isExists() && bestState.getExists().getIsDirty();
          if (badReplicas.size() == 0 && !dirty) {
            // No recovery needed.
            return kj::READY_NOW;
          }

          auto recoveryPromises = kj::heapArrayBuilder<kj::Promise<void>>(followers.size());
          uint64_t recoveryVersion = maxVersion + 1;

          // For each good replica, commit a no-op transaction to increment its version and
          // clear dirty state.
          for (auto i: goodReplicas) {
            auto req = this->followers[i].cap.commitRequest();
            req.setVersion(recoveryVersion);
            // leave transaction defaulted
            recoveryPromises.add(req.send().then([](auto&&) {}));
          }

          // For each bad replica, find a good replica and initiate a copy.
          for (auto i: kj::indices(badReplicas)) {
            auto& bad = this->followers[badReplicas[i]];
            auto& good = this->followers[goodReplicas[i % goodReplicas.size()]];

            auto req = good.cap.copyToRequest();
            req.setReplacer(bad.cap.replaceRequest().send().getReplacer());
            req.setVersion(recoveryVersion);
            recoveryPromises.add(req.send().then([](auto&&) {}));
          }
          return kj::joinPromises(recoveryPromises.finish());
        }).fork()) {}

LeaderImpl::~LeaderImpl() noexcept(false) {
  weak->leader = nullptr;
}

void LeaderImpl::abdicate() {
  weak->leader = nullptr;
  followers = nullptr;
}

void LeaderImpl::write(uint64_t offset, kj::ArrayPtr<const byte> data) {
  allFollowers([&](FollowerRecord& follower) {
    auto req = follower.cap.writeRequest();
    req.setOffset(offset);
    req.setData(data);
    return req.send().then([](auto&&) {});
  });
}

kj::Promise<void> LeaderImpl::sync() {
  ++version;
  return allFollowers([&](FollowerRecord& follower) {
    auto req = follower.cap.syncRequest();
    req.setVersion(version);
    return req.send().then([](auto&&) {});
  });
}

kj::Promise<void> LeaderImpl::modify(ChangeSet::Reader changes) {
  ++version;
  return allFollowers([&](FollowerRecord& follower) {
    auto req = follower.cap.commitRequest();
    req.setVersion(version);
    req.setChanges(changes);
    return req.send().then([](auto&&) {});
  });
}

kj::Own<MidLevelWriter::Replacer> LeaderImpl::startReplace() {
  KJ_UNIMPLEMENTED("startReplace() over replication not implemented");
}

kj::Promise<void> LeaderImpl::getObject(GetObjectContext context) {
  ObjectKey requestKey = context.getParams().getKey();
  if (requestKey != key) {
    // This check should only be possible to fail if there is a malicious (or very confused)
    // client directly accessing the storage-sibling API.
    KJ_LOG(ERROR, "SECURITY: detected attempt to access storage object with wrong key");
    KJ_FAIL_REQUIRE("wrong key");
  }

  // Wait for election / most recent op to finish before creating object.
  return ready.addBranch().then([this,context]() mutable {
    auto results = context.getResults(capnp::MessageSize {4,1});

    KJ_IF_MAYBE(o, weakObject) {
      results.setCap(o->thisCap());
    } else {
      results.setCap(makeHighLevelObject(localObject, *this, thisCap(), weakObject));
    }
  });
}

kj::Promise<void> LeaderImpl::startTransaction(StartTransactionContext context) {
  context.getResults(capnp::MessageSize {4,1})
      .setBuilder(kj::heap<TransactionBuilderImpl>(*this));
  return kj::READY_NOW;
}

kj::Promise<void> LeaderImpl::flushTransaction(FlushTransactionContext context) {
  // Any time we reach "ready", all previous transactions have been dealt with, so all we really
  // need todo here is wait for that.
  return ready.addBranch();
}

kj::Promise<void> LeaderImpl::getTransactionState(GetTransactionStateContext context) {
  #error todo
}

kj::Promise<void> LeaderImpl::allFollowers(
    kj::Function<kj::Promise<void>(FollowerRecord&)>&& func) {
  // Call the given function -- synchronously, right now -- on all followers. The returned
  // promise completes when a quorum of followers that includes the local follower have
  // completed.

  if (weak->leader == nullptr) {
    return KJ_EXCEPTION(DISCONNECTED, "object leader has abdicated");
  }

  auto waiter = kj::refcounted<QuorumWaiter>();

  auto paf = kj::newPromiseAndFulfiller<void>();
  waiter->fulfiller = kj::mv(paf.fulfiller);
  for (auto& follower: followers) {
    auto& waiterRef = *waiter;

    ++waiter->waitingCount;
    tasks.add(func(follower).then([this,&waiterRef,&follower]() mutable {
      if (weak->leader == nullptr) {
        waiterRef.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "object leader has abdicated"));
        return;
      }

      KJ_ASSERT(waiterRef.waitingCount > 0);
      --waiterRef.waitingCount;

      ++waiterRef.successCount;
      if (follower.siblingId == siblingId) {
        waiterRef.sawSelf = true;
      }

      if (waiterRef.successCount == quorumSize && waiterRef.sawSelf) {
        waiterRef.fulfiller->fulfill();
      } else if (waiterRef.waitingCount == 0) {
        // Eek we've lost our quorum.
        abdicate();
        waiterRef.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "leader lost quorum"));
      }
    }, [this,&waiterRef,&follower](kj::Exception&& e) {
      if (weak->leader == nullptr) {
        waiterRef.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "object leader has abdicated"));
        return;
      }

      KJ_ASSERT(waiterRef.waitingCount > 0);
      --waiterRef.waitingCount;

      if (e.getType() == kj::Exception::Type::DISCONNECTED) {
        // This follower disconnected.
        if (follower.siblingId == siblingId ||
            waiterRef.successCount + waiterRef.waitingCount < quorumSize) {
          // We've lost our quorum, or lost connection to the local replica. Give up.
          abdicate();
          waiterRef.fulfiller->reject(KJ_EXCEPTION(
              DISCONNECTED, "local replica disconnected from leader"));
          KJ_LOG(WARNING, "quorum lost", id);
        } else {
          // Ignore. This follower will continuously throw disconnected exceptions, but oh well.
        }
      } else {
        // Pass through non-disconnected exceptions.
        waiterRef.fulfiller->reject(kj::mv(e));
      }
    }).attach(kj::addRef(*waiter)));
  }

  // Delay getObject() until last queued op completes.
  ready = paf.promise.fork();
  return ready.addBranch();
}

void LeaderImpl::taskFailed(kj::Exception&& exception) {
  if (exception.getType() != kj::Exception::Type::DISCONNECTED) {
    KJ_LOG(ERROR, exception);
  }
}

TermInfo::Reader LeaderImpl::makeTermInfo(capnp::MessageBuilder& arena,
    kj::ArrayPtr<capnp::Response<Replica::GetStateResults>> voters) {
  auto builder = arena.getRoot<TermInfo>();
  auto times = builder.initStartTime(voters.size());
  for (auto i: kj::indices(voters)) {
    times.setWithCaveats(i, voters[i].getTime());
  }

  return builder;
}

LeaderImpl::Comparison LeaderImpl::compare(
    Replica::DataState::Reader left, Replica::DataState::Reader right) {
  // Existence is better than non-existence. If neither exists then they are "same".
  if (left.isDoesntExist()) {
    return right.isDoesntExist() ? Comparison::SAME : Comparison::WORSE;
  } else if (right.isDoesntExist()) {
    return Comparison::BETTER;
  }

  // Both exist.
  auto lefte = left.getExists();
  auto righte = right.getExists();

  // Check terms.
  Comparison termComparison = compare(lefte.getExtended().getTerm(),
                                      righte.getExtended().getTerm());
  if (termComparison != Comparison::SAME) return termComparison;

  // Same term. Check versions.
  if (lefte.getVersion() < righte.getVersion()) {
    return Comparison::WORSE;
  } else if (lefte.getVersion() > righte.getVersion()) {
    return Comparison::BETTER;
  }

  // Same version. Check dirtiness. Note that two dirty states are never "same"; if both are
  // dirty, then we can arbitrarily choose one or the other, so we default to "worse".
  if (lefte.getIsDirty()) {
    return Comparison::WORSE;
  } else if (righte.getIsDirty()) {
    return Comparison::BETTER;
  }

  // Same version, neither is dirty.
  return Comparison::SAME;
}

LeaderImpl::Comparison LeaderImpl::compare(TermInfo::Reader left, TermInfo::Reader right) {
  kj::Maybe<Comparison> result;

  for (auto lt: left.getStartTime()) {
    for (auto rt: right.getStartTime()) {
      if (lt.getSiblingId() == rt.getSiblingId()) {
        // Compare times from same sibling.
        Comparison c = Comparison::SAME;

        if (lt.getGeneration() < rt.getGeneration()) {
          // Left is from older generation.
          c = Comparison::WORSE;
        } else if (lt.getGeneration() > rt.getGeneration()) {
          // Left is from newer generation.
          c = Comparison::BETTER;
        } else if (lt.getTick() < rt.getTick()) {
          // Left is from older tick of same generation.
          c = Comparison::WORSE;
        } else if (lt.getTick() > rt.getTick()) {
          // Left is from newer tick of same generation.
          c = Comparison::BETTER;
        }

        KJ_IF_MAYBE(r, result) {
          // Verify consistency.
          KJ_REQUIRE(c == *r, "inconsistent vector times -- can't determine newer");
        } else {
          result = c;
        }
      }
    }
  }

  return KJ_REQUIRE_NONNULL(result, "vector times have no overlap -- can't determine newer");
}

} // namespace storage
} // namespace blackrock

