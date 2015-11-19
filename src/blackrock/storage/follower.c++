// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "follower.h"
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/debug.h>

namespace blackrock {
namespace storage {

class FollowerImpl::StagedTransactionImpl final: public StagedTransaction::Server {
public:
  StagedTransactionImpl(kj::Own<kj::PromiseFulfiller<bool>> fulfiller)
      : fulfiller(kj::mv(fulfiller)) {}
  ~StagedTransactionImpl() noexcept(false) {
    if (fulfiller->isWaiting()) {
      fulfiller->reject(KJ_EXCEPTION(DISCONNECTED,
          "staged transaction was neither committed nor aborted before being dropped"));
    }
  }

protected:
  kj::Promise<void> commit(CommitContext context) override {
    if (!fulfiller->isWaiting()) {
      return KJ_EXCEPTION(DISCONNECTED, "staged transaction is disconnected");
    }

    fulfiller->fulfill(true);
    return kj::READY_NOW;
  }

  kj::Promise<void> abort(AbortContext context) override {
    if (!fulfiller->isWaiting()) {
      return KJ_EXCEPTION(DISCONNECTED, "staged transaction is disconnected");
    }

    fulfiller->fulfill(false);
    return kj::READY_NOW;
  }

private:
  kj::Own<kj::PromiseFulfiller<bool>> fulfiller;
};

FollowerImpl::FollowerImpl(MidLevelObject& object, capnp::Capability::Client capToHold,
                           kj::Maybe<FollowerImpl&>& weakref, WeakLeader::Client leader)
    : state(State {object, kj::mv(capToHold), weakref, kj::mv(leader), kj::READY_NOW}) {
  weakref = *this;
}

FollowerImpl::~FollowerImpl() noexcept(false) {
  if (state != nullptr) disconnect();
}

kj::Promise<void> FollowerImpl::disconnect() {
  KJ_IF_MAYBE(s, state) {
    s->weakref = nullptr;
    auto result = kj::mv(s->writeQueue);
    state = nullptr;
    return kj::mv(result);
  } else {
    return kj::READY_NOW;
  }
}

WeakLeader::Client FollowerImpl::getLeader() {
  return getState().leader;
}

kj::Promise<void> FollowerImpl::commit(CommitContext context) {
  return queueOp([context](MidLevelObject& object) mutable {
    auto params = context.getParams();
    object.setNextVersion(params.getVersion());
    return object.modify(params.getChanges());
  });
}

kj::Promise<void> FollowerImpl::stageDistributed(StageDistributedContext context) {
  auto params = context.getParams();
  auto txnCopy = kj::heap<capnp::MallocMessageBuilder>(params.totalSize().wordCount + 8);
  txnCopy->setRoot(params.getTxn());
  context.releaseParams();

  auto paf = kj::newPromiseAndFulfiller<bool>();
  auto promise = kj::mv(paf.promise);
  context.getResults(capnp::MessageSize {4, 1})
      .setStaged(kj::heap<StagedTransactionImpl>(kj::mv(paf.fulfiller)));

  // Queue an op to save the staged transaction into our extended state.
  auto txnRef = txnCopy->getRoot<DistributedTransactionInfo>().asReader();
  auto result = queueOp([txnRef](MidLevelObject& object) {
    capnp::MallocMessageBuilder builder;
    capnp::FlatArrayMessageReader reader(object.getExtendedState());
    builder.setRoot(reader.getRoot<ReplicaState::Extended>());
    builder.getRoot<ReplicaState::Extended>().setStaged(txnRef);
    return object.modifyExtendedState(capnp::messageToFlatArray(builder));
  });

  // Queue a second op to actually commit the transaction.
  queueOp([KJ_MVCAP(txnCopy),KJ_MVCAP(promise)](MidLevelObject& object) mutable {
    // We must block writes until the transaction commits or aborts.
    return promise.then([KJ_MVCAP(txnCopy),&object](bool committed) mutable {
      // Remove the transaction from the extended state.
      capnp::MallocMessageBuilder builder;
      capnp::FlatArrayMessageReader reader(object.getExtendedState());
      builder.setRoot(reader.getRoot<ReplicaState::Extended>());
      builder.getRoot<ReplicaState::Extended>().disownStaged();
      kj::Array<capnp::word> newExState;
      if (object.getExtendedState().size() <= BLOCK_SIZE) {
        newExState = capnp::messageToFlatArray(builder);
      } else {
        // The state with the transaction was more than one block, meaning it is probably
        // worthwhile for us to do an extra copy to drop back down to one block.
        capnp::MallocMessageBuilder builder2;
        builder2.setRoot(builder.getRoot<ReplicaState::Extended>());
        newExState = capnp::messageToFlatArray(builder2);
      }

      if (committed) {
        auto txn = txnCopy->getRoot<DistributedTransactionInfo>().asReader();
        object.setNextVersion(txn.getVersion());
        object.setNextExtendedState(kj::mv(newExState));
        return object.modify(txn.getChanges());
      } else {
        return object.modifyExtendedState(kj::mv(newExState));
      }
    });
  });

  return kj::mv(result);
}

kj::Promise<void> FollowerImpl::write(WriteContext context) {
  return queueOp([context](MidLevelObject& object) mutable {
    return object.setDirty().then([context,&object]() mutable {
      auto params = context.getParams();
      object.write(params.getOffset(), params.getData());
    });
  });
}

kj::Promise<void> FollowerImpl::sync(SyncContext context) {
  uint64_t version = context.getParams().getVersion();
  context.releaseParams();

  // There's no need for a sync() to block subsequent operations while we wait for it to
  // complete, but we do want to block return from this method until the sync completes. The
  // result is a weird construction where the queued-op stores a promise for the result of sync()
  // off to the side, and then it is picked up in the continuation which only applies to
  // the return branch. Note that using a PromiseFulfillerPair here would be a bad idea as it
  // would not properly propagate the exception in the case that writeQueue is broken.
  auto promise = kj::heap<kj::Promise<void>>(nullptr);
  auto& promiseRef = *promise;
  return queueOp([version,&promiseRef](MidLevelObject& object) mutable {
    object.setNextVersion(version);
    promiseRef = object.sync();
    return kj::READY_NOW;
  }).then([KJ_MVCAP(promise)]() mutable {
    // This slightly-weird construction is so that if
    return kj::mv(*promise);
  });
}

kj::Promise<void> FollowerImpl::replace(ReplaceContext context) {
  #error todo
}

kj::Promise<void> FollowerImpl::copyTo(CopyToContext context) {
  context.allowCancellation();
  #error todo
}

FollowerImpl::State& FollowerImpl::getState() {
  KJ_IF_MAYBE(s, state) {
    return *s;
  } else {
    kj::throwFatalException(KJ_EXCEPTION(DISCONNECTED, "follower disconnected"));
  }
}

kj::Promise<void> FollowerImpl::queueOp(
    kj::Function<kj::Promise<void>(MidLevelObject& object)>&& func) {
  auto& state = getState();
  auto& writeQueue = state.writeQueue;
  auto& object = state.object;
  auto forked = writeQueue.then([KJ_MVCAP(func),&object]() mutable {
    func(object);
  }).fork();
  writeQueue = forked.addBranch();
  return forked.addBranch();
}

// =======================================================================================

VoterImpl::VoterImpl(MidLevelObject& object, capnp::Capability::Client capToHold,
                     kj::Maybe<VoterImpl&>& weakref)
    : state(State {object, kj::mv(capToHold), weakref, nullptr}) {
  weakref = *this;
}

VoterImpl::~VoterImpl() noexcept(false) {
  if (state != nullptr) disconnect();
}

kj::Promise<void> VoterImpl::disconnect() {
  KJ_IF_MAYBE(s, state) {
    s->weakref = nullptr;
    kj::Maybe<kj::Promise<void>> result = s->follower.map([](auto& f) { return f.disconnect(); });
    state = nullptr;
    KJ_IF_MAYBE(r, result) {
      return kj::mv(*r);
    }
  }

  return kj::READY_NOW;
}

kj::Maybe<WeakLeader::Client> VoterImpl::getLeader() {
  return getState().follower.map([](auto& f) { return f.getLeader(); });
}

kj::Promise<void> VoterImpl::vote(VoteContext context) {
  auto params = context.getParams();

  auto& state = getState();
  if (state.follower != nullptr) {
    return KJ_EXCEPTION(DISCONNECTED, "already voted");
  }

  {
    // Update extended state.
    capnp::MallocMessageBuilder scratch;

    {
      capnp::FlatArrayMessageReader reader(state.object.getExtendedState());
      scratch.setRoot(reader.getRoot<ReplicaState::Extended>());
    }

    // Update extended state as appropriate.
    auto exState = scratch.getRoot<ReplicaState::Extended>();

    exState.setTerm(params.getTerm());
    exState.setEffectiveWriteQuorumSize(params.getWriteQuorumSize());

    // Any staged transaction will be delt with by the leader in the first transaction.
    exState.disownStaged();

    // TODO(perf): Make another copy to free holes left by overwritten objects? As-is, those holes
    //   will at least be freed up on the next update. Ideally, POCO support in Cap'n Proto will
    //   solve everything.

    state.object.setNextExtendedState(capnp::messageToFlatArray(scratch));
  }

  context.getResults(capnp::MessageSize {4,1}).setFollower(
      kj::heap<FollowerImpl>(state.object, thisCap(), state.follower, params.getLeader()));

  return kj::READY_NOW;
}

} // namespace storage
} // namespace blackrock

