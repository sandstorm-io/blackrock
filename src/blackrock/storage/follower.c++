// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "follower.h"
#include <capnp/message.h>
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

FollowerImpl::FollowerImpl(MidLevelWriter& object, capnp::Capability::Client capToHold,
                           kj::Maybe<FollowerImpl&>& weakref, WeakLeader::Client leader)
    : state(State {object, kj::mv(capToHold), weakref, kj::mv(leader)}),
      writeQueue(kj::READY_NOW) {
  weakref = *this;
}

void FollowerImpl::disconnect() {
  KJ_IF_MAYBE(s, state) {
    s->weakref = nullptr;
  }
  state = nullptr;

  #error "this could be cyclic"
  writeQueue = nullptr;
}

kj::Promise<void> FollowerImpl::commit(CommitContext context) {
  #error "todo: if this is the first commit, must store term info"
  return queueOp([this,context]() mutable {
    auto params = context.getParams();
    auto& object = getState().object;
    object.setNextVersion(params.getVersion());
    return object.modify(params.getChanges());
  });
}

kj::Promise<void> FollowerImpl::stageDistributed(StageDistributedContext context) {
  #error "todo: if this is the first commit, must store term info"
  auto params = context.getParams();
  auto paramsCopy = kj::heap<capnp::MallocMessageBuilder>(params.totalSize().wordCount + 8);
  paramsCopy->setRoot(params);
  context.releaseParams();

  auto paf = kj::newPromiseAndFulfiller<bool>();
  auto promise = kj::mv(paf.promise);
  context.getResults(capnp::MessageSize {4, 1})
      .setStaged(kj::heap<StagedTransactionImpl>(kj::mv(paf.fulfiller)));

  #error "todo: save staged transaction in a temporary, and require it to be synced before we return"

  queueOp([this,KJ_MVCAP(paramsCopy),KJ_MVCAP(promise)]() mutable {
    // We must block writes until the transaction commits or aborts.
    return promise.then([this,KJ_MVCAP(paramsCopy)](bool committed) mutable {
      if (committed) {
        auto params = paramsCopy->getRoot<StageDistributedParams>();
        auto& object = getState().object;
        object.setNextVersion(params.getVersion());
        return object.modify(params.getChanges());
      }
    }, [this](kj::Exception&& e) -> kj::Promise<void> {
      if (e.getType() == kj::Exception::Type::DISCONNECTED) {
        disconnect();
      }
      return kj::mv(e);
    });
  });

  return kj::READY_NOW;
}

kj::Promise<void> FollowerImpl::write(WriteContext context) {
  #error "todo: if this is the first commit, must store term info"
  return queueOp([this,context]() mutable {
    return getState().object.setDirty().then([this,context]() mutable {
      auto params = context.getParams();
      getState().object.write(params.getOffset(), params.getData());
    });
  });
}

kj::Promise<void> FollowerImpl::sync(SyncContext context) {
  #error "todo: if this is the first commit, must store term info"
  auto paf = kj::newPromiseAndFulfiller<kj::Promise<void>>();
  uint64_t version = context.getParams().getVersion();
  context.releaseParams();

  auto fulfiller = kj::mv(paf.fulfiller);
  return queueOp([this,version,KJ_MVCAP(fulfiller)]() mutable {
    // There's no need for a sync() to block subsequent operations while we wait for it to
    // complete.
    auto& object = getState().object;
    object.setNextVersion(version);
    fulfiller->fulfill(object.sync());
    return kj::READY_NOW;
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

kj::Promise<void> FollowerImpl::queueOp(kj::Function<kj::Promise<void>()>&& func) {
  auto forked = writeQueue.then([KJ_MVCAP(func)]() mutable {
    return func();
  }).fork();
  writeQueue = forked.addBranch();
  return forked.addBranch();
}

} // namespace storage
} // namespace blackrock

