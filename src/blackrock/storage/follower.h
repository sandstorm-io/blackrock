// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_FOLLOWER_H_
#define BLACKROCK_STORAGE_FOLLOWER_H_

#include <blackrock/common.h>
#include <blackrock/storage/sibling.capnp.h>
#include "basics.h"
#include "journal-layer.h"

namespace blackrock {
namespace storage {

class ReplicaImpl {
public:
  JournalLayer& getJournal();
  JournalLayer::Object& getLocalObject();
};

class FollowerImpl: public Follower::Server {
public:
  FollowerImpl(JournalLayer& journal, JournalLayer::Object& object,
               capnp::Capability::Client capToHold, kj::Maybe<FollowerImpl&>& weakref,
               WeakLeader::Client leader);
  ~FollowerImpl() noexcept(false) { disconnect(); }

  void disconnect();

protected:
  kj::Promise<void> commit(CommitContext context) override;
  kj::Promise<void> stageDistributed(StageDistributedContext context) override;
  kj::Promise<void> write(WriteContext context) override;
  kj::Promise<void> sync(SyncContext context) override;
  kj::Promise<void> replace(ReplaceContext context) override;
  kj::Promise<void> copyTo(CopyToContext context) override;

private:
  class StagedTransactionImpl;
  class ReplacerImpl;

  struct State {
    JournalLayer& journal;
    JournalLayer::Object& object;
    capnp::Capability::Client capToHold;
    kj::Maybe<FollowerImpl&>& weakref;
    WeakLeader::Client leader;
  };
  kj::Maybe<State> state;
  // If null, disconnected.

  kj::Promise<void> writeQueue;

  bool isDirty = false;

  State& getState();
  kj::Promise<void> queueOp(kj::Function<kj::Promise<void>()>&& func);
  kj::Promise<void> commit(uint64_t version, RawTransaction::Reader txn);
  kj::Promise<void> setDirty();
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_FOLLOWER_H_
