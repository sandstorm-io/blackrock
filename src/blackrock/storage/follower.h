// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_FOLLOWER_H_
#define BLACKROCK_STORAGE_FOLLOWER_H_

#include <blackrock/common.h>
#include <blackrock/storage/sibling.capnp.h>
#include "basics.h"
#include "journal-layer.h"
#include "mid-level-object.h"

namespace blackrock {
namespace storage {

class FollowerImpl final: public Follower::Server {
public:
  FollowerImpl(MidLevelObject& object, capnp::Capability::Client capToHold,
               kj::Maybe<FollowerImpl&>& weakref, WeakLeader::Client leader);
  // Create follower. Note that the caller is responsible for having done everything that
  // needs to be done in vote() before the follower is created.

  ~FollowerImpl() noexcept(false);

  kj::Promise<void> disconnect();

  struct LeaderInfo {
    WeakLeader::Client cap;
    TermInfo::Reader term;
  };

  LeaderInfo getLeader();

protected:
  kj::Promise<void> commit(CommitContext context) override;
  kj::Promise<void> stageDistributed(StageDistributedContext context) override;
  kj::Promise<void> write(WriteContext context) override;
  kj::Promise<void> sync(SyncContext context) override;
  kj::Promise<void> replace(ReplaceContext context) override;
  kj::Promise<void> copyTo(CopyToContext context) override;
  kj::Promise<void> cleanShutdown(CleanShutdownContext context) override;

private:
  class StagedTransactionImpl;
  class ReplacerImpl;

  struct State {
    MidLevelObject& object;
    capnp::Capability::Client capToHold;
    kj::Maybe<FollowerImpl&>& weakref;
    WeakLeader::Client leader;
    kj::Promise<void> writeQueue;
  };
  kj::Maybe<State> state;
  // If null, disconnected.

  bool isDirty = false;

  State& getState();
  kj::Promise<void> queueOp(kj::Function<kj::Promise<void>(MidLevelObject& object)>&& func);
};

class VoterImpl final: public Voter::Server {
public:
  VoterImpl(MidLevelObject& object, capnp::Capability::Client capToHold,
            kj::Maybe<VoterImpl&>& weakref);

  ~VoterImpl() noexcept(false);

  kj::Promise<void> disconnect();
  // Disconnect this voter from its leader, if any.

  kj::Maybe<FollowerImpl::LeaderInfo> getLeader();
  // If the voter has voted for a leader, return that leader. If the voter has not voted yet,
  // return null. (It is an error to call this if the voter has been disconnected.)

  using Voter::Server::thisCap;

protected:
  kj::Promise<void> vote(VoteContext context) override;

private:
  struct State {
    MidLevelObject& object;
    capnp::Capability::Client capToHold;
    kj::Maybe<VoterImpl&>& weakref;
    kj::Maybe<FollowerImpl&> follower;
  };
  kj::Maybe<State> state;
  bool voted = false;

  State& getState();
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_FOLLOWER_H_
