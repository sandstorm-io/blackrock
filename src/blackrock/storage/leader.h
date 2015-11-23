// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_LEADER_H_
#define BLACKROCK_STORAGE_LEADER_H_

#include <blackrock/common.h>
#include <blackrock/storage/sibling.capnp.h>
#include "basics.h"
#include "mid-level-object.h"
#include "high-level-object.h"
#include <capnp/message.h>

namespace blackrock {
namespace storage {

class LeaderImpl final: public Leader::Server, public MidLevelWriter,
                        private kj::TaskSet::ErrorHandler {
public:
  LeaderImpl(uint siblingId, uint quorumSize,
             MidLevelReader& localObject, capnp::Capability::Client capToHold,
             kj::Array<capnp::Response<Replica::GetStateResults>> voters);

  ~LeaderImpl() noexcept(false);

  void abdicate();
  using Leader::Server::thisCap;  // make this public for WeakLeader

  void write(uint64_t offset, kj::ArrayPtr<const byte> data) override;
  kj::Promise<void> sync() override;
  kj::Promise<void> modify(ChangeSet::Reader changes) override;
  kj::Own<Replacer> startReplace() override;

protected:
  kj::Promise<void> getObject(GetObjectContext context) override;
  kj::Promise<void> startTransaction(StartTransactionContext context) override;
  kj::Promise<void> flushTransaction(FlushTransactionContext context) override;
  kj::Promise<void> getTransactionState(GetTransactionStateContext context) override;

private:
  class WeakLeaderImpl;
  class StagedTransactionImpl;
  class TransactionBuilderImpl;

  ObjectId id;
  uint siblingId;
  uint quorumSize;
  MidLevelReader& localObject;
  capnp::Capability::Client capToHold;
  kj::Own<WeakLeaderImpl> weak;
  kj::Maybe<OwnedStorageBase&> weakObject;
  ObjectKey lastKey;

  capnp::MallocMessageBuilder termInfoMessage;
  TermInfo::Reader termInfo;

  struct FollowerRecord {
    Follower::Client cap;
    uint siblingId;
  };
  struct FollowResponseRecord {
    Follower::Client cap;
    capnp::Response<Replica::GetStateResults> state;
  };

  kj::Array<FollowerRecord> followers;

  uint64_t version = 0;
  // Version after all queued operations complete.

  kj::TaskSet tasks;
  kj::ForkedPromise<void> ready;

  struct QuorumWaiter: public kj::Refcounted {
    uint successCount = 0;
    uint waitingCount = 0;
    bool sawSelf = false;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
  };

  kj::Promise<void> allFollowers(kj::Function<kj::Promise<void>(FollowerRecord&)>&& func);

  void taskFailed(kj::Exception&& exception) override;

  static TermInfo::Reader makeTermInfo(capnp::MessageBuilder& arena,
      kj::ArrayPtr<capnp::Response<Replica::GetStateResults>> voters);

  enum class Comparison {
    BETTER, SAME, WORSE
  };

  static Comparison compare(Replica::DataState::Reader left, Replica::DataState::Reader right);
  static Comparison compare(TermInfo::Reader left, TermInfo::Reader right);
};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_LEADER_H_
