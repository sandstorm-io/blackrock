// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_LEADER_H_
#define BLACKROCK_STORAGE_LEADER_H_

#include <blackrock/common.h>
#include <blackrock/storage/sibling.capnp.h>
#include "basics.h"
#include "journal-layer.h"
#include <capnp/message.h>

namespace blackrock {
namespace storage {

class LeaderImpl: public Leader::Server {
public:
  LeaderImpl(ObjectKey key, uint siblingId, uint quorumSize,
             JournalLayer::Object& localObject, capnp::Capability::Client capToHold,
             kj::Array<capnp::Response<Replica::GetStateResults>> voters);

  ~LeaderImpl() noexcept(false);

  void abdicate();
  using Leader::Server::thisCap;  // make this public for WeakLeader
  JournalLayer::Object& getLocalObject();
  void write(uint64_t offset, kj::Array<const byte> data);
  kj::Promise<void> sync();
  kj::Promise<void> modify(RawTransaction::Reader mod);

protected:
  kj::Promise<void> getObject(GetObjectContext context) override;
  kj::Promise<void> startTransaction(StartTransactionContext context) override;
  kj::Promise<void> flushTransaction(FlushTransactionContext context) override;
  kj::Promise<void> getTransactionState(GetTransactionStateContext context) override;

private:
  class WeakLeaderImpl;
  class TransactionBuilderImpl;
  class StagedTransactionImpl;

  ObjectKey key;
  ObjectId id;
  uint siblingId;
  uint quorumSize;
  JournalLayer::Object& localObject;
  capnp::Capability::Client capToHold;
  kj::Own<WeakLeaderImpl> weak;
  kj::Maybe<OwnedStorage<>::Server&> weakObject;

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

  kj::ForkedPromise<void> ready;
  kj::TaskSet tasks;

  struct QuorumWaiter: public kj::Refcounted {
    uint successCount = 0;
    uint waitingCount = 0;
    bool sawSelf = false;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
  };

  kj::Promise<void> queueOp(kj::Function<kj::Promise<void>(FollowerRecord&)>&& func);

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
