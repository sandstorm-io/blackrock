// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "journal-layer.h"
#include "blob-local.h"
#include <blackrock/storage/journal-test-fuzzer.capnp.h>
#include <sandstorm/util.h>
#include <kj/main.h>
#include <kj/debug.h>
#include <kj/one-of.h>
#include <capnp/message.h>
#include <unordered_set>
#include <sodium/crypto_stream_chacha20.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <signal.h>
#include <set>
#include <blackrock/storage.capnp.h>
#include <blackrock/nbd-bridge.h>
#include <kj/async-unix.h>
#include <sys/mount.h>
#undef BLOCK_SIZE

namespace blackrock {
namespace storage {
namespace {

template <typename T>
T dataTo(capnp::Data::Reader data) {
  KJ_REQUIRE(data.size() == sizeof(T));
  T result;
  memcpy(&result, data.begin(), data.size());
  return result;
}

class DeletedObject: public capnp::Capability::Server {
  // It's not necessarily an error to *open* a deleted object (for the purposes of this test), but
  // it is an error to *use* it in any way, hence this capability throws on calls but does not
  // resolve to an error. (This avoids some spurrious log messages.)

public:
  kj::Promise<void> dispatchCall(
      uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    KJ_FAIL_REQUIRE("object has been deleted");
  }
};

class TestJournalImpl: public TestJournal::Server {
public:
  TestJournalImpl(JournalLayer& journal, uint64_t recoveredCount)
      : journal(journal), tempCounter(recoveredCount) {}

  TestJournal::Object::Client wrap(kj::Own<JournalLayer::Object>&& object) {
    return objects.add(kj::heap<ObjectImpl>(kj::mv(object)));
  }
  TestJournal::Object::Client wrap(kj::Own<JournalLayer::RecoverableTemporary>&& temp) {
    return objects.add(kj::heap<ObjectImpl>(kj::mv(temp)));
  }

protected:
  kj::Promise<void> openObject(OpenObjectContext context) override {
    ObjectId id = context.getParams().getId();
    context.releaseParams();
    return journal.openObject(id)
        .then([this,context,id](auto&& maybeObject) mutable {
      auto results = context.getResults(capnp::MessageSize { 4, 1 });
      KJ_IF_MAYBE(o, maybeObject) {
        results.setObject(
            wrap(kj::mv(KJ_REQUIRE_NONNULL(maybeObject, id))));
      } else {
        results.setObject(capnp::Capability::Client(kj::heap<DeletedObject>())
            .castAs<TestJournal::Object>());
      }
    });
  }

  kj::Promise<void> newTransaction(NewTransactionContext context) override {
    context.getResults().setTransaction(kj::heap<TransactionImpl>(*this, thisCap()));
    return kj::READY_NOW;
  }

private:
  JournalLayer& journal;
  capnp::CapabilityServerSet<TestJournal::Object> objects;
  uint64_t tempCounter = 0;

  class ObjectImpl: public TestJournal::Object::Server {
  public:
    typedef kj::Own<JournalLayer::Object> InnerObject;
    typedef kj::Own<JournalLayer::RecoverableTemporary> InnerTemp;

    explicit ObjectImpl(kj::Own<JournalLayer::Object>&& object) {
      inner.init<InnerObject>(kj::mv(object));
    }
    explicit ObjectImpl(kj::Own<JournalLayer::RecoverableTemporary>&& temp) {
      inner.init<InnerTemp>(kj::mv(temp));
    }

    void update(JournalLayer::Transaction& txn, capnp::Data::Reader xattrData,
                kj::Maybe<kj::Own<BlobLayer::Temporary>>&& content) {
      if (inner.is<InnerObject>()) {
        auto& innerObject = *inner.get<InnerObject>();
        Xattr xattr = xattrData.size() > 0 ? dataTo<Xattr>(xattrData)
                                           : innerObject.getXattr();
        auto txnObject = txn.wrap(innerObject);
        KJ_IF_MAYBE(c, content) {
          txnObject->overwrite(xattr, kj::mv(*c));
        } else {
          txnObject->setXattr(xattr);
        }
      } else {
        auto& innerTemp = *inner.get<InnerTemp>();
        TemporaryXattr xattr = xattrData.size() > 0 ? dataTo<TemporaryXattr>(xattrData)
                                                    : innerTemp.getXattr();
        auto txnTemp = txn.wrap(innerTemp);
        KJ_IF_MAYBE(c, content) {
          txnTemp->overwrite(xattr, kj::mv(*c));
        } else {
          txnTemp->setXattr(xattr);
        }
      }
    }

    void remove(JournalLayer::Transaction& txn) {
      KJ_REQUIRE(inner.is<InnerObject>(), "temporaries aren't removed this way");
      txn.wrap(*inner.get<InnerObject>())->remove();
    }

    kj::Promise<void> consumeAndCommit(JournalLayer::Transaction& txn) {
      KJ_REQUIRE(inner.is<InnerTemp>(), "temporaries aren't removed this way");
      return txn.commit(kj::mv(inner.get<InnerTemp>()));
    }

  protected:
    kj::Promise<void> read(ReadContext context) {
      auto results = context.getResults();

      if (inner.is<InnerObject>()) {
        auto xattr = inner.get<InnerObject>()->getXattr();
        memcpy(results.initXattr(sizeof(xattr)).begin(), &xattr, sizeof(xattr));
      } else {
        auto xattr = inner.get<InnerTemp>()->getXattr();
        memcpy(results.initXattr(sizeof(xattr)).begin(), &xattr, sizeof(xattr));
      }

      BlobLayer::Content& content = inner.is<InnerObject>()
          ? inner.get<InnerObject>()->getContent()
          : inner.get<InnerTemp>()->getContent();
      return content.read(0, results.initContent(content.getSize().endMarker));
    }

  private:
    kj::OneOf<InnerObject, InnerTemp> inner;
  };

  class TransactionImpl: public TestJournal::Transaction::Server {
  public:
    explicit TransactionImpl(TestJournalImpl& journal, TestJournal::Client journalCap)
        : journal(journal), journalCap(kj::mv(journalCap)), transaction(journal.journal) {}

  protected:
    kj::Promise<void> createObject(CreateObjectContext context) override {
      auto params = context.getParams();
      auto temp = journal.journal.newDetachedTemporary();
      temp->getContent().write(0, params.getContent());
      context.getResults(capnp::MessageSize {4,1}).setObject(
          journal.wrap(transaction.createObject(
              params.getId(), dataTo<Xattr>(params.getXattr()), kj::mv(temp))));
      return kj::READY_NOW;
    }

    kj::Promise<void> createTemp(CreateTempContext context) override {
      auto params = context.getParams();
      auto temp = journal.journal.newDetachedTemporary();
      temp->getContent().write(0, params.getContent());
      context.getResults(capnp::MessageSize {4,1}).setObject(
          journal.wrap(transaction.createRecoverableTemporary(
              RecoveryId(RecoveryType::BACKBURNER, journal.tempCounter++),
              dataTo<TemporaryXattr>(params.getXattr()), kj::mv(temp))));
      return kj::READY_NOW;
    }

    kj::Promise<void> update(UpdateContext context) override {
      auto params = context.getParams();
      auto cap = kj::heap(params.getObject());

      auto promise = journal.objects.getLocalServer(*cap);
      return promise.then([this,context,params,KJ_MVCAP(cap)](
          kj::Maybe<TestJournal::Object::Server&> maybeObject) mutable {
        kj::Maybe<kj::Own<BlobLayer::Temporary>> content;
        if (params.hasContent()) {
          auto temp = journal.journal.newDetachedTemporary();
          temp->getContent().write(0, params.getContent());
          content = kj::mv(temp);
        }

        kj::downcast<ObjectImpl>(KJ_REQUIRE_NONNULL(maybeObject))
            .update(transaction, params.getXattr(), kj::mv(content));
      });
    }

    kj::Promise<void> remove(RemoveContext context) override {
      auto params = context.getParams();
      auto cap = kj::heap(params.getObject());

      auto promise = journal.objects.getLocalServer(*cap);
      return promise.then([this,context,params,KJ_MVCAP(cap)](
          kj::Maybe<TestJournal::Object::Server&> maybeObject) mutable {
        kj::downcast<ObjectImpl>(KJ_REQUIRE_NONNULL(maybeObject)).remove(transaction);
      });
    }

    kj::Promise<void> commit(CommitContext context) override {
      auto params = context.getParams();
      if (!params.hasTempToConsume()) {
        return transaction.commit();
      }

      auto cap = params.getTempToConsume();
      auto promise = journal.objects.getLocalServer(cap);
      return promise.then([this,context,params,KJ_MVCAP(cap)](
          kj::Maybe<TestJournal::Object::Server&> maybeObject) mutable {
        return kj::downcast<ObjectImpl>(KJ_REQUIRE_NONNULL(maybeObject))
            .consumeAndCommit(transaction);
      });
    }

  private:
    TestJournalImpl& journal;
    TestJournal::Client journalCap;
    JournalLayer::Transaction transaction;
  };
};

class TestJournalRecoveryImpl: public TestJournal::Recovery::Server {
public:
  explicit TestJournalRecoveryImpl(JournalLayer::Recovery& inner): inner(inner) {}

protected:
  kj::Promise<void> recover(RecoverContext context) override {
    uint64_t recoveredCount = 0;

    auto recovered = KJ_MAP(temp, inner.recoverTemporaries(RecoveryType::BACKBURNER)) {
      return temp->keepAs(RecoveryId(RecoveryType::BACKBURNER, recoveredCount++));
    };

    auto journalImpl = kj::heap<TestJournalImpl>(inner.finish(), recoveredCount);

    auto results = context.getResults();

    auto builder = results.initRecoveredTemps(recovered.size());
    for (auto i: kj::indices(recovered)) {
      auto item = builder[i];
      auto xattr = recovered[i]->getXattr();
      memcpy(item.initXattr(sizeof(xattr)).begin(), &xattr, sizeof(xattr));
      item.setObject(journalImpl->wrap(kj::mv(recovered[i])));
    }
    results.setJournal(kj::mv(journalImpl));

    return kj::READY_NOW;
  }

private:
  JournalLayer::Recovery& inner;
};

// =======================================================================================
// Wrapper layer which deals with back-end disconnects and recovery.

struct TempLabel {
  // The bytes we put into TemporaryXattr for the purposes of this test. It's OK that it doesn't
  // match the real TemporaryXattr at all because the journal does not actually interpret the
  // bytes.

  union {
    TemporaryXattr forceSize;

    struct {
      bool isTransaction;
      // If true, this temporary was created as part of a transaction for the sole purpose of
      // being able to verify whether or not the transaction completed in the case that commit()
      // throws DISCONNECTED.
      //
      // If false, this is just a randomly-generated temporary object being used for testing.

      uint64_t id;
      // ID number for this object. We store it in Xattr because the ID in RecoveryId is intended
      // to reset with each recovery, and temporaries have no other long-term ID.

      uint64_t other;
      // Some random data that we can modify.
    };
  };

  inline bool operator<(const TempLabel& other) const {
    return isTransaction == other.isTransaction ? id < other.id : isTransaction;
  }
};

struct TempClient {
  // Stupid hack to work around STL containers disliking the non-const inputs to capability client
  // copy constructors.
  TempClient(TestJournal::Object::Client client): client(kj::mv(client)) {}
  KJ_DISALLOW_COPY(TempClient);
  TempClient(TempClient&& other) = default;

  TestJournal::Object::Client client;
};

class RecoverySet;

class RecoverySetCapTableReader: public capnp::_::CapTableReader {
  // TODO(cleanup): This should me mostly refactored into the Cap'n Proto library.

public:
  RecoverySetCapTableReader(RecoverySet& recoverySet, uint64_t interfaceId, uint16_t methodId,
                            capnp::AnyStruct::Reader params)
      : recoverySet(recoverySet), interfaceId(interfaceId), methodId(methodId), params(params) {}

  capnp::AnyPointer::Reader imbue(capnp::AnyPointer::Reader reader) {
    return capnp::AnyPointer::Reader(imbue(
        capnp::_::PointerHelpers<capnp::AnyPointer>::getInternalReader(kj::mv(reader))));
  }

  capnp::_::PointerReader imbue(capnp::_::PointerReader reader) {
    KJ_REQUIRE(inner == nullptr, "can only call this once");
    inner = reader.getCapTable();
    return reader.imbue(this);
  }

  capnp::_::StructReader imbue(capnp::_::StructReader reader) {
    KJ_REQUIRE(inner == nullptr, "can only call this once");
    inner = reader.getCapTable();
    return reader.imbue(this);
  }

  capnp::_::ListReader imbue(capnp::_::ListReader reader) {
    KJ_REQUIRE(inner == nullptr, "can only call this once");
    inner = reader.getCapTable();
    return reader.imbue(this);
  }

  kj::Maybe<kj::Own<capnp::ClientHook>> extractCap(uint index) override;

private:
  RecoverySet& recoverySet;
  uint64_t interfaceId;
  uint16_t methodId;
  capnp::AnyStruct::Reader params;
  capnp::_::CapTableReader* inner = nullptr;
};

class RecoverySetCapTableBuilder: public capnp::_::CapTableBuilder {
  // TODO(cleanup): This should me mostly refactored into the Cap'n Proto library.

public:
  explicit RecoverySetCapTableBuilder(RecoverySet& recoverySet): recoverySet(recoverySet) {}

  capnp::AnyPointer::Builder imbue(capnp::AnyPointer::Builder builder) {
    KJ_REQUIRE(inner == nullptr, "can only call this once");
    auto pointerBuilder =
        capnp::_::PointerHelpers<capnp::AnyPointer>::getInternalBuilder(kj::mv(builder));
    inner = pointerBuilder.getCapTable();
    return capnp::AnyPointer::Builder(pointerBuilder.imbue(this));
  }

  capnp::AnyPointer::Builder unimbue(capnp::AnyPointer::Builder builder) {
    auto pointerBuilder =
        capnp::_::PointerHelpers<capnp::AnyPointer>::getInternalBuilder(kj::mv(builder));
    KJ_REQUIRE(pointerBuilder.getCapTable() == this);
    return capnp::AnyPointer::Builder(pointerBuilder.imbue(inner));
  }

  kj::Maybe<kj::Own<capnp::ClientHook>> extractCap(uint index) override {
    KJ_UNIMPLEMENTED("can't extract here");
  }

  uint injectCap(kj::Own<capnp::ClientHook>&& cap) override;

  void dropCap(uint index) override {
    inner->dropCap(index);
  }

private:
  RecoverySet& recoverySet;
  capnp::_::CapTableBuilder* inner = nullptr;
};

class RecoverableCapability: virtual public capnp::Capability::Server {
  // A capability which can be recovered later.

public:
  RecoverableCapability(RecoverySet& recoverySet, capnp::Capability::Client inner);
  ~RecoverableCapability();

  virtual void recover(TestJournal::Client journal,
                       std::map<TempLabel, TempClient>& recovered) = 0;

  capnp::Capability::Client getInner() { return inner; }

protected:
  void setInner(capnp::Capability::Client newInner) {
    inner = kj::mv(newInner);
    ++generation;
    if (waitingForReconnect) {
      reconnectDoneFulfiller->fulfill();
      waitingForReconnect = false;
    }
  }

  kj::Promise<void> doCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) {
    auto params = context.getParams();
    auto req = inner.typelessRequest(interfaceId, methodId, params.targetSize());
    RecoverySetCapTableBuilder capTable(recoverySet);
    capTable.imbue(req).set(params);
    uint oldGen = generation;
    return req.send()
        .then([this,context,params,interfaceId,methodId](auto&& response) mutable
               -> kj::Promise<void> {
      RecoverySetCapTableReader capTable(recoverySet, interfaceId, methodId,
                                         params.getAs<capnp::AnyStruct>());
      context.getResults(response.targetSize()).set(capTable.imbue(response));
      return kj::READY_NOW;
    }, [this,oldGen,interfaceId,methodId,context](kj::Exception&& e) mutable -> kj::Promise<void> {
      if (e.getType() == kj::Exception::Type::DISCONNECTED) {
        return reconnect(oldGen).then([this,interfaceId,methodId,context]() mutable {
          return doCall(interfaceId, methodId, context);
        });
      } else {
        return kj::mv(e);
      }
    });
  }

  kj::Promise<void> reconnect(uint oldGeneration) {
    if (generation > oldGeneration) {
      // Already advanced to next generation.
      return kj::READY_NOW;
    } else if (!waitingForReconnect) {
      // This is the first we're aware that we've been disconnected. Set up reconnect promises.
      auto paf = kj::newPromiseAndFulfiller<void>();
      reconnectDoneFulfiller = kj::mv(paf.fulfiller);
      reconnectDonePromise = paf.promise.fork();
      waitingForReconnect = true;
    }

    return reconnectDonePromise.addBranch();
  }

  uint getGeneration() { return generation; }
  RecoverySet& getRecoverySet() { return recoverySet; }

  TestJournal::Object::Client wrapObject(ObjectId id, TestJournal::Object::Client inner);
  TestJournal::Object::Client wrapTemp(uint64_t id, TestJournal::Object::Client inner);

private:
  RecoverySet& recoverySet;
  uint generation = 0;
  bool waitingForReconnect = false;
  capnp::Capability::Client inner;
  kj::Own<kj::PromiseFulfiller<void>> reconnectDoneFulfiller;
  kj::ForkedPromise<void> reconnectDonePromise = nullptr;
};

class RecoverableJournal: public RecoverableCapability, public TestJournal::Server {
public:
  RecoverableJournal(RecoverySet& recoverySet, TestJournal::Client inner)
      : RecoverableCapability(recoverySet, kj::mv(inner)) {}

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
    setInner(kj::mv(journal));
  }

  kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    return doCall(interfaceId, methodId, context);
  }
};

class RecoverableObject: public RecoverableCapability, public TestJournal::Object::Server {
public:
  RecoverableObject(RecoverySet& recoverySet, ObjectId id, TestJournal::Object::Client inner)
      : RecoverableCapability(recoverySet, kj::mv(inner)), id(id) {}

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
    auto req = journal.openObjectRequest();
    id.copyTo(req.initId());
    setInner(req.send().getObject());
  }

  kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    return doCall(interfaceId, methodId, context);
  }

private:
  ObjectId id;
};

std::set<uint64_t> tempsBeingDeleted;
// HACK: The temporaries which are currently pending a deletion request. We will inconsistencies
//   in these during recovery.

class RecoverableTemp: public RecoverableCapability, public TestJournal::Object::Server {
public:
  RecoverableTemp(RecoverySet& recoverySet, uint64_t id, TestJournal::Object::Client inner)
      : RecoverableCapability(recoverySet, kj::mv(inner)), id(id) {
//    auto msg = kj::str(id >> 48, ": ", id & ((1ull << 48) - 1), " !!!\n");
//    kj::FdOutputStream(STDOUT_FILENO).write(msg.begin(), msg.size());
  }
  ~RecoverableTemp() noexcept(false) {
//    auto msg = kj::str(id >> 48, ": ", id & ((1ull << 48) - 1), " XXX\n");
//    kj::FdOutputStream(STDOUT_FILENO).write(msg.begin(), msg.size());
  }

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
//    auto msg = kj::str(id >> 48, ": ", id & ((1ull << 48) - 1), " RECOVER\n");
//    kj::FdOutputStream(STDOUT_FILENO).write(msg.begin(), msg.size());

    TempLabel label;
    memset(&label, 0, sizeof(label));
    label.isTransaction = false;
    label.id = id;
    auto iter = recovered.find(label);
    if (iter == recovered.end() && tempsBeingDeleted.count(id)) {
      // ignore
      return;
    }
    KJ_ASSERT(iter != recovered.end(), "temporary was not recovered",
              (id >> 48), (id & ((1ull << 48) - 1)));
    setInner(kj::mv(iter->second.client));
    recovered.erase(iter);
  }

  kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    return doCall(interfaceId, methodId, context);
  }

private:
  uint64_t id;
};

class RecoverableTransaction
    : public RecoverableCapability, public TestJournal::Transaction::Server,
      private kj::TaskSet::ErrorHandler {
public:
  RecoverableTransaction(RecoverySet& recoverySet, uint64_t id,
                         TestJournal::Transaction::Client inner)
      : RecoverableCapability(recoverySet, kj::cp(inner)), id(id),
        tasks(*this), commitSentinel(makeCommitSentinel(kj::mv(inner))) {}

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
    auto newInner = journal.newTransactionRequest().send().getTransaction();

    if (!committed) {
      // Check if commit succeeded.
      TempLabel label;
      memset(&label, 0, sizeof(label));
      label.isTransaction = true;
      label.id = id;
      auto iter = recovered.find(label);
      if (iter != recovered.end()) {
        committed = true;
        recovered.erase(iter);

        for (auto& call: calls) {
          if (call->methodId == 0) {
            // createObject
            auto req = journal.openObjectRequest();
            auto id = call->params.getReader().as<CreateObjectParams>().getId();
            req.setId(id);

            // We need to delay creation of the RecoverableObject to prevent it from becoming
            // part of this recovery cycle.
            auto replacement = req.send().getObject();
            call->resultFulfiller->fulfill(kj::evalLater(
                [this,id,KJ_MVCAP(replacement)]() mutable -> TestJournal::Object::Client {
                  return wrapObject(id, kj::mv(replacement));
                }));
          } else if (call->methodId == 1) {
            // createTemp
            auto label = dataTo<TempLabel>(
                call->params.getReader().as<CreateTempParams>().getXattr());
            auto iter = recovered.find(label);
            KJ_ASSERT(iter != recovered.end(),
                "failed to recover temporary that was just committed");

            // We need to delay creation of the RecoverableTemp to prevent it from becoming
            // part of this recovery cycle.
            auto replacement = kj::mv(iter->second.client);
            call->resultFulfiller->fulfill(kj::evalLater(
                [this,label,KJ_MVCAP(replacement)]() mutable -> TestJournal::Object::Client {
                  return wrapTemp(label.id, kj::mv(replacement));
                }));

            recovered.erase(iter);
          } else {
            call->resultFulfiller->fulfill(nullptr);
          }
        }

        setInner(kj::mv(newInner));
      } else {
        // Replay previous calls before continuing current ones.
        commitSentinel = makeCommitSentinel(newInner);
        auto promises = kj::heapArrayBuilder<kj::Promise<void>>(calls.size());
        for (auto& call: calls) {
          auto& callRef = *call;
          // Delay replaying the call until later because first we need to finish recovering all
          // the input object references.
          promises.add(kj::evalLater([this,newInner,&callRef]() mutable {
            return replayCall(kj::mv(newInner), callRef);
          }));
        }

        // Make sure any new calls to the inner capability are delayed until all replayed calls
        // finish, otherwise we might call commit too early.
        setInner(kj::joinPromises(promises.finish()).then([KJ_MVCAP(newInner)]() mutable {
          return kj::mv(newInner);
        }));
      }
    }
  }

  kj::Promise<void> createObject(CreateObjectContext context) override {
    auto paf = kj::newPromiseAndFulfiller<TestJournal::Object::Client>();
    context.getResults().setObject(kj::mv(paf.promise));
    return doReplayable(0, context.getParams(), kj::mv(paf.fulfiller));
  }

  kj::Promise<void> createTemp(CreateTempContext context) override {
    auto paf = kj::newPromiseAndFulfiller<TestJournal::Object::Client>();
    context.getResults().setObject(kj::mv(paf.promise));
    return doReplayable(1, context.getParams(), kj::mv(paf.fulfiller));
  }

  kj::Promise<void> update(UpdateContext context) override {
    return doReplayable(2, context.getParams());
  }

  kj::Promise<void> remove(RemoveContext context) override {
    return doReplayable(3, context.getParams());
  }

  kj::Promise<void> commit(CommitContext context) override;

private:
  uint64_t id;

  struct ReplayableCall {
    uint16_t methodId;
    capnp::Orphan<capnp::AnyStruct> params;

    TestJournal::Object::Client lastResult;
    kj::Own<kj::PromiseFulfiller<TestJournal::Object::Client>> resultFulfiller;
    // Some methods return objects, but if we replay the method later then we may need to
    // substitute a different object. So, we return promise capabilities first. `lastResult`
    // is the capability we got the last time we made the call. Once the txn is actually
    // committed, we must fulfill `resultFulfiller` with this capability.
  };

  capnp::MallocMessageBuilder arena;
  kj::Vector<kj::Own<ReplayableCall>> calls;
  bool committed = false;

  kj::TaskSet tasks;
  TestJournal::Object::Client commitSentinel;

  void taskFailed(kj::Exception&& exception) override {
    if (exception.getType() != kj::Exception::Type::DISCONNECTED) {
      KJ_DEFER(abort());
      KJ_LOG(FATAL, exception);
    }
  }

  TestJournal::Object::Client makeCommitSentinel(TestJournal::Transaction::Client inner) {
    TempLabel label;
    memset(&label, 0, sizeof(label));
    label.isTransaction = true;
    label.id = id;
    auto req = inner.createTempRequest();
    memcpy(req.initXattr(sizeof(label)).begin(), &label, sizeof(label));
    return req.send().getObject();
  }

  kj::Promise<void> doReplayable(uint16_t methodId, capnp::AnyStruct::Reader params,
      kj::Own<kj::PromiseFulfiller<TestJournal::Object::Client>> fulfiller =
          kj::heap<DummyFulfiller>()) {
    auto replay = kj::heap(ReplayableCall {
      methodId,
      arena.getOrphanage().newOrphanCopy(params),
      nullptr,
      kj::mv(fulfiller)
    });

    calls.add(kj::mv(replay));
    return replayCall(getInner(), *calls.back());
  }

  kj::Promise<void> replayCall(capnp::Capability::Client inner, ReplayableCall& call) {
    auto req = inner.typelessRequest(capnp::typeId<TestJournal::Transaction>(),
        call.methodId, nullptr);
    RecoverySetCapTableBuilder capTable(getRecoverySet());
    capTable.imbue(req).setAs<capnp::AnyStruct>(call.params.getReader());

    return req.send()
        .then([this, &call](capnp::Response<capnp::AnyPointer>&& response) -> kj::Promise<void> {
      auto pointers = response.getAs<capnp::AnyStruct>().getPointerSection();
      if (pointers.size() > 0) {
        call.lastResult = pointers[0].getAs<TestJournal::Object>();
      } else {
        call.lastResult = nullptr;
      }
      return kj::READY_NOW;
    }, [](kj::Exception&& e) -> kj::Promise<void> {
      if (e.getType() == kj::Exception::Type::DISCONNECTED) {
        // Will retry later.
        return kj::READY_NOW;
      } else {
        return kj::mv(e);
      }
    });
  }

  class DummyFulfiller: public kj::PromiseFulfiller<TestJournal::Object::Client> {
  public:
    void fulfill(TestJournal::Object::Client&&) override {}
    void reject(kj::Exception&& exception) override {}
    bool isWaiting() override { return false; }
  };
};

class RecoverySet {
public:
  void add(RecoverableCapability* cap) {
    KJ_REQUIRE(activeCaps.insert(cap).second);
  }
  void remove(RecoverableCapability* cap) {
    KJ_REQUIRE(activeCaps.erase(cap) == 1) { break; }
  }

  kj::Promise<void> recover(TestJournal::Recovery::Client newJournalRecovery) {
    return newJournalRecovery.recoverRequest().send().then([this](auto&& response) {
      std::map<TempLabel, TempClient> recovered;

      for (auto temp: response.getRecoveredTemps()) {
        KJ_ASSERT(recovered.insert(std::make_pair(
            dataTo<TempLabel>(temp.getXattr()), TempClient(temp.getObject()))).second);
      }

      auto newJournal = response.getJournal();
      for (auto cap: activeCaps) {
        cap->recover(newJournal, recovered);
      }

      for (auto& entry: recovered) {
        KJ_ASSERT(entry.first.isTransaction || tempsBeingDeleted.count(entry.first.id) > 0,
            "recovered unexpected temporaries",
            (entry.first.id >> 48), (entry.first.id & ((1ull << 48) - 1)));
      }
    });
  }

  TestJournal::Client wrap(TestJournal::Client inner) {
    return serverSet.add(kj::heap<RecoverableJournal>(*this, kj::mv(inner))).castAs<TestJournal>();
  }

  capnp::Capability::Client wrap(uint64_t creatingInterfaceId, uint16_t creatingMethodId,
      capnp::AnyStruct::Reader params, capnp::Capability::Client inner) {
    switch (creatingInterfaceId + creatingMethodId) {
      case capnp::typeId<TestJournal>() + 0:
        return serverSet.add(kj::heap<RecoverableObject>(*this,
            params.as<TestJournal::OpenObjectParams>().getId(),
            inner.castAs<TestJournal::Object>()));
      case capnp::typeId<TestJournal>() + 1:
        return serverSet.add(kj::heap<RecoverableTransaction>(*this, txnCounter++,
            inner.castAs<TestJournal::Transaction>()));
      case capnp::typeId<TestJournal::Transaction>() + 0:
        return serverSet.add(kj::heap<RecoverableObject>(*this,
            params.as<TestJournal::Transaction::CreateObjectParams>().getId(),
            inner.castAs<TestJournal::Object>()));
      case capnp::typeId<TestJournal::Transaction>() + 1:
        return serverSet.add(kj::heap<RecoverableTemp>(*this,
          dataTo<TempLabel>(params.as<TestJournal::Transaction::CreateTempParams>().getXattr()).id,
          inner.castAs<TestJournal::Object>()));
      case capnp::typeId<TestJournal::Transaction>() + 2:
      case capnp::typeId<TestJournal::Transaction>() + 3:
        return nullptr;

      default:
        KJ_FAIL_ASSERT("unknown method", creatingInterfaceId, creatingMethodId);
    }
  }

  TestJournal::Object::Client wrapObject(ObjectId id, TestJournal::Object::Client inner) {
    return serverSet.add(kj::heap<RecoverableObject>(*this, id, kj::mv(inner)))
        .castAs<TestJournal::Object>();
  }

  TestJournal::Object::Client wrapTemp(uint64_t id, TestJournal::Object::Client inner) {
    return serverSet.add(kj::heap<RecoverableTemp>(*this, id, kj::mv(inner)))
        .castAs<TestJournal::Object>();
  }

  capnp::Capability::Client unwrap(capnp::Capability::Client cap) {
    auto promise = serverSet.getLocalServer(cap);
    return promise.then([KJ_MVCAP(cap)](kj::Maybe<capnp::Capability::Server&>&& unwrapped) mutable {
      return dynamic_cast<RecoverableCapability&>(KJ_REQUIRE_NONNULL(unwrapped)).getInner();
    });
  }

private:
  std::unordered_set<RecoverableCapability*> activeCaps;
  capnp::CapabilityServerSet<capnp::Capability> serverSet;
  uint64_t txnCounter = 0;
};

kj::Maybe<kj::Own<capnp::ClientHook>> RecoverySetCapTableReader::extractCap(uint index) {
  return inner->extractCap(index).map([this](kj::Own<capnp::ClientHook>&& cap) {
    return capnp::ClientHook::from(recoverySet.wrap(
        interfaceId, methodId, params, capnp::Capability::Client(kj::mv(cap))));
  });
}

uint RecoverySetCapTableBuilder::injectCap(kj::Own<capnp::ClientHook>&& cap) {
  return inner->injectCap(capnp::ClientHook::from(
      recoverySet.unwrap(capnp::Capability::Client(kj::mv(cap)))));
}

RecoverableCapability::RecoverableCapability(
    RecoverySet& recoverySet, capnp::Capability::Client inner)
    : recoverySet(recoverySet), inner(kj::mv(inner)) {
  recoverySet.add(this);
}
RecoverableCapability::~RecoverableCapability() {
  recoverySet.remove(this);
}

kj::Promise<void> RecoverableTransaction::commit(CommitContext context) {
  uint oldGen = getGeneration();

  return commitSentinel.whenResolved().then([this,context]() mutable {
    auto req = getInner().castAs<TestJournal::Transaction>().commitRequest();
    auto params = context.getParams();
    if (params.hasTempToConsume()) {
      req.setTempToConsume(getRecoverySet().unwrap(params.getTempToConsume())
          .castAs<TestJournal::Object>());
    }

    return req.send().then([this,context](auto&& response) mutable -> kj::Promise<void> {
      committed = true;

      for (auto& call: calls) {
        call->resultFulfiller->fulfill(
            getRecoverySet()
                .wrap(capnp::typeId<TestJournal::Transaction>(), call->methodId,
                      call->params.getReader(), kj::mv(call->lastResult))
                .castAs<TestJournal::Object>());
      }
      return kj::READY_NOW;
    });
  }).catch_([this,oldGen,context](kj::Exception&& e) mutable -> kj::Promise<void> {
    if (e.getType() == kj::Exception::Type::DISCONNECTED) {
      return reconnect(oldGen).then([this,context]() mutable -> kj::Promise<void> {
        if (committed) {
          // Recovery discovered that this was committed after all.
          return kj::READY_NOW;
        } else {
          return commit(context);
        }
      });
    } else {
      return kj::mv(e);
    }
  });
}

TestJournal::Object::Client RecoverableCapability::wrapObject(
    ObjectId id, TestJournal::Object::Client inner) {
  return recoverySet.wrapObject(id, kj::mv(inner));
}

TestJournal::Object::Client RecoverableCapability::wrapTemp(
    uint64_t id, TestJournal::Object::Client inner) {
  return recoverySet.wrapTemp(id, kj::mv(inner));
}

// =======================================================================================
// Fake disk

class Random {
public:
  explicit Random(uint64_t seed) {
    memset(key, 0, sizeof(key));
    memcpy(key, &seed, sizeof(seed));
  }

  bool flip() {
    byte b;
    fill(b);
    return b & 1;
  }

  uint64_t in(uint64_t low, uint64_t high) {
    uint64_t result;
    fill(result);
    result %= (high - low);
    return result + low;
  }

  uint64_t exponential(uint maxBits) {
    uint64_t result;
    fill(result);
    return result >> (63 - in(0, maxBits));
  }

  uint64_t exponential(uint minBits, uint maxBits) {
    uint64_t result;
    fill(result);
    return result >> (63 - in(minBits, maxBits));
  }

  void fill(void* buf, size_t size) {
    byte* p = reinterpret_cast<byte*>(buf);
    while (size > 0) {
      if (pos >= sizeof(buffer)) refresh();
      size_t n = kj::min(size, sizeof(buffer) - pos);
      memcpy(p, buffer + pos, n);
      pos += n;
      p += n;
      size -= n;
    }
  }

  template <typename T>
  inline void fill(T& t) {
    static_assert(__has_trivial_destructor(T), "not a pod");
    fill(&t, sizeof(t));
  }

private:
  byte key[crypto_stream_chacha20_KEYBYTES];
  uint64_t counter = 0;
  size_t pos = sizeof(buffer);
  byte buffer[65536];

  void refresh() {
    static_assert(sizeof(counter) == crypto_stream_chacha20_NONCEBYTES, "nonce size changed?");
    crypto_stream_chacha20(buffer, sizeof(buffer), reinterpret_cast<byte*>(&counter), key);
    ++counter;
    pos = 0;
  }
};

class RamVolume: public Volume::Server, public kj::Refcounted {
public:
  RamVolume(kj::Timer& timer, Random& random): timer(timer), random(random) {}

  kj::Promise<void> runFor(uint count) {
    if (writeCount == 0) {
      unsyncedBlocks.clear();
    }

    writeCount = count;
    auto paf = kj::newPromiseAndFulfiller<void>();
    brokenFulfiller = kj::mv(paf.fulfiller);
    return kj::mv(paf.promise);
  }

  bool isBroken() { return writeCount == 0; }

  kj::Promise<void> read(ReadContext context) override {
    auto params = context.getParams();
    auto start = params.getBlockNum();
    auto count = params.getCount();
    auto end = start + count;
    context.releaseParams();

    auto data = context.getResults().initData(count * Volume::BLOCK_SIZE);
    byte* ptr = data.begin();
    for (auto i = start; i < end; i++, ptr += Volume::BLOCK_SIZE) {
      auto iter1 = unsyncedBlocks.find(i);
      if (iter1 == unsyncedBlocks.end()) {
        // Not in unsyncedBlocks, fall back to blocks.
        auto iter2 = blocks.find(i);
        if (iter2 == blocks.end()) {
          // Not found, leave zero.
        } else {
          memcpy(ptr, iter2->second.begin(), Volume::BLOCK_SIZE);
        }
      } else {
        // Found in usyncedBlocks.
        KJ_IF_MAYBE(b, iter1->second) {
          memcpy(ptr, b->begin(), Volume::BLOCK_SIZE);
        } else {
          // This block has been zeroed.
        }
      }
    }
    return kj::READY_NOW;
  }

  kj::Promise<void> write(WriteContext context) override {
    if (writeCount > 0) {
      if (--writeCount == 0) break_();
    }

    auto params = context.getParams();
    auto start = params.getBlockNum();
    auto data = params.getData();
    auto count = data.size() / Volume::BLOCK_SIZE;
    auto end = start + count;

    const byte* ptr = data.begin();
    for (auto i = start; i < end; i++, ptr += Volume::BLOCK_SIZE) {
      auto block = kj::heapArray<byte>(Volume::BLOCK_SIZE);
      memcpy(block.begin(), ptr, Volume::BLOCK_SIZE);
      unsyncedBlocks[i] = kj::mv(block);
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> zero(ZeroContext context) override {
    if (writeCount > 0) {
      if (--writeCount == 0) break_();
    }

    auto params = context.getParams();
    auto start = params.getBlockNum();
    auto count = params.getCount();
    auto end = start + count;

    for (auto i = start; i < end; i++) {
      unsyncedBlocks[i] = nullptr;
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> sync(SyncContext context) override {
    // Randomly delay sometimes.
    if (random.in(0, 16) == 0) {
      return timer.afterDelay(4 * kj::MILLISECONDS).then([this,context]() mutable {
        return sync(context);
      });
    }

    if (writeCount > 0) {
      for (auto& block: unsyncedBlocks) {
        KJ_IF_MAYBE(b, block.second) {
          blocks[block.first] = kj::mv(*b);
        } else {
          blocks.erase(block.first);
        }
      }
      unsyncedBlocks.clear();
    }

    return kj::READY_NOW;
  }

private:
  kj::Timer& timer;
  Random& random;

  std::unordered_map<uint32_t, kj::Array<byte>> blocks;
  std::unordered_map<uint32_t, kj::Maybe<kj::Array<byte>>> unsyncedBlocks;

  uint writeCount = 0;
  // Number of writes we'll accept before we stop syncing, effectively snapshotting thne disk
  // at that point.

  kj::Own<kj::PromiseFulfiller<void>> brokenFulfiller;

  void break_() {
    // Randomly sync blocks.
    decltype(unsyncedBlocks) replacement;
    for (auto& block: unsyncedBlocks) {
      if (random.flip()) {
        KJ_IF_MAYBE(b, block.second) {
          blocks[block.first] = kj::mv(*b);
        } else {
          blocks.erase(block.first);
        }
      } else {
        replacement[block.first] = kj::mv(block.second);
      }
    }
    unsyncedBlocks = kj::mv(replacement);
    brokenFulfiller->fulfill();
  }
};

// =======================================================================================
// The fuzzing!

class JournalFuzzer: private kj::TaskSet::ErrorHandler {
public:
  JournalFuzzer(TestJournal::Client journal, Random& random, uint clientId)
      : journal(kj::mv(journal)), random(random), clientId(clientId), tasks(*this) {}

  kj::Promise<void> run() {
    // TODO(now): Do verifications in parallel with transactions.
    kj::Promise<void> promise = random.in(0, 16) == 0 ?
        verify() : doRandomTransaction();

    return promise.then([this]() { return run(); });
  }

private:
  struct ObjectState {
    ObjectState() = default;
    KJ_DISALLOW_COPY(ObjectState);
    ObjectState(ObjectState&&) = default;

    kj::Maybe<TestJournal::Object::Client> cap;
    ObjectId id = nullptr;     // if cap is null but id is non-null, it exists but isn't open
    Xattr xattr;
    kj::Array<byte> content;
  };
  struct TempState {
    TempState() = default;
    KJ_DISALLOW_COPY(TempState);
    TempState(TempState&&) = default;

    kj::Maybe<TestJournal::Object::Client> cap;
    TempLabel xattr;
    kj::Array<byte> content;
  };

  TestJournal::Client journal;
  Random& random;
  uint clientId;
  uint64_t objectCount = 0;
  uint64_t tempCount = 0;

  ObjectState objects[128];
  TempState temps[128];

  kj::TaskSet tasks;

  void logCreate(ObjectId id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(clientId, ": ",
        id, " <= ", sandstorm::hexEncode(xattr), " : ", content.size(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logCreate(uint64_t id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(clientId, ": ", id & 0x0000FFFFFFFFFFFFull,
        " <= ", sandstorm::hexEncode(xattr), " : ", content.size(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logUpdate(ObjectId id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(clientId, ": ",
        id, " <- ", xattr.size() == 0 ? "?" : sandstorm::hexEncode(xattr).cStr(),
            " : ", content.size() == 0 ? "" : kj::str(content.size()).cStr(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logUpdate(uint64_t id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(clientId, ": ", id & 0x0000FFFFFFFFFFFFull,
        " <- ", xattr.size() == 0 ? "?" : sandstorm::hexEncode(xattr).cStr(),
        " : ", content.size() == 0 ? "" : kj::str(content.size()).cStr(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logClose(ObjectId id) {
    auto message = kj::str(clientId, ": ",id, " x\n");
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logDelete(ObjectId id) {
    auto message = kj::str(clientId, ": ",id, " X\n");
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logDelete(uint64_t id) {
    auto message = kj::str(clientId, ": ", id & 0x0000FFFFFFFFFFFFull, " X\n");
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logCommit() {
    auto message = kj::str(clientId, ": COMMIT\n");
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  kj::Promise<void> doRandomTransaction() {
    TestJournal::Transaction::Client txn = journal.newTransactionRequest().send().getTransaction();
    kj::Vector<kj::Promise<void>> promises;

    uint8_t deck[256];
    for (uint i = 0; i < 256; i++) {
      deck[i] = i;
    }

    kj::Maybe<TestJournal::Object::Client> tempToConsume;
    kj::Vector<uint64_t> myTempsBeingDeleted;

    uint count = random.in(0, 16);
    for (uint i = 0; i < count; i++) {
      // Choose a random number in [0,256) that we haven't used already.
      uint deckPos = random.in(i, 256);
      uint index = deck[deckPos];
      deck[deckPos] = deck[i];

      if (index < 128) {
        // Choose an object.
        auto& object = objects[index];

        if (object.cap == nullptr && object.id != nullptr) {
          // Open the object first.
          auto req = journal.openObjectRequest();
          object.id.copyTo(req.initId());
          auto promise = req.send();
          object.cap = promise.getObject();
          promises.add(promise.then([](auto&&) {}));
        }

        KJ_IF_MAYBE(cap, object.cap) {
          // Update existing object!
          if (random.flip()) {
            // Close or delete it.

            if (random.flip()) {
              // Delete it.
              logDelete(object.id);
              auto req = txn.removeRequest();
              req.setObject(kj::mv(*cap));
              promises.add(req.send().then([](auto&&) {}));
              object.id = nullptr;
            } else {
              logClose(object.id);
            }

            object.cap = nullptr;
          } else {
            // Modify it.
            auto req = txn.updateRequest();
            req.setObject(*cap);

            uint choice = random.in(0, 3);  // xattr, content, or both?
            if (choice <= 1) {
              // Update Xattr.
              random.fill(object.xattr);
              memcpy(req.initXattr(sizeof(object.xattr)).begin(),
                     &object.xattr, sizeof(object.xattr));
            }
            if (choice >= 1) {
              // Update content.
              object.content = kj::heapArray<byte>(random.exponential(16));
              random.fill(object.content.begin(), object.content.size());
              req.setContent(object.content);
            }

            logUpdate(object.id, req.getXattr(), req.getContent());

            promises.add(req.send().then([](auto&&) {}));
          }
        } else {
          // Create a new object!
          ObjectKey key;
          memset(&key, 0, sizeof(key));
          key.key[0] = clientId;
          key.key[1] = objectCount++;

          object.id = key;
          random.fill(object.xattr);
          object.content = kj::heapArray<byte>(random.exponential(16));
          random.fill(object.content.begin(), object.content.size());

          auto req = txn.createObjectRequest();
          object.id.copyTo(req.initId());
          memcpy(req.initXattr(sizeof(object.xattr)).begin(), &object.xattr, sizeof(object.xattr));
          req.setContent(object.content);

          logCreate(object.id, req.getXattr(), req.getContent());

          auto promise = req.send();
          object.cap = promise.getObject();
          promises.add(promise.then([](auto&&) {}));
        }
      } else {
        // Choose a temporary.

        index -= 128;

        auto& object = temps[index];

        KJ_IF_MAYBE(cap, object.cap) {
          // Update existing temporary!
          if (random.flip()) {
            // Delete it.
            //
            // Deleting a temporary is actually a matter of dropping the cap, but let's hold on
            // to one cap to consume as part of the transaction, too.
            logDelete(object.xattr.id);
            KJ_ASSERT(tempsBeingDeleted.insert(object.xattr.id).second);
            myTempsBeingDeleted.add(object.xattr.id);

            tempToConsume = kj::mv(*cap);
            object.cap = nullptr;
          } else {
            // Modify it.
            auto req = txn.updateRequest();
            req.setObject(*cap);

            uint choice = random.in(0, 3);  // xattr, content, or both?
            if (choice <= 1) {
              // Update Xattr.
              random.fill(object.xattr.other);
              memcpy(req.initXattr(sizeof(object.xattr)).begin(),
                     &object.xattr, sizeof(object.xattr));
            }
            if (choice >= 1) {
              // Update content.
              object.content = kj::heapArray<byte>(random.exponential(16));
              random.fill(object.content.begin(), object.content.size());
              req.setContent(object.content);
            }

            logUpdate(object.xattr.id, req.getXattr(), req.getContent());

            promises.add(req.send().then([](auto&&) {}));
          }
        } else {
          // Create a new temporary!
          memset(&object.xattr, 0, sizeof(object.xattr));
          object.xattr.id = tempCount++ | (static_cast<uint64_t>(clientId) << 48u);
          random.fill(object.xattr.other);
          object.content = kj::heapArray<byte>(random.exponential(16));
          random.fill(object.content.begin(), object.content.size());

          auto req = txn.createTempRequest();
          memcpy(req.initXattr(sizeof(object.xattr)).begin(), &object.xattr, sizeof(object.xattr));
          req.setContent(object.content);

          logCreate(object.xattr.id, req.getXattr(), req.getContent());

          auto promise = req.send();
          object.cap = promise.getObject();
          promises.add(promise.then([](auto&&) {}));
        }
      }
    }

    return kj::joinPromises(promises.releaseAsArray())
        .then([this,KJ_MVCAP(txn),KJ_MVCAP(tempToConsume),KJ_MVCAP(myTempsBeingDeleted)]() mutable {
      auto req = txn.commitRequest();
      KJ_IF_MAYBE(t, tempToConsume) {
        req.setTempToConsume(kj::mv(*t));
      }
      return req.send().then([this,KJ_MVCAP(myTempsBeingDeleted)](auto&&) {
        logCommit();
        for (auto id: myTempsBeingDeleted) {
          KJ_ASSERT(tempsBeingDeleted.erase(id));
        }
      });
    });
  }

  kj::Promise<void> verify() {
    kj::Vector<kj::Promise<void>> promises;

    for (auto& object: objects) {
      if (object.id != nullptr) {
        TestJournal::Object::Client cap = nullptr;
        KJ_IF_MAYBE(c, object.cap) {
          cap = *c;
        } else {
          auto req = journal.openObjectRequest();
          object.id.copyTo(req.initId());
          cap = req.send().getObject();
        }

        promises.add(cap.readRequest().send().then([&object](auto&& response) {
          auto xattr = response.getXattr();
          KJ_ASSERT(xattr.size() == sizeof(object.xattr) &&
              memcmp(xattr.begin(), &object.xattr, sizeof(object.xattr)) == 0,
              object.id, sandstorm::hexEncode(xattr),
              sandstorm::hexEncode(kj::arrayPtr(&object.xattr, 1).asBytes()));
          auto actualContent = response.getContent();
          KJ_ASSERT(actualContent.size() == object.content.size() &&
              memcmp(actualContent.begin(), object.content.begin(), object.content.size()) == 0,
              object.id, actualContent.size(), object.content.size());
        }));

        auto txn = journal.newTransactionRequest().send().getTransaction();
        auto req = txn.removeRequest();
        req.setObject(kj::mv(cap));
        promises.add(req.send().then([](auto&&) {}));
      }
    }

    for (auto& object: temps) {
      KJ_IF_MAYBE(cap, object.cap) {
        promises.add(cap->readRequest().send().then([&object](auto&& response) {
          auto xattr = response.getXattr();
          KJ_ASSERT(xattr.size() == sizeof(object.xattr) &&
              memcmp(xattr.begin(), &object.xattr, sizeof(object.xattr)) == 0,
              object.xattr.id, sandstorm::hexEncode(xattr),
              sandstorm::hexEncode(kj::arrayPtr(&object.xattr, 1).asBytes()));
          auto actualContent = response.getContent();
          KJ_ASSERT(actualContent.size() == object.content.size() &&
              memcmp(actualContent.begin(), object.content.begin(), object.content.size()) == 0,
              actualContent.size(), object.content.size());
        }));
      }
    }

    return kj::joinPromises(promises.releaseAsArray());
  }

  void taskFailed(kj::Exception&& exception) override {
    KJ_DEFER(abort());
    KJ_LOG(FATAL, exception);
  }
};

// =======================================================================================

class JournalFuzzerMain {
  // A program that tests the journaling code by trying to perform some storage tasks while
  // simulating frequent power failures.
  //
  // This is not a unit test because it takes a long time (the longer you run it, the better, as
  // it randomly tests scenarios) and because it playings with NBD (requiring root privs and
  // possibly screwing with the kernel state -- running in a VM is advised).
  //
  // The fuzzer process forks a child which actually runs the journal code, so that we can kill
  // that child at arbitrary times. We also run the child on top of an nbd device which we can
  // disconnect at arbitrary times, since killing the process alone does not verify that it is
  // correctly syncing underlying storage.

public:
  JournalFuzzerMain(kj::ProcessContext& context): context(context) {
    kj::_::Debug::setLogLevel(kj::LogSeverity::INFO);
  }

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Blackrock storage fuzzer",
          "Randomly fuzzes the storage stack, possibly in an environment involving machine "
          "failures. It is a good idea for <storage-location> to be a ramdisk.")
        .addOption({"direct"}, [this]() {mode=DIRECT;return true;},
            "Test directly against an in-process journal without any failures, to verify that "
            "it produces expected results under the friendliest circumstances.")
        .addOption({"in-proc"}, [this]() {mode=IN_PROC;return true;},
            "Test with the journal in the same process but the recovery layer in-place, but "
            "with no actual failures occurring. If this fails but --direct passes then there "
            "is a bug in the test, not the journal.")
        .addOption({"no-fail"}, [this]() {mode=NO_FAILURES;return true;},
            "Test with the journal in a separate process and the recovery layer in-place, but "
            "with no actual failures occurring. If this fails but --direct passes then there "
            "is a bug in the test, not the journal.")
        .addOption({"proc-fail"}, [this]() {mode=PROCESS_FAILURES;return true;},
            "Test journal behavior in the presence of random process failures, but without "
            "killing the disk. This will not catch failures to sync().")
        .addOption({"mach-fail"}, [this]() {mode=MACHINE_FAILURES;return true;},
            "Test journal behavior in the presence of random power failures, simulated by "
            "testing against an nbd volume that disconnects at the same time the process is "
            "killed. This catches failures to sync(). This requires root privileges. It is "
            "highly recommended that you run it in a virtual machine since it can mess "
            "up the kernel state.")
        .addOption({"child"}, [this]() {mode=CHILD;return true;},
            "Run child process. (For internal use only.)")
        .addOption({"child-nbd"}, [this]() {mode=CHILD_NBD;return true;},
            "Run NBD child process. (For internal use only.)")
        .addOptionWithArg({"clients"}, KJ_BIND_METHOD(*this, setClientCount), "<count>",
            "Run <count> concurrent clients. Default: 8")
        .expectArg("<storage-location>", KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext& context;
  uint clientCount = 8;

  enum {
    UNSET,
    DIRECT,
    IN_PROC,
    NO_FAILURES,
    PROCESS_FAILURES,
    MACHINE_FAILURES,
    CHILD,
    CHILD_NBD
  } mode = UNSET;

  kj::MainBuilder::Validity setClientCount(kj::StringPtr arg) {
    KJ_IF_MAYBE(c, sandstorm::parseUInt(arg, 0)) {
      clientCount = *c;
      return true;
    } else {
      return "couldn't parse integer";
    }
  }

  kj::MainBuilder::Validity run(kj::StringPtr path) {
    switch (mode) {
      case UNSET:
        return "you must specify a mode";
      case DIRECT:
        runDirect(path);
        return true;
      case IN_PROC:
        runInProc(path);
        return true;
      case NO_FAILURES:
        runNoFail(path);
        return true;
      case PROCESS_FAILURES:
        runProcessFailures(path);
        return true;
      case MACHINE_FAILURES:
        runMachineFailures();
        return true;
      case CHILD:
        runChild(path);
        return true;
      case CHILD_NBD:
        runNbdLayer(path);
        return true;
    }
  }

  struct JournalContext {
    kj::AutoCloseFd dirfd;
    LocalBlobLayer blobLayer;
    JournalLayer::Recovery journal;
    TestJournal::Recovery::Client client;

    JournalContext(kj::StringPtr path, kj::AsyncIoContext& ioContext)
        : dirfd(sandstorm::raiiOpen(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC)),
          blobLayer(ioContext.unixEventPort, dirfd),
          journal(blobLayer),
          client(kj::heap<TestJournalRecoveryImpl>(journal)) {}
  };

  struct ChildProc {
    int scratch;
    sandstorm::Pipe pipe;
    sandstorm::Subprocess subprocess;
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyClient rpcSystem;
    TestJournal::Recovery::Client client;

    sandstorm::Subprocess::Options makeSubprocessOptions(
        kj::StringPtr path, kj::Maybe<int> nbdKernelEnd) {
      KJ_IF_MAYBE(fd, nbdKernelEnd) {
        sandstorm::Subprocess::Options result({
            "/proc/self/exe", "--child-nbd", path});
        result.stdin = pipe.readEnd;
        scratch = *fd;
        result.moreFds = kj::arrayPtr(&scratch, 1);
        return result;
      } else {
        sandstorm::Subprocess::Options result({
            "/proc/self/exe", "--child", path});
        result.stdin = pipe.readEnd;
        return result;
      }
    }

    kj::Promise<void> kill() {
      subprocess.signal(SIGKILL);
      (void)subprocess.waitForExitOrSignal();
      return rpcSystem.onDisconnect();
    }

    ChildProc(kj::StringPtr path, kj::AsyncIoContext& ioContext,
              kj::Maybe<int> nbdKernelEnd = nullptr)
        : pipe(sandstorm::Pipe::makeTwoWayAsync()),
          subprocess(makeSubprocessOptions(path, nbdKernelEnd)),
          stream(ioContext.lowLevelProvider->wrapSocketFd(pipe.writeEnd,
              kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
              kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK)),
          rpcSystem(*stream),
          client(rpcSystem.bootstrap().castAs<TestJournal::Recovery>()) {
      pipe.readEnd = nullptr;
    }
  };

  void runChild(kj::StringPtr path) {
    auto ioContext = kj::setupAsyncIo();
    JournalContext journal(path, ioContext);
    auto socket = ioContext.lowLevelProvider->wrapSocketFd(STDIN_FILENO,
        kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);
    capnp::TwoPartyClient rpc(*socket, journal.client, capnp::rpc::twoparty::Side::SERVER);
    rpc.onDisconnect().wait(ioContext.waitScope);
    KJ_FAIL_ASSERT("client disconnected");
  }

  void runNbdLayer(kj::StringPtr path) {
    kj::UnixEventPort::captureSignal(SIGTERM);
    kj::UnixEventPort::captureSignal(SIGINT);
    auto ioContext = kj::setupAsyncIo();
    sandstorm::SubprocessSet subprocs(ioContext.unixEventPort);

    KJ_SYSCALL(fcntl(3, F_SETFD, FD_CLOEXEC));

    KJ_SYSCALL(unshare(CLONE_NEWNS));
    KJ_SYSCALL(mount("none", "/", nullptr, MS_REC | MS_PRIVATE, nullptr));

    NbdDevice device;
    NbdBinding binding(device, kj::AutoCloseFd(3), NbdAccessType::READ_WRITE);
    if (path == "format") device.format();
    Mount mount(device.getPath(), "/mnt", MS_NOATIME, "discard");

    sandstorm::Subprocess child2({"/proc/self/exe", "--child", "/mnt"});
    auto task = ioContext.unixEventPort.onSignal(SIGTERM)
        .exclusiveJoin(ioContext.unixEventPort.onSignal(SIGINT))
        .then([&](siginfo_t) {
      child2.signal(SIGKILL);
    }).eagerlyEvaluate(nullptr);

    subprocs.waitForExitOrSignal(child2).wait(ioContext.waitScope);
  }

  void runDirect(kj::StringPtr path) {
    KJ_SYSCALL(mkdir(path.cStr(), 0700));

    auto ioContext = kj::setupAsyncIo();
    JournalContext journal(path, ioContext);
    doFuzzer(journal.client.recoverRequest().send().getJournal())
        .catch_([&](kj::Exception&& e) {
      context.exitError(kj::str(e));
    }).wait(ioContext.waitScope);
  }

  void runInProc(kj::StringPtr path) {
    KJ_SYSCALL(mkdir(path.cStr(), 0700));

    auto ioContext = kj::setupAsyncIo();
    JournalContext journal(path, ioContext);
    RecoverySet recoverySet;
    doFuzzer(recoverySet.wrap(journal.client.recoverRequest().send().getJournal()))
        .catch_([&](kj::Exception&& e) {
      context.exitError(kj::str(e));
    }).wait(ioContext.waitScope);
  }

  void runNoFail(kj::StringPtr path) {
    KJ_SYSCALL(mkdir(path.cStr(), 0700));

    auto ioContext = kj::setupAsyncIo();
    ChildProc child(path, ioContext);
    RecoverySet recoverySet;
    doFuzzer(recoverySet.wrap(child.client.recoverRequest().send().getJournal()))
        .catch_([&](kj::Exception&& e) {
      context.exitError(kj::str(e));
    }).wait(ioContext.waitScope);
  }

  static kj::Promise<void> loop(kj::Function<kj::Promise<void>()>&& func) {
    auto promise = func();
    return promise.then([KJ_MVCAP(func)]() mutable {
      return loop(kj::mv(func));
    });
  }

  void runProcessFailures(kj::StringPtr path) {
    KJ_SYSCALL(mkdir(path.cStr(), 0700));

    auto ioContext = kj::setupAsyncIo();
    auto child = kj::heap<ChildProc>(path, ioContext);
    RecoverySet recoverySet;
    auto client = recoverySet.wrap(child->client.recoverRequest().send().getJournal());

    Random random(time(nullptr));
    uint counter = 0;
    auto killer = loop([&]() {
      return ioContext.lowLevelProvider->getTimer()
          .afterDelay(random.exponential(14) * kj::MILLISECONDS)
          .then([&]() mutable {
        context.warning(kj::str("**** KILL KILL KILL ", counter++, " ****"));
        kj::StringPtr msg("**** KILL KILL KILL ****\n");
        kj::FdOutputStream(STDOUT_FILENO).write(msg.begin(), msg.size());
        return child->kill();
      }).then([&]() {
        child = nullptr;
        child = kj::heap<ChildProc>(path, ioContext);
        return recoverySet.recover(child->client);
      });
    }).eagerlyEvaluate([this](kj::Exception&& e) {
      context.exitError(kj::str(e));
    });

    doFuzzer(client).catch_([&](kj::Exception&& e) {
      context.exitError(kj::str(e));
    }).wait(ioContext.waitScope);
  }

  struct NbdChildProc {
    kj::AutoCloseFd userEnd;
    NbdVolumeAdapter nbdAdapter;
    kj::ForkedPromise<void> runTask;
    ChildProc child;
    kj::ForkedPromise<int> childTask;

    kj::Promise<void> kill() {
      child.subprocess.signal(SIGTERM);
      return childTask.addBranch().then([this](int) {
        return child.rpcSystem.onDisconnect();
      }).then([this]() {
        return runTask.addBranch();
      });
    }

    NbdChildProc(kj::AsyncIoContext& ioContext, Volume::Client volume, bool format,
                 sandstorm::SubprocessSet& subprocs,
                 sandstorm::Pipe socketpair = sandstorm::Pipe::makeTwoWayAsync())
        : userEnd(kj::mv(socketpair.readEnd)),
          nbdAdapter(
              ioContext.lowLevelProvider->wrapSocketFd(userEnd.get(),
                  kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
                  kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK),
              kj::mv(volume), NbdAccessType::READ_WRITE),
          runTask(nbdAdapter.run().eagerlyEvaluate([](kj::Exception&& e) {
            KJ_LOG(ERROR, e);
          }).fork()),
          child(format ? "format" : "", ioContext, socketpair.writeEnd.get()),
          childTask(subprocs.waitForExitOrSignal(child.subprocess).fork()) {}
  };

  void runMachineFailures() {
    NbdDevice::loadKernelModule();

    kj::UnixEventPort::captureSignal(SIGINT);
    auto ioContext = kj::setupAsyncIo();
    sandstorm::SubprocessSet subprocs(ioContext.unixEventPort);

    Random random(time(nullptr));
    kj::Own<RamVolume> volume = kj::refcounted<RamVolume>(
        ioContext.lowLevelProvider->getTimer(), random);
    auto child = kj::heap<NbdChildProc>(ioContext, kj::addRef(*volume), true, subprocs);
    RecoverySet recoverySet;
    auto client = recoverySet.wrap(child->child.client.recoverRequest().send().getJournal());
    kj::Promise<void> volumeRunner = volume->runFor(500);

    uint counter = 0;
    auto killer = loop([&]() {
      return volumeRunner.then([&]() mutable {
        context.warning(kj::str("**** KILL KILL KILL ", counter++, " ****"));
        kj::StringPtr msg("**** KILL KILL KILL ****\n");
        kj::FdOutputStream(STDOUT_FILENO).write(msg.begin(), msg.size());
        return child->kill();
      }).then([&]() {
        child = nullptr;

        volumeRunner = volume->runFor(random.exponential(4, 11) + 1);

        child = kj::heap<NbdChildProc>(ioContext, kj::addRef(*volume), false, subprocs);
        return recoverySet.recover(child->child.client);
      });
    }).eagerlyEvaluate([this](kj::Exception&& e) {
      context.exitError(kj::str(e));
    });

    doFuzzer(client).catch_([&](kj::Exception&& e) {
      context.error(kj::str(e));
      return child->kill();
    }).exclusiveJoin(ioContext.unixEventPort.onSignal(SIGINT).then([&](siginfo_t) {
      context.error("interrupted, attempting shutdown...");
      return child->kill();
    })).wait(ioContext.waitScope);

    context.exitInfo("exited (gracefully)");
  }

  kj::Promise<void> doFuzzer(TestJournal::Client client) {
    auto promises = KJ_MAP(i, kj::range<uint>(0, clientCount)) -> kj::Promise<void> {
      auto random = kj::heap<Random>(i);
      auto fuzzer = kj::heap<JournalFuzzer>(client, *random, i);
      auto promise = fuzzer->run();
      return promise.attach(kj::mv(random), kj::mv(fuzzer));
    };

    kj::Promise<void> result = kj::NEVER_DONE;
    for (auto& promise: promises) {
      result = result.exclusiveJoin(kj::mv(promise));
    }
    return kj::mv(result);
  }
};

}  // namespace
}  // namespace storage
}  // namespace blackrock

KJ_MAIN(blackrock::storage::JournalFuzzerMain);
