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
      context.getResults(capnp::MessageSize { 4, 1 }).setObject(
          wrap(kj::mv(KJ_REQUIRE_NONNULL(maybeObject, id))));
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

      auto cap = kj::heap(params.getTempToConsume());
      auto promise = journal.objects.getLocalServer(*cap);
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

private:
  RecoverySet& recoverySet;
  uint generation = 0;
  bool waitingForReconnect = false;
  capnp::Capability::Client inner;
  kj::Own<kj::PromiseFulfiller<void>> reconnectDoneFulfiller;
  kj::ForkedPromise<void> reconnectDonePromise = nullptr;

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

class RecoverableTemp: public RecoverableCapability, public TestJournal::Object::Server {
public:
  RecoverableTemp(RecoverySet& recoverySet, uint64_t id, TestJournal::Object::Client inner)
      : RecoverableCapability(recoverySet, kj::mv(inner)), id(id) {}

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
    TempLabel label;
    memset(&label, 0, sizeof(label));
    label.isTransaction = false;
    label.id = id;
    auto iter = recovered.find(label);
    KJ_ASSERT(iter != recovered.end(), "temporary was not recovered");
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
      : RecoverableCapability(recoverySet, kj::mv(inner)), id(id),
        tasks(*this) {}

  void recover(TestJournal::Client journal,
               std::map<TempLabel, TempClient>& recovered) override {
    auto newInner = journal.newTransactionRequest().send().getTransaction();

    if (commitCalled && !committed) {
      // Check if commit succeeded.
      TempLabel label;
      memset(&label, 0, sizeof(label));
      label.isTransaction = true;
      label.id = id;
      auto iter = recovered.find(label);
      if (iter != recovered.end()) {
        committed = true;
        recovered.erase(iter);
      }
    }

    if (!committed) {
      // Repelay previous calls before continuing current ones.
      for (auto& call: calls) {
        auto req = newInner.typelessRequest(call.interfaceId, call.methodId, nullptr);
        req.setAs<capnp::AnyStruct>(call.params.getAsReader<capnp::AnyStruct>());
        tasks.add(req.send().then([](auto) {}));
      }
    }

    setInner(kj::mv(newInner));
  }

  kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    KJ_REQUIRE(!commitCalled);

    bool isCommit = interfaceId == capnp::typeId<TestJournal::Transaction>() &&
                    methodId == 4;
    commitCalled = isCommit;

    ReplayableCall replay = {
      interfaceId, methodId,
      arena.getOrphanage().newOrphanCopy(context.getParams())
    };

    return doCall(interfaceId, methodId, context).then([this,isCommit,KJ_MVCAP(replay)]() mutable {
      if (isCommit) {
        committed = true;
        calls = kj::Vector<ReplayableCall>();
      } else {
        calls.add(kj::mv(replay));
      }
    });
  }

private:
  uint64_t id;

  struct ReplayableCall {
    uint64_t interfaceId;
    uint16_t methodId;
    capnp::Orphan<capnp::AnyPointer> params;
  };

  capnp::MallocMessageBuilder arena;
  kj::Vector<ReplayableCall> calls;
  bool committed = false;
  bool commitCalled = false;

  kj::TaskSet tasks;

  void taskFailed(kj::Exception&& exception) override {
    if (exception.getType() != kj::Exception::Type::DISCONNECTED) {
      KJ_DEFER(abort());
      KJ_LOG(FATAL, exception);
    }
  }
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
        KJ_ASSERT(entry.first.isTransaction,
            "recovered unexpected temporaries");
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

      default:
        KJ_FAIL_ASSERT("unknown method", creatingInterfaceId, creatingMethodId);
    }
  }

  capnp::Capability::Client unwrap(capnp::Capability::Client outer) {
    auto cap = kj::heap(outer);
    auto promise = serverSet.getLocalServer(*cap);
    return promise.then([KJ_MVCAP(cap)](kj::Maybe<capnp::Capability::Server&>&& unwrapped) {
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

// =======================================================================================
// The fuzzing!

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
    return result >> (64 - maxBits);
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
    auto message = kj::str(id, " <= ", sandstorm::hexEncode(xattr), " : ", content.size(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logCreate(uint id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(id, " <= ", sandstorm::hexEncode(xattr), " : ", content.size(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logUpdate(ObjectId id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(
        id, " <- ", xattr.size() == 0 ? "?" : sandstorm::hexEncode(xattr).cStr(),
            " : ", content.size() == 0 ? "" : kj::str(content.size()).cStr(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logUpdate(uint id, capnp::Data::Reader xattr, capnp::Data::Reader content) {
    auto message = kj::str(
        id, " <- ", xattr.size() == 0 ? "?" : sandstorm::hexEncode(xattr).cStr(),
            " : ", content.size() == 0 ? "" : kj::str(content.size()).cStr(), '\n');
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logDelete(ObjectId id) {
    auto message = kj::str(id, " X\n");
    kj::FdOutputStream(STDOUT_FILENO).write(message.begin(), message.size());
  }

  void logDelete(uint id) {
    auto message = kj::str(id, " X\n");
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

            logDelete(object.id);

            if (random.flip()) {
              // Delete it.
              auto req = txn.removeRequest();
              req.setObject(kj::mv(*cap));
              promises.add(req.send().then([](auto&&) {}));
              object.id = nullptr;
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
          object.xattr.id = tempCount++;
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
        .then([KJ_MVCAP(txn),KJ_MVCAP(tempToConsume)]() mutable {
      auto req = txn.commitRequest();
      KJ_IF_MAYBE(t, tempToConsume) {
        req.setTempToConsume(kj::mv(*t));
      }
      return req.send().then([](auto&&) {});
    });
  }

  kj::Promise<void> verify() {
    kj::Vector<kj::Promise<void>> promises;

    for (auto& object: objects) {
      if (object.id != nullptr) {
        TestJournal::Object::Client cap = nullptr;
        KJ_IF_MAYBE(c, object.cap) {
          cap = kj::mv(*c);
          object.cap = nullptr;
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
        object.cap = nullptr;
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
  JournalFuzzerMain(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Blackrock storage fuzzer",
          "Randomly fuzzes the storage stack, possibly in an environment involving machine "
          "failures. It is a good idea for <storage-location> to be a ramdisk.")
        .addOption({"direct"}, [this]() {mode=DIRECT;return true;},
            "Test directly against an in-process journal without any failures, to verify that "
            "it produces expected results under the friendliest circumstances.")
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
        .expectArg("<storage-location>", KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext& context;

  enum {
    UNSET,
    DIRECT,
    NO_FAILURES,
    PROCESS_FAILURES,
    MACHINE_FAILURES
  } mode = UNSET;

  kj::MainBuilder::Validity run(kj::StringPtr path) {
    switch (mode) {
      case UNSET:
        return "you must specify a mode";
      case DIRECT:
        runDirect(path);
        return true;
      case NO_FAILURES:
      case PROCESS_FAILURES:
      case MACHINE_FAILURES:
        KJ_UNIMPLEMENTED("todo");
    }
  }

  void runDirect(kj::StringPtr path) {
    KJ_SYSCALL(mkdir(path.cStr(), 0700));
    KJ_DEFER(sandstorm::recursivelyDelete(path));
    auto dirfd = sandstorm::raiiOpen(path, O_RDONLY | O_DIRECTORY);

    auto ioContext = kj::setupAsyncIo();
    LocalBlobLayer blobLayer(ioContext.unixEventPort, dirfd);
    JournalLayer::Recovery journal(blobLayer);

    KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
      TestJournal::Recovery::Client client = kj::heap<TestJournalRecoveryImpl>(journal);
      auto journalClient = client.recoverRequest().send().getJournal();

      auto random = kj::heap<Random>(0);

      // TODO(now): multiple fuzzer "threads"
      JournalFuzzer fuzzer(journalClient, *random, 0);
      fuzzer.run().wait(ioContext.waitScope);
    })) {
      // Avoid trying to unwind past this point because there could be tasks on the event queue
      // still which can't be destroyed cleanly.
      context.exitError(kj::str(*exception));
    }
  }
};

}  // namespace
}  // namespace storage
}  // namespace blackrock

KJ_MAIN(blackrock::storage::JournalFuzzerMain);
