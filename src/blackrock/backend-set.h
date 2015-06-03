// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_BACKEND_SET_H_
#define BLACKROCK_BACKEND_SET_H_

#include "common.h"
#include <blackrock/cluster-rpc.capnp.h>
#include <map>

namespace blackrock {

class BackendSetBase {
public:
  BackendSetBase(): BackendSetBase(kj::newPromiseAndFulfiller<void>()) {}
  ~BackendSetBase() noexcept(false);

  capnp::Capability::Client chooseOne();

  void clear();
  void add(uint64_t id, capnp::Capability::Client client);
  void remove(uint64_t id);

private:
  struct Backend {
    capnp::Capability::Client client;

    Backend(Backend&&) = default;
    Backend(const Backend&) = delete;
    // Convince STL to use the move constructor.
  };

  std::map<uint64_t, Backend> backends;
  std::map<uint64_t, Backend>::iterator next;
  kj::ForkedPromise<void> readyPromise;
  kj::Own<kj::PromiseFulfiller<void>> readyFulfiller;

  explicit BackendSetBase(kj::PromiseFulfillerPair<void> paf);
};

template <typename T>
class BackendSetImpl: public BackendSet<T>::Server, public kj::Refcounted {
public:
  typename T::Client chooseOne() { return base.chooseOne().template castAs<T>(); }
  // Choose a capability from the set and return it, cycling through the set every time this
  // method is called. If the backend set is empty, return a promise that resolves once a backend
  // is available.
  //
  // TODO(someady): Would be nice to build in disconnect handling here, e.g. pass in a callback
  //   function that initiates the work, catches exceptions and retries with a different back-end.

protected:
  typedef typename BackendSet<T>::Server Interface;
  kj::Promise<void> reset(typename Interface::ResetContext context) {
    base.clear();
    for (auto backend: context.getParams().getBackends()) {
      base.add(backend.getId(), backend.getBackend());
    }
    return kj::READY_NOW;
  }
  kj::Promise<void> add(typename Interface::AddContext context) {
    auto params = context.getParams();
    base.add(params.getId(), params.getBackend());
    return kj::READY_NOW;
  }
  kj::Promise<void> remove(typename Interface::RemoveContext context) {
    base.remove(context.getParams().getId());
    return kj::READY_NOW;
  }

private:
  BackendSetBase base;
};

// =======================================================================================

class BackendSetFeederBase: private kj::TaskSet::ErrorHandler {
public:
  explicit BackendSetFeederBase(uint minCount): minCount(minCount), tasks(*this) {}
  KJ_DISALLOW_COPY(BackendSetFeederBase);

  class Registration {
  public:
    virtual ~Registration() noexcept(false);
  };

  kj::Own<Registration> addBackend(capnp::Capability::Client cap);
  kj::Own<Registration> addConsumer(BackendSet<>::Client set);

private:
  class BackendRegistration;
  class ConsumerRegistration;

  uint minCount;
  bool ready = minCount == 0;  // Becomes true when minCount backends are first available.
  uint64_t backendCount = 0;
  uint64_t nextId = 0;
  BackendRegistration* backendsHead = nullptr;
  BackendRegistration** backendsTail = &backendsHead;
  ConsumerRegistration* consumersHead = nullptr;
  ConsumerRegistration** consumersTail = &consumersHead;
  kj::TaskSet tasks;

  void taskFailed(kj::Exception&& exception) override;
};

template <typename T>
class BackendSetFeeder final: public BackendSetFeederBase {
  // Manages the process of maintaining BackendSets.
  //
  // A BackendSetFeeder is created by the master machine for each kind of load-balanced set. For
  // example, there is a BackendSetFeeder for StorageRoots. Each StorageRoot capability is added
  // using addBackend(), then each BackendSet which needs to be populated by StorageRoots (e.g.
  // the front-end) is added using addConsumer().

public:
  explicit BackendSetFeeder(uint minCount)
      : BackendSetFeederBase(minCount) {}
  // The feeder will wait until at least minBackendCount backends have been added before it
  // initializes any consumers. This prevents flooding the first machine in a set with traffic
  // while the others are still coming online.

  using BackendSetFeederBase::Registration;

  kj::Own<Registration> addBackend(typename T::Client cap) KJ_WARN_UNUSED_RESULT {
    // Inserts this capability into all consumer sets. When the returned Backend is dropped
    // (indicating that the back-end has disconnected), removes the capability from all consumer
    // sets.
    return BackendSetFeederBase::addBackend(kj::mv(cap));
  }

  kj::Own<Registration> addConsumer(typename BackendSet<T>::Client set) KJ_WARN_UNUSED_RESULT {
    // Inserts all backends into this consumer. When the returned Consumer is dropped (indicating
    // that it has disconnected), stops updating it.
    return BackendSetFeederBase::addConsumer(set.template asGeneric<>());
  }
};

} // namespace blackrock

#endif // BLACKROCK_BACKEND_SET_H_
