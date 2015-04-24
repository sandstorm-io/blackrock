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

} // namespace blackrock

#endif // BLACKROCK_BACKEND_SET_H_
