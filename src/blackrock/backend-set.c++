// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "backend-set.h"
#include <kj/debug.h>

namespace blackrock {

BackendSetBase::BackendSetBase(kj::PromiseFulfillerPair<void> paf)
    : next(backends.end()),
      readyPromise(paf.promise.fork()),
      readyFulfiller(kj::mv(paf.fulfiller)) {}
BackendSetBase::~BackendSetBase() noexcept(false) {}

capnp::Capability::Client BackendSetBase::chooseOne() {
  if (backends.empty()) {
    return readyPromise.addBranch().then([this]() {
      return chooseOne();
    });
  } else {
    if (next == backends.end()) {
      next = backends.begin();
    }

    return (next++)->second.client;
  }
}

void BackendSetBase::clear() {
  backends.clear();
}

void BackendSetBase::add(uint64_t id, capnp::Capability::Client client) {
  if (backends.empty()) {
    readyFulfiller->fulfill();
  }

  backends.insert(std::make_pair(id, Backend { kj::mv(client) }));
}

void BackendSetBase::remove(uint64_t id) {
  if (next != backends.end() && next->first == id) {
    ++next;
  }
  backends.erase(id);

  if (backends.empty()) {
    auto paf = kj::newPromiseAndFulfiller<void>();
    readyPromise = paf.promise.fork();
    readyFulfiller = kj::mv(paf.fulfiller);
  }
}

} // namespace blackrock

