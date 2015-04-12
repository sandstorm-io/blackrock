// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "backend-set.h"
#include <kj/debug.h>

namespace blackrock {

BackendSetBase::BackendSetBase(): next(backends.end()) {}
BackendSetBase::~BackendSetBase() noexcept(false) {}

capnp::Capability::Client BackendSetBase::chooseOne() {
  if (backends.empty()) {
    // "Overloaded" really means "resources temporarily unavailable". It differs from
    // "disconnected" in that the caller is unlikely to be able to fix the problem by merely
    // re-establishing connections, but it differs from "failed" in that the problem is expected
    // not to be permanent.
    //
    // TODO(cleanup): Rename KJ "overloaded" exception type to make this clearer?
    kj::throwFatalException(KJ_EXCEPTION(OVERLOADED, "no back-end available"));
  }

  if (next == backends.end()) {
    next = backends.begin();
  }

  return (next++)->second.client;
}

void BackendSetBase::clear() {
  backends.clear();
}

void BackendSetBase::add(uint64_t id, capnp::Capability::Client client) {
  backends.insert(std::make_pair(id, Backend { kj::mv(client) }));
}

void BackendSetBase::remove(uint64_t id) {
  if (next != backends.end() && next->first == id) {
    ++next;
  }
  backends.erase(id);
}

} // namespace blackrock

