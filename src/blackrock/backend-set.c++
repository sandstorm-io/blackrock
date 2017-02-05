// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// =======================================================================================

class BackendSetFeederBase::ConsumerRegistration final: public Registration {
public:
  ConsumerRegistration(BackendSetFeederBase& feeder, BackendSet<>::Client set);

  ~ConsumerRegistration() noexcept(false);

private:
  friend class BackendSetFeederBase;

  BackendSetFeederBase& feeder;
  BackendSet<>::Client set;
  ConsumerRegistration* next;
  ConsumerRegistration** prev;

  void init();
};

class BackendSetFeederBase::BackendRegistration final: public Registration {
public:
  BackendRegistration(BackendSetFeederBase& feeder, capnp::Capability::Client cap);

  ~BackendRegistration() noexcept(false);

private:
  friend class BackendSetFeederBase;

  BackendSetFeederBase& feeder;
  uint64_t id;
  capnp::Capability::Client cap;
  BackendRegistration* next;
  BackendRegistration** prev;
};

auto BackendSetFeederBase::addBackend(capnp::Capability::Client cap) -> kj::Own<Registration> {
  auto result = kj::heap<BackendRegistration>(*this, kj::mv(cap));

  if (ready) {
    // Consumers are already initialized. Add the new backend to each one.
    for (ConsumerRegistration* consumer = consumersHead; consumer != nullptr;
         consumer = consumer->next) {
      tasks.add(kj::evalNow([&]() {
        auto req = consumer->set.addRequest(capnp::MessageSize {4, 0});
        req.setId(result->id);
        req.getBackend().setAs<capnp::Capability>(result->cap);
        return req.send().then([](auto&&) {});
      }));
    }
  } else if (backendCount >= minCount) {
    // We have enough backends to initialize all consumers.
    ready = true;
    for (ConsumerRegistration* consumer = consumersHead; consumer != nullptr;
         consumer = consumer->next) {
      consumer->init();
    }
  }

  return kj::mv(result);
}

auto BackendSetFeederBase::addConsumer(BackendSet<>::Client set) -> kj::Own<Registration> {
  auto result = kj::heap<ConsumerRegistration>(*this, kj::mv(set));

  if (ready) {
    // We already have all the backends we need, so go ahead and initialize the consumer.
    result->init();
  }

  return kj::mv(result);
}

void BackendSetFeederBase::taskFailed(kj::Exception&& exception) {
  KJ_LOG(ERROR, exception);
}

BackendSetFeederBase::Registration::~Registration() noexcept(false) {}

BackendSetFeederBase::ConsumerRegistration::ConsumerRegistration(
    BackendSetFeederBase& feeder, BackendSet<>::Client set)
    : feeder(feeder), set(kj::mv(set)),
      next(nullptr), prev(feeder.consumersTail) {
  *feeder.consumersTail = this;
  feeder.consumersTail = &next;
}

BackendSetFeederBase::ConsumerRegistration::~ConsumerRegistration() noexcept(false) {
  if (next == nullptr) {
    feeder.consumersTail = prev;
  } else {
    next->prev = prev;
  }
  *prev = next;
}

void BackendSetFeederBase::ConsumerRegistration::init() {
  auto req = set.resetRequest();
  auto list = req.initBackends(feeder.backendCount);
  uint i = 0;
  for (BackendRegistration* backend = feeder.backendsHead; backend != nullptr;
       backend = backend->next) {
    auto element = list[i++];
    element.setId(backend->id);
    element.getBackend().setAs<capnp::Capability>(backend->cap);
  }
  feeder.tasks.add(req.send().then([](auto&&) {}));
}

BackendSetFeederBase::BackendRegistration::BackendRegistration(
    BackendSetFeederBase& feeder, capnp::Capability::Client cap)
    : feeder(feeder), id(feeder.nextId++), cap(kj::mv(cap)),
      next(nullptr), prev(feeder.backendsTail) {
  *feeder.backendsTail = this;
  feeder.backendsTail = &next;
  ++feeder.backendCount;
}

BackendSetFeederBase::BackendRegistration::~BackendRegistration() noexcept(false) {
  --feeder.backendCount;
  if (next == nullptr) {
    feeder.backendsTail = prev;
  } else {
    next->prev = prev;
  }
  *prev = next;

  // Remove from all consumers.
  for (ConsumerRegistration* consumer = feeder.consumersHead; consumer != nullptr;
       consumer = consumer->next) {
    feeder.tasks.add(kj::evalNow([&]() {
      auto req = consumer->set.removeRequest(capnp::MessageSize {4, 0});
      req.setId(id);
      return req.send().then([](auto&&) {});
    }));
  }
}

} // namespace blackrock

