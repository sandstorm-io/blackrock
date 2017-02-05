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

#include "local-persistent-registry.h"
#include <sodium/randombytes.h>
#include <kj/debug.h>

namespace blackrock {

class LocalPersistentRegistry::PersistentImpl: public Persistent::Server, public kj::Refcounted {
public:
  PersistentImpl(Registration& registration, capnp::Capability::Client inner)
      : registry(registration.registry), registration(registration),
        inner(kj::mv(inner)) {}

  void unregister() {
    registration = nullptr;
  }

  kj::Promise<void> dispatchCall(uint64_t interfaceId, uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    // TODO(perf): We need a better way to check if a method is implemented locally. Here we
    //   attempt a local call and catch UNIMPLEMENTED exceptions, but constructing exceptions is
    //   slow due to string manipulation (even though no actual throw/catch will take place here).
    return Persistent::Server::dispatchCall(interfaceId, methodId, context)
        .catch_([=](kj::Exception&& e) mutable -> kj::Promise<void> {
      if (e.getType() == kj::Exception::Type::UNIMPLEMENTED) {
        auto params = context.getParams();
        auto req = inner.typelessRequest(interfaceId, methodId, params.targetSize());
        req.set(params);
        return context.tailCall(kj::mv(req));
      } else {
        return kj::mv(e);
      }
    });
  }

  kj::Promise<void> save(SaveContext context) override {
    // TODO(security): Pay attention to `sealFor`.
    context.releaseParams();

    auto ref = context.getResults(capnp::MessageSize {16, 0}).initSturdyRef().initTransient();
    ref.setVat(registry.thisVatPath);

    KJ_IF_MAYBE(reg, registration) {
      auto savedRef = kj::heap<SavedRef>(*reg);
      ref.getLocalRef().setAs<capnp::Data>(kj::ArrayPtr<byte>(savedRef->token));
      reg->registry.savedRefs[kj::ArrayPtr<byte>(savedRef->token)] = kj::mv(savedRef);
    } else {
      ref.getLocalRef().initAs<capnp::Data>(sizeof(SavedRef::token));
    }
    return kj::READY_NOW;
  }

private:
  LocalPersistentRegistry& registry;
  kj::Maybe<Registration&> registration;
  capnp::Capability::Client inner;
};

LocalPersistentRegistry::Registration::Registration(
    LocalPersistentRegistry& registry, capnp::Capability::Client cap)
    : registry(registry), wrapped(kj::refcounted<PersistentImpl>(*this, kj::mv(cap))) {}

LocalPersistentRegistry::Registration::~Registration() noexcept(false) {
  wrapped->unregister();

  for (auto ref: savedRefs) {
    // Note: This actually deletes the ref.
    registry.savedRefs.erase(kj::ArrayPtr<byte>(ref->token));
  }
}

Persistent::Client LocalPersistentRegistry::Registration::getWrapped() {
  return kj::addRef(*wrapped);
}

LocalPersistentRegistry::SavedRef::SavedRef(Registration& registration)
    : registration(registration) {
  randombytes_buf(token, sizeof(token));
  registration.savedRefs.insert(this);
}

LocalPersistentRegistry::SavedRef::~SavedRef() noexcept(false) {
  registration.savedRefs.erase(this);
}

kj::Own<LocalPersistentRegistry::Registration>
LocalPersistentRegistry::makePersistent(capnp::Capability::Client cap) {
  return kj::heap<Registration>(*this, kj::mv(cap));
}

// =======================================================================================

class LocalPersistentRegistry::RestorerImpl: public Restorer<capnp::Data>::Server {
public:
  RestorerImpl(LocalPersistentRegistry& registry, VatPath::Reader clientId)
      : registry(registry), clientId(clientId.totalSize().wordCount + 4) {
    this->clientId.setRoot(clientId);
  }

protected:
  kj::Promise<void> restore(RestoreContext context) override {
    auto iter = registry.savedRefs.find(context.getParams().getSturdyRef());
    KJ_REQUIRE(iter != registry.savedRefs.end(),
        "requested local SturdyRef doesn't exist; maybe the object was deleted");
    context.releaseParams();

    SavedRef& savedRef = *iter->second;
    context.getResults(capnp::MessageSize { 4, 1 }).setCap(savedRef.registration.getWrapped());
    return kj::READY_NOW;
  }

  kj::Promise<void> drop(DropContext context) override {
    registry.savedRefs.erase(context.getParams().getSturdyRef());
    return kj::READY_NOW;
  }

private:
  LocalPersistentRegistry& registry;
  capnp::MallocMessageBuilder clientId;
};

Restorer<capnp::Data>::Client
LocalPersistentRegistry::createRestorerFor(VatPath::Reader clientId) {
  return kj::heap<RestorerImpl>(*this, clientId);
}

} // namespace blackrock
