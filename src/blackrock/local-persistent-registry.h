// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_LOCAL_PERSISTENT_REGISTRY_H_
#define BLACKROCK_LOCAL_PERSISTENT_REGISTRY_H_

#include "common.h"
#include <blackrock/cluster-rpc.capnp.h>
#include <capnp/message.h>
#include <unordered_map>
#include <set>

namespace blackrock {

class LocalPersistentRegistry {
  // Class which manages the set of persistent capabilities hosted by this vat which, when saved,
  // will use the "Transient" SturdyRef type; i.e. these capabilities are specific to this vat and
  // won't continue to exist once the process exits.
  //
  // Typically a LocalPersistentRegistry& should be passed around to any component that needs to be
  // able to make its capabilities persistent. The LocalPersistentRegistry's scope should match the
  // RpcSystem.

  struct SavedRef;
  class PersistentImpl;
  class RestorerImpl;

  struct DataHash {
    inline size_t operator()(capnp::Data::Reader r) const {
      // The keys in the map are randomly-generated so the hash might as well be the prefix bytes.
      size_t result = 0;
      memcpy(&result, r.begin(), kj::min(r.size(), sizeof(result)));
      return result;
    }
  };

public:
  LocalPersistentRegistry(VatPath::Reader thisVatPath): thisVatPath(thisVatPath) {}

  class Registration {
  public:
    Registration(LocalPersistentRegistry& registry, capnp::Capability::Client cap);

    KJ_DISALLOW_COPY(Registration);
    ~Registration() noexcept(false);
    // Dropping the registration invalidates all saved SturdyRefs. Calls to save() will still
    // succeed but return tokens that don't work (as if save() had been called just before the
    // deregistration).

    Persistent::Client getWrapped();
    // Get a capability which forwards all calls to the original except for save() which is handled
    // by the LocalPersistentRegistry.

  private:
    LocalPersistentRegistry& registry;
    kj::Own<PersistentImpl> wrapped;
    std::set<SavedRef*> savedRefs;
    friend class LocalPersistentRegistry;
  };

  kj::Own<Registration> makePersistent(capnp::Capability::Client cap);
  // Wraps the capability in a wrapper that implements save() by returning a transient SturdyRef.

  Restorer<capnp::Data>::Client createRestorerFor(VatPath::Reader clientId);
  // Create a Restorer to be used by the given authenticated client.

private:
  VatPath::Reader thisVatPath;

  struct SavedRef {
    explicit SavedRef(Registration& registration);
    ~SavedRef() noexcept(false);

    Registration& registration;
    byte token[16];

    // TODO(security): Track the ref's owner. This is easy enough when the owner is another vat,
    //   but if it's e.g. the storage system then we don't really have a good way to authenticate
    //   that here.
  };

  std::unordered_map<capnp::Data::Reader, kj::Own<SavedRef>, DataHash> savedRefs;
};

} // namespace blackrock

#endif // BLACKROCK_LOCAL_PERSISTENT_REGISTRY_H_
