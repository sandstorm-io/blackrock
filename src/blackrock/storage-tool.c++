// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "fs-storage.h"
#include <blackrock/fs-storage.capnp.h>
#include <blackrock/storage-schema.capnp.h>
#include <kj/main.h>
#include <capnp/serialize.h>
#include <unistd.h>
#include <sandstorm/util.h>
#include <capnp/pretty-print.h>

namespace blackrock {

class StorageTool {
public:
  StorageTool(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Blackrock",
          "Tool for probing the contents of Blackrock storage.")
        .expectArg("<userId>", KJ_BIND_METHOD(*this, setUserId))
        .expectArg("<grainId>", KJ_BIND_METHOD(*this, setGrainId))
        .callAfterParsing(KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::StringPtr userId;
  kj::StringPtr grainId;

  typedef FilesystemStorage::ObjectId ObjectId;
  typedef FilesystemStorage::ObjectKey ObjectKey;

  class RawClientHook: public capnp::ClientHook, public kj::Refcounted {
  public:
    explicit RawClientHook(StoredObject::CapDescriptor::Reader descriptor)
        : descriptor(descriptor) {}

    StoredObject::CapDescriptor::Reader descriptor;

    capnp::Request<capnp::AnyPointer, capnp::AnyPointer> newCall(
        uint64_t interfaceId, uint16_t methodId, kj::Maybe<capnp::MessageSize> sizeHint) override {
      KJ_UNIMPLEMENTED("RawClientHook doesn't implement anything");
    }

    VoidPromiseAndPipeline call(uint64_t interfaceId, uint16_t methodId,
                                kj::Own<capnp::CallContextHook>&& context) override {
      KJ_UNIMPLEMENTED("RawClientHook doesn't implement anything");
    }

    kj::Maybe<ClientHook&> getResolved() override {
      KJ_UNIMPLEMENTED("RawClientHook doesn't implement anything");
    }

    kj::Maybe<kj::Promise<kj::Own<ClientHook>>> whenMoreResolved() override {
      KJ_UNIMPLEMENTED("RawClientHook doesn't implement anything");
    }

    kj::Own<ClientHook> addRef() override {
      return kj::addRef(*this);
    }

    const void* getBrand() override {
      return nullptr;
    }
  };

  ObjectKey getUser(kj::StringPtr userId) {
    capnp::StreamFdMessageReader reader(sandstorm::raiiOpen(
        kj::str("roots/user-", userId), O_RDONLY));
    return reader.getRoot<StoredRoot>().getKey();
  }

  ObjectKey getGrain(ObjectKey user, kj::StringPtr grainId) {
    auto fd = sandstorm::raiiOpen(kj::str("main/", ObjectId(user).filename('o').begin()), O_RDONLY);

    auto children = ({
      capnp::StreamFdMessageReader reader(fd.get());
      KJ_MAP(c, reader.getRoot<StoredChildIds>().getChildren()) -> ObjectId { return c; };
    });

    capnp::StreamFdMessageReader reader(fd.get());
    auto object = reader.getRoot<StoredObject>();

    capnp::ReaderCapabilityTable capTable(KJ_MAP(cap, object.getCapTable())
        -> kj::Maybe<kj::Own<capnp::ClientHook>> {
      return kj::Own<capnp::ClientHook>(kj::refcounted<RawClientHook>(cap));
    });

    auto imbued = capTable.imbue(object);

    for (auto grain: imbued.getPayload().getAs<AccountStorage>().getGrains()) {
      if (grain.getId() == grainId) {
        auto descriptor = capnp::ClientHook::from(grain.getState())
            .downcast<RawClientHook>()->descriptor;
        KJ_ASSERT(descriptor.isChild(), descriptor);
        return descriptor.getChild();
      }
    }

    KJ_FAIL_REQUIRE("user had no such grain");
  }

  bool setUserId(kj::StringPtr arg) {
    userId = arg;
    return true;
  }

  bool setGrainId(kj::StringPtr arg) {
    grainId = arg;
    return true;
  }

  bool run() {
    context.exitInfo(ObjectId(getGrain(getUser(userId), grainId)).filename('o').begin());
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::StorageTool)
