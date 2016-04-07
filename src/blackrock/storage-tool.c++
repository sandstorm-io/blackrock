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
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/xattr.h>

namespace blackrock {

class StorageTool {
public:
  StorageTool(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Blackrock",
          "Tool for probing the contents of Blackrock storage.")
        .addOption({'f', "fix"}, KJ_BIND_METHOD(*this, setFix), "fix it")
        .expectArg("<userId>", KJ_BIND_METHOD(*this, setUserId))
        .expectArg("<grainId>", KJ_BIND_METHOD(*this, setGrainId))
        .callAfterParsing(KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::StringPtr userId;
  kj::StringPtr grainId;
  bool fix = false;

  typedef FilesystemStorage::ObjectId ObjectId;
  typedef FilesystemStorage::ObjectKey ObjectKey;

  enum class Type: uint8_t {
    // (zero skipped to help detect errors)
    BLOB = 1,
    VOLUME,
    IMMUTABLE,
    ASSIGNABLE,
    COLLECTION,
    OPAQUE,
    REFERENCE
  };

  struct Xattr {
    // Format of the xattr block stored on each file. On ext4 we have about 76 bytes available in
    // the inode to store this attribute, but in theory this space could get smaller in the future,
    // so we should try to keep this minimal.

    static constexpr const char* NAME = "user.sandstor";
    // Extended attribute name. Abbreviated to be 8 bytes to avoid losing space to alignment (ext4
    // doesn't store the "user." prefix). Actually short for "sandstore", not "sandstorm". :)

    Type type;

    bool readOnly;
    // For volumes, prevents the volume from being modified. For Blobs, indicates that initialization
    // has completed with a `done()` call, indicating the entire stream was received (otherwise,
    // either the stream is still uploading, or it failed to fully upload). Once set this
    // can never be unset.

    byte reserved[2];
    // Must be zero.

    uint32_t accountedBlockCount;
    // The number of 4k blocks consumed by this object the last time we considered it for
    // accounting/quota purposes. The on-disk size could have changed in the meantime.

    uint64_t transitiveBlockCount;
    // The number of 4k blocks in this object and all child objects.

    ObjectId owner;
    // What object owns this one?
  };

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

  ObjectKey getVolume(ObjectKey grain) {
    auto fd = sandstorm::raiiOpen(kj::str(
        "main/", ObjectId(grain).filename('o').begin()), O_RDONLY);

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
    auto volume = imbued.getPayload().getAs<GrainState>().getVolume();
    auto descriptor = capnp::ClientHook::from(volume).downcast<RawClientHook>()->descriptor;
    KJ_ASSERT(descriptor.isChild(), descriptor);
    return descriptor.getChild();
  }

  bool setUserId(kj::StringPtr arg) {
    userId = arg;
    return true;
  }

  bool setGrainId(kj::StringPtr arg) {
    grainId = arg;
    return true;
  }

  bool setFix() {
    fix = true;
    return true;
  }

  bool run() {
    auto grain = getGrain(getUser(userId), grainId);
    auto volume = getVolume(grain);

    auto filename = kj::str("main/", ObjectId(volume).filename('o').begin());
    struct stat stats;
    KJ_SYSCALL(stat(filename.cStr(), &stats));

    Xattr expected;
    memset(&expected, 0, sizeof(expected));
    expected.type = Type::VOLUME;
    expected.accountedBlockCount = stats.st_blocks / 8;
    expected.transitiveBlockCount = expected.accountedBlockCount;
    expected.owner = grain;

    Xattr xattr;
    ssize_t n = getxattr(filename.cStr(), Xattr::NAME, &xattr, sizeof(xattr));
    if (n < 0) {
      context.error(kj::str("missing xattr:", strerror(errno)));

      if (fix) {
        KJ_SYSCALL(setxattr(filename.cStr(), Xattr::NAME, &expected, sizeof(expected), XATTR_CREATE));
      }
    } else if (n != sizeof(xattr)) {
      context.error(kj::str("unexpected xattr size: ", n));
    } else if (memcmp(&xattr, &expected, sizeof(xattr)) != 0) {
      KJ_LOG(ERROR, (uint)expected.type, (uint)xattr.type);
      KJ_LOG(ERROR, expected.accountedBlockCount, xattr.accountedBlockCount);
      KJ_LOG(ERROR, expected.transitiveBlockCount, xattr.transitiveBlockCount);
      KJ_LOG(ERROR, expected.owner.filename('o').begin(), xattr.owner.filename('o').begin());
      context.error("xattrs don't match");

      if (fix) {
        KJ_SYSCALL(setxattr(filename.cStr(), Xattr::NAME, &expected, sizeof(expected), XATTR_REPLACE));
      }
    } else {
      context.warning("xattrs match expected");
    }

    context.exitInfo(kj::str(ObjectId(grain).filename('o').begin(), ' ',
                             ObjectId(volume).filename('o').begin()));
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::StorageTool)
