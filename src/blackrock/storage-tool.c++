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
        .addOption({"r2k"}, KJ_BIND_METHOD(*this, rootToKey), "root to key")
        .addOption({"k2i"}, KJ_BIND_METHOD(*this, keyToId), "key to id")
        .addOption({"i2f"}, KJ_BIND_METHOD(*this, idToFilename), "id to filename")
        .addOption({"user"}, KJ_BIND_METHOD(*this, dumpStoredUser), "dump user account")
        .callAfterParsing(KJ_BIND_METHOD(*this, done))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::Array<byte> input;
  bool read = false;

  void ensureRead() {
    if (!read) {
      input = kj::heapArray(sandstorm::readAll(STDIN_FILENO).asBytes());
      read = true;
    }
  }

  bool rootToKey() {
    ensureRead();
    capnp::FlatArrayMessageReader reader(
        kj::ArrayPtr<capnp::word>(reinterpret_cast<capnp::word*>(input.begin()),
                                  input.size() * sizeof(capnp::word)));
    FilesystemStorage::ObjectKey key(reader.getRoot<StoredRoot>().getKey());
    input = kj::heapArray(kj::arrayPtr(&key, 1).asBytes());
    return true;
  }

  bool keyToId() {
    ensureRead();
    KJ_ASSERT(input.size() == sizeof(FilesystemStorage::ObjectKey));
    FilesystemStorage::ObjectId id =
        *reinterpret_cast<FilesystemStorage::ObjectKey*>(input.begin());
    input = kj::heapArray(kj::arrayPtr(&id, 1).asBytes());
    return true;
  }

  bool idToFilename() {
    ensureRead();
    KJ_ASSERT(input.size() == sizeof(FilesystemStorage::ObjectId));
    auto fn = reinterpret_cast<FilesystemStorage::ObjectId*>(input.begin())->filename('o');
    input = kj::heapArray(kj::StringPtr(fn.begin()).asBytes());
    return true;
  }

  bool dumpStoredUser() {
    ensureRead();
    kj::ArrayPtr<const capnp::word> words(reinterpret_cast<capnp::word*>(input.begin()),
                                          input.size() * sizeof(capnp::word));
    capnp::FlatArrayMessageReader reader(words);
    auto children = KJ_MAP(i, reader.getRoot<StoredChildIds>().getChildren()) {
      return kj::heapString(FilesystemStorage::ObjectId(i).filename('o').begin());
    };
    KJ_DBG(children);

    words = kj::arrayPtr(reader.getEnd(), words.end());
    capnp::FlatArrayMessageReader reader2(words);
    auto obj = reader2.getRoot<StoredObject>();
    reader2.initCapTable(
        KJ_MAP(dummy, obj.getCapTable()) -> kj::Maybe<kj::Own<capnp::ClientHook>> {
          return capnp::newBrokenCap("dummy cap");
        });
    KJ_DBG(capnp::prettyPrint(obj));

    auto account = obj.getPayload().getAs<AccountStorage>();
    KJ_DBG(capnp::prettyPrint(account));
    return true;
  }

  bool done() {
    ensureRead();
    kj::FdOutputStream(STDOUT_FILENO).write(input.begin(), input.size());
    if (isatty(STDOUT_FILENO)) kj::FdOutputStream(STDOUT_FILENO).write("\n", 1);
    return true;
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::StorageTool)
