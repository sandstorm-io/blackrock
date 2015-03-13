// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_VOLUME_H_
#define BLACKROCK_VOLUME_H_

#include "common.h"
#include <blackrock/storage.capnp.h>
#include <blackrock/fs-storage.capnp.h>
#include <kj/io.h>
#include <unordered_map>

namespace blackrock {

class FilesystemStorage {
public:
  explicit FilesystemStorage(int mainDirFd, int stagingDirFd)
      : mainDirFd(mainDirFd), stagingDirFd(stagingDirFd) {}

  StorageZone<>::Client getRoot();

private:
  struct ObjectKey {
    uint64_t key[4];

    ObjectKey() = default;
    ObjectKey(StoredObjectKey::Reader reader);

    static ObjectKey generate();

    inline void copyTo(StoredObjectKey::Builder builder) const {
      builder.setKey0(key[0]);
      builder.setKey1(key[1]);
      builder.setKey2(key[2]);
      builder.setKey3(key[3]);
    }
  };

  struct ObjectId {
    uint64_t id[2];
    // The object ID. Equals the 16-byte blake2b hash of the key.

    ObjectId() = default;
    ObjectId(StoredObjectId::Reader reader);
    ObjectId(const ObjectKey& key);

    inline bool operator==(const ObjectId& other) const {
      return !operator!=(other);
    }
    inline bool operator!=(const ObjectId& other) const {
      return (id[0] ^ other.id[0]) | (id[1] ^ other.id[1]);  // constant-time
    }

    inline void copyTo(StoredObjectId::Builder builder) const {
      builder.setId0(id[0]);
      builder.setId1(id[1]);
    }

    struct Hash {
      inline size_t operator()(const ObjectId& id) const { return id.id[0]; }
    };

    kj::FixedArray<char, 24> filename(char prefix) const;
  };

  int mainDirFd;
  int stagingDirFd;
  kj::Array<ObjectId> path;

  std::unordered_map<ObjectId, capnp::ClientHook*, ObjectId::Hash> children;
  // Maps child object ID -> weak reference to the live capability representing it.

  kj::Maybe<capnp::ClientHook&> factoryCap;
  // Weak reference to the factory capability.

  class ObjectBase;
  class AssignableImpl;
  class VolumeImpl;
  class StorageFactoryImpl;
  enum class Type: uint8_t;
  struct Xattr;
  class Journal;

  kj::Maybe<kj::AutoCloseFd> openObject(ObjectId id);
  kj::Maybe<kj::AutoCloseFd> openStaging(uint32_t number);
  kj::AutoCloseFd createObject(ObjectId id);
  kj::AutoCloseFd createTempFile();
  void linkTempIntoStaging(uint32_t number, int fd);
  void deleteObject(ObjectId id);
  void deleteStaging(uint32_t number);
  void finalizeStagingIfExists(uint32_t stagingId, ObjectId finalId, const Xattr& attributes);
  void setAttributesIfExists(ObjectId objectId, const Xattr& attributes);
  void sync();
};

}  // namespace blackrock

#endif  // BLACKROCK_VOLUME_H_
