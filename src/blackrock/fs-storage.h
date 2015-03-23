// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_VOLUME_H_
#define BLACKROCK_VOLUME_H_

#include "common.h"
#include <blackrock/storage.capnp.h>
#include <blackrock/fs-storage.capnp.h>
#include <kj/io.h>

namespace kj {
  class UnixEventPort;
  class Timer;
}

namespace blackrock {

class FilesystemStorage {
public:
  FilesystemStorage(int mainDirFd, int stagingDirFd, int deathRowFd, int journalFd,
                    kj::UnixEventPort& eventPort, kj::Timer& timer,
                    Restorer<SturdyRef>::Client&& restorer);
  ~FilesystemStorage() noexcept(false);

  struct ObjectKey {
    uint64_t key[4];

    ObjectKey() = default;
    ObjectKey(StoredObjectKey::Reader reader)
        : key { reader.getKey0(), reader.getKey1(), reader.getKey2(), reader.getKey3() } {}

    static ObjectKey generate();

    inline void copyTo(StoredObjectKey::Builder builder) const {
      builder.setKey0(key[0]);
      builder.setKey1(key[1]);
      builder.setKey2(key[2]);
      builder.setKey3(key[3]);
    }
  };

  OwnedAssignable<>::Client getRoot(ObjectKey key);
  kj::Promise<ObjectKey> setRoot(OwnedAssignable<>::Client object);
  StorageFactory::Client getFactory();

  template <typename T>
  typename OwnedAssignable<T>::Client getRoot(ObjectKey key) {
    return getRoot(key).castAs<OwnedAssignable<T>>();
  }
  template <typename T>
  kj::Promise<ObjectKey> setRoot(T object) {
    return setRoot(object.template asGeneric<>());
  }

private:
  struct ObjectId {
    uint64_t id[2];
    // The object ID. Equals the 16-byte blake2b hash of the key.

    ObjectId() = default;
    ObjectId(decltype(nullptr)): id {0, 0} {}
    ObjectId(StoredObjectId::Reader reader)
        : id { reader.getId0(), reader.getId1() } {}
    ObjectId(const ObjectKey& key);

    inline bool operator==(const ObjectId& other) const {
      return ((id[0] ^ other.id[0]) | (id[1] ^ other.id[1])) == 0;  // constant-time
    }
    inline bool operator!=(const ObjectId& other) const {
      return !operator==(other);
    }
    inline bool operator==(decltype(nullptr)) const {
      return (id[0] | id[1]) == 0;
    }
    inline bool operator!=(decltype(nullptr)) const {
      return !operator==(nullptr);
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

  class ObjectBase;
  class BlobImpl;
  class VolumeImpl;
  class ImmutableImpl;
  class AssignableImpl;
  class CollectionImpl;
  class OpaqueImpl;
  class StorageFactoryImpl;
  enum class Type: uint8_t;
  struct Xattr;
  class Journal;
  class ObjectFactory;

  int mainDirFd;
  int stagingDirFd;
  int deathRowFd;

  kj::Own<Journal> journal;
  kj::Own<ObjectFactory> factory;

  kj::Maybe<kj::AutoCloseFd> openObject(ObjectId id);
  kj::Maybe<kj::AutoCloseFd> openStaging(uint64_t number);
  kj::AutoCloseFd createObject(ObjectId id);
  kj::AutoCloseFd createTempFile();
  void linkTempIntoStaging(uint64_t number, int fd, const Xattr& xattr);
  void deleteStaging(uint64_t number);
  void deleteAllStaging();
  void finalizeStagingIfExists(uint64_t stagingId, ObjectId finalId, const Xattr& attributes);
  void setAttributesIfExists(ObjectId objectId, const Xattr& attributes);
  void moveToDeathRow(ObjectId id);
  void sync();

  static bool isStoredObjectType(Type type);
};

}  // namespace blackrock

#endif  // BLACKROCK_VOLUME_H_
