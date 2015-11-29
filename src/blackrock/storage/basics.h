// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_BASICS_H_
#define BLACKROCK_STORAGE_BASICS_H_

#include <blackrock/common.h>
#include <blackrock/fs-storage.capnp.h>
#include <sodium/utils.h>

namespace blackrock {
namespace storage {

constexpr uint64_t BLOCK_SIZE = 4096;
#define BLOCK_SIZE BLOCK_SIZE  // watch out for headers that #define this

struct ObjectKey {
  uint64_t key[4];

  ObjectKey() = default;
  ObjectKey(StoredObjectKey::Reader reader)
      : key { reader.getKey0(), reader.getKey1(), reader.getKey2(), reader.getKey3() } {}
  ~ObjectKey() {
    sodium_memzero(key, sizeof(key));
  }

  static ObjectKey generate();

  inline void copyTo(StoredObjectKey::Builder builder) const {
    builder.setKey0(key[0]);
    builder.setKey1(key[1]);
    builder.setKey2(key[2]);
    builder.setKey3(key[3]);
  }

  inline bool operator==(const ObjectKey& other) const {
    return sodium_memcmp(key, other.key, sizeof(key)) == 0;
  }
  inline bool operator!=(const ObjectKey& other) const {
    return !operator==(other);
  }
};

struct ObjectId {
  uint64_t id[2];
  // The object ID. Equals the 16-byte blake2b hash of the key.

  ObjectId() = default;
  ObjectId(decltype(nullptr)): id {0, 0} {}
  ObjectId(StoredObjectId::Reader reader)
      : id { reader.getId0(), reader.getId1() } {}
  ObjectId(const ObjectKey& key);

  inline bool operator==(const ObjectId& other) const {
    // Constant-time for less branching, but note that object IDs are not secrets so we don't have
    // to jump through hoops to prevent optimization.
    return ((id[0] ^ other.id[0]) | (id[1] ^ other.id[1])) == 0;
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

kj::String KJ_STRINGIFY(ObjectId id);

enum class ObjectType: uint8_t {
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

  ObjectType type :8;

  bool readOnly :1;
  // For volumes, prevents the volume from being modified. For Blobs, indicates that initialization
  // has completed with a `done()` call, indicating the entire stream was received (otherwise,
  // either the stream is still uploading, or it failed to fully upload). Once set this
  // can never be unset.

  bool dirty :1;
  // Indicates whether direct writes have occurred since the last verison bump.

  bool dirtyTransitiveSize :1;
  // Indicates whether `transitiveBlockCount` needs to be recalculated by summing all child
  // sizes.
  //
  // This is employed to allow transitive sizes to be updated without distributed transactions.
  // When a transaction is to be committed updating an object's transitive size, first the
  // `dirtyTransitiveSize` bit is set on the parent, then the child is updated, then the parent
  // is updated and `dirtyTransitiveSize` is cleared. If a failure occurs during the process,
  // the parent will need to recompute its transitive size later by examining all children.

  bool pendingRemoval :1;
  // If true, and `owner` is the ID of an object that doesn't exist (especially null), then this
  // object needs to be recursively deleted. The object cannot actually be deleted until
  // `pendingRemoval` has been set true on all children.
  //
  // If `owner` does exist, and is permanent, then `pendingRemoval` should be set false.
  //
  // If `owner` does exist, but is still temporary (not saved to disk), then `pendingRemoval`
  // should be left alone until the owner becomes permanent or is dropped.

  byte reserved :4;

  uint64_t version :48;
  // The object's version number. Used in replicated mode to determine which copy of the object
  // is the newest. Not visible to clients.

  uint64_t transitiveBlockCount;
  // The number of 4k blocks in this object and all child objects.
  //
  // Note that for Volumes, which are written non-transactionally, we don't update this on every
  // write, therefore the value may drift from the actual storage used. Periodically, we update
  // this value to match the real amount, and schedule to update parents at that time.

  ObjectId owner;
  // What object owns this one?
};

static_assert(sizeof(Xattr) == 32, "Xattr format changed");

enum class RecoveryType: uint8_t {
  STAGING,
  // Staging content is the subject of a queued or upcoming journal entry.

  JOURNAL,
  // The journal itself. There is only temporary of this type; it has number zero.

  OBJECT_STATE,
  // Extra transient state about an object, such as backburner tasks currently pending against
  // it.

  SIBLING_STATE,
  // State of this storage node.
};
constexpr uint RECOVERY_TYPE_COUNT = static_cast<uint>(RecoveryType::SIBLING_STATE) + 1;
constexpr auto ALL_RECOVERY_TYPES = transformCollection(
    kj::range<uint>(0, RECOVERY_TYPE_COUNT), StaticCastFunctor<uint, RecoveryType>());

kj::StringPtr KJ_STRINGIFY(RecoveryType type);

struct RecoveryId {
  RecoveryType type;
  uint8_t reserved[7];
  uint64_t id;

  RecoveryId() = default;
  inline RecoveryId(RecoveryType type, uint64_t id): type(type), reserved{0,0,0,0,0,0,0}, id(id) {}

  inline bool operator<(const RecoveryId& other) const {
    if (type < other.type) return true;
    if (type > other.type) return false;
    return id < other.id;
  }
};

static_assert(sizeof(RecoveryId) == 16, "RecoveryId format changed; consider effect on journal");

struct TemporaryXattr {
  // Much like Xattr, but assigned to recoverable temporary files.

  static constexpr const char* NAME = "user.sandstot";
  // Extended attribute name for RecoveryXattr.

  union {
    struct {
      byte reserved[16];

      ObjectId ojbectId;
      // Affected object ID.
    } objectState;

    struct {
      // Nothing currently here.
    } siblingState;
  };
};

static_assert(sizeof(TemporaryXattr) == 32, "TemporaryXattr format changed");

struct JournalEntry {
  // In order to implement atomic transactions, we organize disk changes into a stream of
  // idempotent modifications. Each change is appended to the journal before being actually
  // performed, so that on system failure we can replay the journal to get up-to-date.

  // Note that the logical "first" fields are `type` and `txSize`, but we put them at the end of
  // the Entry in order to better-detect when an entry is only partially-written. I'm actually
  // not sure that such a thing is possible, but being safe is free in this case.

  uint64_t stagingId;
  // Identifies a staging file which should be rename()ed to replace this object or
  // temporary. The xattrs should be written to the file just before it is renamed. If no such
  // staging file exists, this operation probably already occurred; ignore it.
  //
  // If there is no existing file matching the ID, we are probably replaying an operation that
  // was already completed, and some later operation probably deletes this object; ignore the
  // op.

  union {
    struct {
      // For entry types targetting objects.

      ObjectId id;
      // ID of the object to update.

      Xattr xattr;
      // Updated Xattr structure to write into the file. Applies only when creating/updating an
      // object.
    } object;

    struct {
      // For entry types targetting recoverable temporaries.

      RecoveryId id;
      // ID of recoverable temporary to address.

      TemporaryXattr xattr;
    } temporary;
  };

  enum class Type: uint8_t {
    CREATE_OBJECT,
    // Move the staging file identified by `stagingId` into main storage, first setting the xattrs
    // on the staging file, then rename()ing it into place, expecting that there is no existing
    // file there already. If no such staging file exists, this operation probably already
    // occurred; ignore it.

    UPDATE_OBJECT,
    // Replace the object with a staging file identified by `stagingId`, first setting the xattrs
    // on the staging file, then rename()ing it into place. If no such staging file exists, this
    // operation probably already occurred; ignore it.
    //
    // If there is not already a file in the target location, assume it was asynchronously
    // deleted and immediately delete this file as well.

    UPDATE_XATTR,
    // Update the xattrs on an existing file.

    DELETE_OBJECT,
    // Delete this object. Usually this comes bundled with entries adding backburner tasks to
    // delete children.

    CREATE_TEMPORARY,
    // Create a temporary object and assign it a recovery id `recoveryId`.

    UPDATE_TEMPORARY,
    // Overwrite the recoverable temporary identified by `recoveryId` with the content of the one
    // identified by `stagingId` (consuming the latter).

    UPDATE_TEMPORARY_XATTR,
    // Update just the temporary's xattrs.

    DELETE_TEMPORARY,
    // Delete the recoverable temporary identified by `recoveryId`.
  };

  Type type;
  // Transaction type.

  byte reserved[3];

  uint32_t txSize;
  // Number of entries remaining in this transaction, including this one. Do not start applying
  // operations unless the full transaction is available and all `txSize` values are correct.
  // If recovering from a failure, ignore an incomplete transaction at the end of the journal.
};

static_assert(sizeof(JournalEntry) == 64,
    "journal entry size changed; please keep power-of-two and consider migration issues");
// We want the entry size to be a power of two so that they are page-aligned.

}  // namespace storage
}  // namespace blackrock

#endif // BLACKROCK_STORAGE_BASICS_H_
