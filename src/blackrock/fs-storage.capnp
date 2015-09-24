# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xfc40bcbedafbe11c;
# An implementation of the Storage interfaces based on a standard filesystem.
#
# TODO(doc): The following is outdated!
#
# All objects are stored in a massive directory with filenames like:
#     o<objectId>: object content
#
# In all cases above, an <id> is a 16-byte value base64-encoded to 22 bytes (with '-' and '_' as
# digits 62 and 63). Note that ext4 directory entries are 8 + name_len bytes, rounded up to a
# multiple of 4, with no NUL terminator stored. Since our filenames are 23 bytes (including prefix
# character), each directory entry comes out to 32 bytes (31 rounded up). That seems sort of nice?
#
# A second directory, called "staging", stores files whose names consist of exactly 16 hex digits,
# and which are intended to be rename()ed into place later on. Files in staging exist only for
# their content. If that content includes outgoing owned references, the target objects are either
# in staging themselves or are owned by some non-staging objects and are scheduled to have owneship
# transferred in an upcoming transaction. In other words, when deleting an object out of staging,
# it does NOT make sense to recursively delete its children.
#
# A third directory, called "deathrow", contains objects scheduled for recursive deletion. Objects
# here used to be under the main directory, but have been deleted. Before actually deleting the
# file, it is necessary to move all of its children into "deathrow". This process of recursive
# deletion can occur in a separate thread (or process!) so that deep deletions do not block other
# tasks.

$import "/capnp/c++.capnp".namespace("blackrock");
using Storage = import "storage.capnp";
using SturdyRef = import "cluster-rpc.capnp".SturdyRef;

struct StoredObjectId {
  # 16-byte ID of the object. This is calculated as the 16-byte blake2b hash of the object key.

  id0 @0 :UInt64;
  id1 @1 :UInt64;
}

struct StoredObjectKey {
  # Key to decrypt an object.

  key0 @0 :UInt64;
  key1 @1 :UInt64;
  key2 @2 :UInt64;
  key3 @3 :UInt64;
}

struct StoredIncomingRef {
  # Stored in `ref/<id>`, where <id> is the base64('+','_') of the 16-byte blake2b hash of
  # the ref key (the 32-byte key stored in the SturdyRef). Encrypted by the ref key.

  owner @0 :SturdyRef.Owner;
  # Who is allowed to restore this ref?

  key @1 :StoredObjectKey;
  # Key to the object.
}

struct StoredChildIds {
  # A stored `Assignable` or `Immutable` object file contains two Cap'n Proto messages:
  # StoredChildIds followed by StoredObject. The latter could be encrypted.

  children @0 :List(StoredObjectId);
  # List of owned children of this object. If this object is deleted, all children should be
  # deleted as well.
}

struct StoredObject {
  # A stored `Assignable` or `Immutable` object file contains two Cap'n Proto messages:
  # StoredChildIds followed by StoredObject. The latter could be encrypted.

  capTable @0 :List(CapDescriptor);
  payload @1 :AnyPointer;

  struct CapDescriptor {
    union {
      none @0 :Void;
      # Null. (But `null` is not a good variable name due to macro conflicts.)

      child @1 :StoredObjectKey;
      # This points to an owned child object.

      external @2 :SturdyRef;
      # A remote capability. (Could point back to storage, but the object isn't owned by us.)
    }
  }
}

struct StoredRoot {
  # A root object.

  key @0 :StoredObjectKey;
}
