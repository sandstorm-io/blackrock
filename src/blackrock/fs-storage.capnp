# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xfc40bcbedafbe11c;
# An implementation of the Storage interfaces based on a standard filesystem.
#
# All objects are stored in a massive directory with filenames like:
#     o<objectId>: object content
#     c<objectId>: list of object IDs that should be deleted if this object is deleted. This list
#                  is generally append-only and so can contain IDs that no longer exist; those
#                  should be ignored when deleting.
#
# A second directory, called "staging", stores files whose names consist of exactly 7 hex digits,
# and which are intended to be rename()ed into place later on.
#
# In all cases above, an <id> is a 16-byte value base64-encoded to 22 bytes (with '-' and '_' as
# digits 62 and 63). Note that ext4 directory entries are 8 + name_len bytes, rounded up to a
# multiple of 4, with no NUL terminator stored. Since our filenames are 23 bytes (including prefix
# character), each directory entry comes out to 32 bytes (31 rounded up). That seems sort of nice?

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
      external @0 :SturdyRef;
      # A remote capability. Does not point back into storage.

      sibling @1 :StoredObjectKey;
      # This points to another object in the same zone, and that sibling's refcount currently counts
      # this object.

      outOfZone @2 :StoredObjectKey;
      # Reference to another object in storage.
    }
  }
}

struct StoredZone {
  root @0 :StoredObjectKey;
  # The zone's root Assignable.

  # TODO(someday): Some stuff about quotas?
}
