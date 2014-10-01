# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xbdcb3e9621f08052;

$import "/capnp/c++.capnp".namespace("sandstorm::blackrock");

using Util = import "/sandstorm/util.capnp";
using Assignable = Util.Assignable;
using ByteStream = Util.ByteStream;

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface StorageZone {
  # A grouping of storage objects with a quota.
  #
  # Newly-created objects (other than the root) are in a detached state, similar to a temporary file
  # that is open but has been unlinked from the file system. Once all live references to said
  # object go away, the object will only persist if a capability to it has been stored into some
  # other object *in the same zone*. In other words, cross-zone links or links from external
  # sources are "weak" -- they do not force the capability to persist. This ensures that the user's
  # quota can't unknowingly be consumed by an object to which they no longer have any access because
  # the only link to it is external.
  #
  # TODO(someday): Parameterize type for root type.

  getRoot @0 () -> (assignable :Assignable);

  getFactory @1 () -> (factory :StorageFactory);

  addQuota @2 (quota :Quota, startTime :Timepoint);
  # Add quota to this zone, starting at the given time, or immediately if `startTime` is in the
  # past.

  getStats @3 () -> (totalBytes :UInt64, usedBytes :UInt64);
  # Get total available quota and space currently used. Note that the latter can actually be greater
  # than the former in two cases:
  # - Some "courtesy" space is provided beyond the specified quota so that users don't lose data
  #   if they go a little over. However, the user will be receiving dire warnings at this point and
  #   eventually writes will start failing.
  # - If quota is reduced to be less than the space already used, the storage system will NOT
  #   delete anything. The user simply won't be able to write anything new until they reduce their
  #   usage or get their quota back. This is important to make sure a mistake (e.g. a missed
  #   payment while they're in the hospital) doesn't cause the user to immediately lose all their
  #   data.
}

interface StorageSibling {
  # Interface which Storage nodes use to talk to each other.

  # TODO
}

interface StorageFactory {
  createBlob @0 (content :Data) -> (blob :Blob);
  # Create a new blob from some bytes.

  uploadBlob @1 () -> (blob :Blob, sink :ByteStream);
  # Begin uploading a large blob. The content should be written to `sink`. The blob is returned
  # immediately, but any attempt to read from it will block waiting for bytes to be uploaded.
  # If an error later occurs during upload, the blob will be left broken, and attempts to read it
  # may throw exceptions.

  createImmutable @2 (value :AnyPointer) -> (immutable :Immutable);
  # Store the given value immutably, returning a persistable capability that can be used to read
  # the value back later. Note that `value` can itself contain other capabilities, which will
  # themselves be persisted by the storage server. If any of these capabilities are not persistable,
  # they will be replaced with capabilities that always throw an exception.

  createAssignable @3 (initialValue :AnyPointer) -> (assignable :Assignable);
  # Create a new assignable slot, the value of which can be changed over time.

  createSubZone @4 (initialRootValue :AnyPointer) -> (zone :StorageZone);
  # Create a sub-zone of this zone with its own root. The zones will share quota, but all objects
  # in the sub-zone must be reachable from its own root in order to persist. The sub-zone object
  # itself must be reachable from some object in the parent zone or the sub-zone and everything
  # inside it will be destroyed.
}

struct StoredObjectId {
  # SturdyRefObjectId for persisted objects.

  key0 @0 :UInt64;
  key1 @1 :UInt64;
  key2 @2 :UInt64;
  key3 @3 :UInt64;
  # 256-bit object key. This both identifies the object and may serve as a symmetric key for
  # decrypting the object.
}

interface Blob {
  # Represents a large byte blob living in long-term storage.

  getSize @0 () -> (size :UInt64);
  # Get the total size of the blob. May block if the blob is still being uploaded and the size is
  # not yet known.

  writeTo @1 (sink :ByteStream, startAtOffset :UInt64 = 0) -> ();
  # Write the contents of the blob to `sink`.

  getSlice @2 (offset :UInt64, size :UInt32) -> (data :Data);
  # Read a slice of the blob starting at the given offset. `size` cannot be greater than Cap'n
  # Proto's limit of 2^29-1, and reasonable servers will likely impose far lower limits. If the
  # slice would cross past the end of the blob, it is truncated. Otherwise, `data` is always
  # exactly `size` bytes (though the caller should check for security purposes).
  #
  # One technique that makes a lot of sense is to start off by calling e.g. `getSlice(0, 65536)`.
  # If the returned data is less than 65536 bytes then you know you got the whole blob, otherwise
  # you may want to switch to `writeTo`.
}

interface Immutable {
  # TODO(someday): Use generics when available.

  get @0 () -> (value :AnyPointer);
}

interface Quota {
  # TODO(soon): Something like the purse protocol (classic capability protocol for payments). Note
  #   that there are two parameters: space and time.
}
