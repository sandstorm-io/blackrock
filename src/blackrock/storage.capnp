# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xbdcb3e9621f08052;

$import "/capnp/c++.capnp".namespace("blackrock");
using Persistent = import "/capnp/persistent.capnp".Persistent;

using Util = import "/sandstorm/util.capnp";
using ByteStream = Util.ByteStream;
using StoredObjectId = import "cluster-rpc.capnp".StoredObjectId;

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface StorageZone(T) extends(Persistent) {
  # A grouping of storage objects with a quota.
  #
  # Newly-created objects (other than the root) are in a detached state, similar to a temporary file
  # that is open but has been unlinked from the file system. Once all live references to said
  # object go away, the object will only persist if a capability to it has been stored into some
  # other object *in the same zone*. In other words, cross-zone links or links from external
  # sources are "weak" -- they do not force the capability to persist. This ensures that the user's
  # quota can't unknowingly be consumed by an object to which they no longer have any access because
  # the only link to it is external.

  getRoot @0 () -> (assignable :Assignable(T));

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
  newBlob @0 (content :Data) -> (blob :Blob);
  # Create a new blob from some bytes.

  uploadBlob @1 () -> (blob :Blob, sink :ByteStream);
  # Begin uploading a large blob. The content should be written to `sink`. The blob is returned
  # immediately, but any attempt to read from it will block waiting for bytes to be uploaded.
  # If an error later occurs during upload, the blob will be left broken, and attempts to read it
  # may throw exceptions.

  newMutableBlob @2 () -> (blob :MutableBlob);
  # Create a new MutableBlob.

  newImmutable @3 [T] (value :T) -> (immutable :Immutable(T));
  # Store the given value immutably, returning a persistable capability that can be used to read
  # the value back later. Note that `value` can itself contain other capabilities, which will
  # themselves be persisted by the storage server. If any of these capabilities are not persistable,
  # they will be replaced with capabilities that always throw an exception.

  newAssignable @4 [T] (initialValue :T) -> (assignable :Assignable(T));
  # Create a new assignable slot, the value of which can be changed over time.

  newCollection @5 [T] () -> (collection :Collection(T));
  # Create a new collection.

  newVolume @7 () -> (volume :Volume);
  # Create a new block-device-like volume.

  newSubZone @6 [T] (initialRootValue :T) -> (zone :StorageZone(T));
  # Create a sub-zone of this zone with its own root. The zones will share quota, but all objects
  # in the sub-zone must be reachable from its own root in order to persist. The sub-zone object
  # itself must be reachable from some object in the parent zone or the sub-zone and everything
  # inside it will be destroyed.
}

interface Blob extends(Persistent) {
  # Represents a large byte blob living in long-term storage.

  getSize @0 () -> (size :UInt64);
  # Get the total size of the blob. May block if the blob is still being uploaded and the size is
  # not yet known.

  writeTo @1 (sink :ByteStream, startAtOffset :UInt64 = 0);
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

  snapshot @3 () -> (blob :Blob);
  # Make a copy of this blob in its current state which is guaranteed never to change. This may
  # simply return the same object if it is already immutable.

  clone @4 () -> (blob :MutableBlob);
  # Make a new MutableBlob which is initialized to be a copy of this blob.
}

interface MutableBlob extends(Blob) {
  # Like `Blob` but the content can be modified.

  setSize @0 (size :UInt64);
  # Changes the size of the blob. If the new size is smaller than the old, the trailing data is
  # truncated. If it is larger, then the new space is filled with zeros.

  openStream @1 (startAtOffset :UInt64 = 0) -> (sink :ByteStream);
  # Returns a ByteStream that, when written to, writes the data to the file starting at the given
  # offset.

  setSlice @2 (offset :UInt64, data :Data);
  # Overwrite a slice of the blob starting at the given offset with the given data.

  zeroSlice @3 (offset :UInt64, size :UInt64);
  # Like setSlice() but sets all the bytes to zero. Large contiguous blocks of zeros created this
  # way will not count against the storage quota.
}

interface Immutable(T) extends(Persistent) {
  get @0 () -> (value :T);
}

interface Assignable(T) extends(Util.Assignable(T), Persistent) {}

struct Function(Input, Output) {
  # TODO(soon): Pointfree function that takes an input of type Input and produces a value of type
  #   Output. Usually used to select a field of a struct to use as a key. Kind of like RPC pipeline
  #   ops.
}

struct Box(T) {
  # TODO(someday): Make Box(T) a Cap'n Proto built-in type, so that T can be a primitive, and so
  #   that List(Box(T)) can collapse to List(T)?

  value @0 :T;
}

interface Collection(T) extends(Persistent) {
  # Use cases:
  # - Read all.
  # - Insert.
  # - Get slot as Assignable.
  # - Observe changes.
  # - Set up index.

  insert @0 (value :T) -> (slot :Assignable(T));

  getAll @1 () -> (cursor :Cursor);

  makeIndex @2 [Key] (selector :Function(T, Key)) -> (index :Index(Key));

  interface Index(Key) extends(Persistent) {
    getMatching @0 (key :Key) -> (cursor :Cursor);
    getRange @1 (start :Key, end :Key) -> (cursor :Cursor);
  }

  interface Cursor {
    getNext @0 (count :UInt32) -> (elements :List(Box(T)));
    getAll @1 () -> (elements :List(Box(T)));
    getOne @2 () -> (value :T);
    skip @3 (count :UInt64);
    count @4 () -> (count :UInt64);
    observeChanges @5 () -> (observer :Observer);
  }

  interface Observer {
    inserted @0 (value :T);
    removed @1 (value :T);
  }
}

interface Volume {
  # Block storage supporting up to 2^32 blocks, typically of 4k each.
  #
  # Initially, all of the bytes are zero. The user is not charged quota for all-zero blocks.

  read @0 (blockNum :UInt32, count :UInt32 = 1) -> (data :Data);
  # Reads a block, or multiple sequential blocks. Returned data is always count * block size bytes.

  write @1 (blockNum :UInt32, data :Data);
  # Writes a block, or multiple sequential blocks. `data` must be a multiple of the block size.
  #
  # This method returns before the write actually reaches disk. Use sync() to wait for previous
  # writes to fully complete.

  zero @2 (blockNum :UInt32, count :UInt32 = 1);
  # Overwrites one or more blocks with zeros.
  #
  # This method returns before the write actually reaches disk. Use sync() to wait for previous
  # writes to fully complete.

  sync @3 ();
  # Does not return until all previous write()s and zero()s are permanently stored.

  getBlockCount @4 () -> (count :UInt32);
  # Get the number of non-zero blocks present in the volume.

  watchBlockCount @5 (watcher :Watcher) -> (handle :Util.Handle);
  interface Watcher {
    blockCountChanged @0 (newCount :UInt32);
  }
}

interface Quota {
  sprout @0 () -> (quota :Quota);
  deposit @1 (from :Quota, byteCount :UInt64, begin :Timepoint, end :Timepoint);
}
