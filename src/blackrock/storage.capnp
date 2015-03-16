# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xbdcb3e9621f08052;

$import "/capnp/c++.capnp".namespace("blackrock");
using persistent = import "/capnp/persistent.capnp".persistent;

using Util = import "/sandstorm/util.capnp";
using ByteStream = Util.ByteStream;
using StoredObjectId = import "cluster-rpc.capnp".StoredObjectId;

using Timepoint = UInt64;
# Nanoseconds since epoch.

interface StorageSibling {
  # Interface which Storage nodes use to talk to each other.

  # TODO
}

# ========================================================================================

interface Blob {
  # Represents a large byte blob living in long-term storage.

  getSize @0 () -> (size :UInt64);
  # Get the total size of the blob. May block if the blob is still being uploaded and the size is
  # not yet known.

  writeTo @1 (sink :ByteStream, startAtOffset :UInt64 = 0) -> (handle :Util.Handle);
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

interface Volume {
  # Block storage supporting up to 2^32 blocks, typically of 4k each.
  #
  # Initially, all of the bytes are zero. The user is not charged quota for all-zero blocks.
  #
  # Only blocks that you write are actually stored. Hence, there's no need for an explicit
  # capacity (other than the 2^32-block limit imposed by the choice of integer width). The client
  # is welcome to pretend the capacity is any number. It may even make sense to allocate a small
  # file system early on and only grow it as needed.

  const blockSize :UInt32 = 4096;
  # All volumes use a block size of 4096.

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

  asBlob @4 () -> (blob :Blob);
  # Get a Blob that reflects the content of this volume.
}

interface Immutable(T) {
  get @0 () -> (value :T);
}

interface Assignable(T) extends(Util.Assignable(T)) {}

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

interface Collection(T) {
  # Use cases:
  # - Read all.
  # - Insert.
  # - Get slot as Assignable.
  # - Observe changes.
  # - Set up index.

  insert @0 (value :T) -> (slot :Assignable(T));

  getAll @1 () -> (cursor :Cursor);

  makeIndex @2 [Key] (selector :Function(T, Key)) -> (index :Index(Key));

  interface Index(Key) $persistent {
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

interface Opaque {
  # A storage object whose contents are opaque to the holder of this capability. Usually used as
  # `OwnedOpaque` to represent an object whose storage you own but whose contents may be encrypted
  # with a key you don't have. You can see how big the object is, assign quota to it, delete the
  # object, or even transfer ownership, but you cannot see inside the object.
  #
  # So, how does anyone actually access the content of the object? Simple: they receive a
  # persistent -- but *weak* -- reference to some object inside. The persistent reference includes
  # a key which can be used to obtain access, but does not prevent the object from being deleted
  # by its owner.
  #
  # For example, data belonging to a user using encrypted login is encrypted at rest, and we do
  # not have the key.
}

# ========================================================================================

interface OwnedStorage(T) {
  # Represents an object in storage which is "owned" by some other object. For example, imagine an
  # `Assignable(GrainState)` containing the current state of a grain. The struct type `GrainState`
  # may have a field of type OwnedStorage(Volume) which is the capability to its storage volume.
  # The grain owns this storage, and its size is counted as part of the grain size, which in turn
  # is charged against the owning user's quota.
  #
  # An `OwnedStorage` is initially created in an orphaned state using `StorageFactory`. At this
  # point, it is much like a temporary file that has been opened but unlinked from the filesystem --
  # if the live reference is dropped, the object will be deleted. But, if the capability is stored
  # in some other storage object, then the the latter object becomes the owner of this one. After
  # that, this object is only destroyed if the owner is updated to no longer contain a capability
  # to the object, or if the owner itself is deleted.
  #
  # An `OwnedStorage` can never have more than one owner. Once it has an owner, trying to store
  # the capability into a second owner will fail, unless done as part of a Transaction that
  # simultaneously removes it from the original owner.
  #
  # Note that OwnedStorage itself does *not* implement the `Persistent` interface. This is because
  # the object cannot be saved to a SturdyRef from a system outsife of the storage system. The ref
  # can only be stored inside other objects in the same storage system.

  getPersistentWeakRef @0 () -> (ref :T);
  # Gets a capability pointing to the same object but which implements `Persistent`, meaning it
  # can be saved as a `SturdyRef` by anybody (not just within the same storage). However, this
  # reference is weak: if the owner of this object drops it, then the object is deleted and all
  # weak references will no longer function.
  #
  # TODO(someday): Let caller store some petname on this?

  getStats @1 () -> (totalBytes :UInt64, usedBytes :UInt64);
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

  # TODO(someday): Get / observe the total size of this object (including children). Use cases:
  # - Track size of grain to display to user. This only needs to run on-demand.
  # - Track size of user's storage to enforce quotas. This may need to take a persistent callback
  #   capability to invoke when some watermark is reached?
}

interface OwnedBlob extends(Blob, OwnedStorage(Blob)) {}
interface OwnedVolume extends(Volume, OwnedStorage(Volume)) {}
interface OwnedImmutable(T) extends(Immutable(T), OwnedStorage(Immutable(T))) {}
interface OwnedAssignable(T) extends(Assignable(T), OwnedStorage(Assignable(T))) {}
interface OwnedCollection(T) extends(Collection(T), OwnedStorage(Collection(T))) {}
interface OwnedOpaque extends(Opaque, OwnedStorage(Opaque)) {}

# ========================================================================================

interface StorageFactory {
  # Capability to create new objects in storage. All objecst a

  newBlob @0 (content :Data) -> (blob :OwnedBlob);
  # Create a new blob from some bytes.

  uploadBlob @1 () -> (blob :OwnedBlob, sink :ByteStream);
  # Begin uploading a large blob. The content should be written to `sink`. The blob is returned
  # immediately, but any attempt to read from it will block waiting for bytes to be uploaded.
  # If an error later occurs during upload, the blob will be left broken, and attempts to read it
  # may throw exceptions.

  newVolume @2 () -> (volume :OwnedVolume);
  # Create a new block-device-like volume.

  newImmutable @3 [T] (value :T) -> (immutable :OwnedImmutable(T));
  # Store the given value immutably, returning a persistable capability that can be used to read
  # the value back later. Note that `value` can itself contain other capabilities, which will
  # themselves be persisted by the storage server. If any of these capabilities are not persistable,
  # they will be replaced with capabilities that always throw an exception.

  newAssignable @4 [T] (initialValue :T) -> (assignable :OwnedAssignable(T));
  # Create a new assignable slot, the value of which can be changed over time.

  newCollection @5 [T] () -> (collection :OwnedCollection(T));
  # Create a new collection.

  newOpaque @6 [T] (object :OwnedStorage(T)) -> (opaque :OwnedStorage(Opaque));
  # Create an opaque object wrapping some other object.

  newTransaction @7 () -> (transaction :Transaction);
  # Start a transaction.
}

interface Transaction {
  # Allows several operations within the same storage system to be performed atomically.

  commit @0 ();
  # Commit the transaction atomically. Throws a DISCONNECTED exception if the transaction has been
  # invalidated by concurrent writes.

  set @1 [T] (assignable :Util.Assignable(T).Setter, value :T);
  # Set the given assignable as part of the transaction. If the setter is implementing optimistic
  # concurrency, the entire transaction will fail if the assignable has been modified concurrently.

  # TODO(someday): Collection modifications.
}

# ========================================================================================
# Main storage schemas for Blackrock.
#
# TODO(cleanup): Maybe this belongs in a different file? It's more "private" than most of the
#   stuff here.

struct StorageRoot {
  # The root of the storage system is an Assignable(StorageRoot).

  # TODO(someday):
  # - Collection of users.
  # - Collection of apps.
  # - Gateway storage.
  # - Others?
}

struct AccountStorage {
  # TODO(someday):
  # - Basic metadata.
  # - Quota etc.
  # - Opaque collection of owned grains.
  # - Opaque collection of received capabilities.
}

struct GatewayStorage {
  # TODO(someday):
  # - Incoming and outgoing SturdyRefs.
}
