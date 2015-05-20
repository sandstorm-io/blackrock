# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xbdcb3e9621f08052;

$import "/capnp/c++.capnp".namespace("blackrock");
using persistent = import "/capnp/persistent.capnp".persistent;

using Util = import "/sandstorm/util.capnp";
using ByteStream = Util.ByteStream;
using ClusterRpc = import "cluster-rpc.capnp";
using StoredObjectId = ClusterRpc.StoredObjectId;

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

  getExclusive @5 () -> (exclusive :Volume);
  # Get a capability representing the same Volume except that it will be automatically disconnected
  # the next time getExclusive() is called. This is basically a form of optimistic concurrency: it
  # gives you exclusive access without taking a lock (as long as all other clients also use
  # `getExclusive()`).
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
  # A collection of values of type T.
  #
  # T must be an OwnedStorage type. Items in a collection have no notion of a "primary key" -- the
  # capability itself is the key.

  insert @0 (value :T);
  # Add an item to the collection.
  #
  # Keep in mind that because the item must be an OwnedStorage derivative, either it must be a
  # newly-created object or this `insert()` must be performed as part of a transaction.

  remove @1 (value :T);
  # Remove an item from the collection.

  getAll @2 () -> (cursor :Cursor);
  # Query all items in the set, returned in arbitrary order.

  makeIndex @3 [Key] (selector :Function(T, Key)) -> (index :OwnedIndex(Key));
  # Make an index allowing an item to be looked up by key. This index does not allow range queries;
  # only equality. The index is probably backed by a hashtable. The index is automatically updated
  # when items are inserted, removed, or modified in a way that changes the key.

  makeOrderedIndex @4 [Key] (selector :Function(T, Key)) -> (index :OwnedOrderedIndex(Key));
  # Make an index allowing ordered ranges of items to be queried. For example, you could index on
  # timestamp in order to get chronological lists of items, or you could order on titles to get
  # alphabetical lists. The index is probably backed by a b-tree. The index is automatically
  # updated when items are inserted, removed, or modified in a way that changes the key.

  interface Index(Key) {
    find @0 (key :Key) -> (value :T);
    # Look up the value matching `key`. `value` is null if there was no match.
  }
  interface OrderedIndex(Key) extends(Index(Key)) {
    findRange @0 (start :Key, end :Key) -> (cursor :Cursor);
    # Get a cursor over a range of values. The values will be sorted by key. To sort in descending
    # order, reverse `start` and `end`.
  }

  interface OwnedIndex(Key) extends (Index(Key), OwnedStorage(Index(Key))) {}
  interface OwnedOrderedIndex(Key) extends (OrderedIndex(Key), OwnedStorage(OrderedIndex(Key))) {}

  interface Cursor {
    # Represents a stream of results of a query.

    getNext @0 (count :UInt32) -> (elements :List(Box(T)));
    # Get the next `count` items, or fewer if the end of the stream is reached.
    #
    # Hint: Set `count` to a very large number to read the whole collection at once, but keep in
    #   mind that RPC messages have a maximum size so this won't work on large data sets.

    skip @1 (count :UInt64);
    # Skip `count` items in the stream.

    count @2 () -> (count :UInt64);
    # Count the number of items remaining in the stream. This also advances the cursor to the end
    # of the stream, so you'll need to start a new query if you wanted to read any of the items.

    observeChanges @3 (observer :Observer) -> (handle :Util.Handle);
    # Begin observing insertions and removals that match the query which created this cursor,
    # starting from the set of items that have already been read, skipped, or counted through this
    # cursor. That is, if `observeChanges()` is the first call you make on a cursor, then the
    # observer will immediately receive a series of `inserted()` calls for all existing items
    # matching the query. On the other hand, if you first traverse over all the items using other
    # methods, then call `observeChanges()` last, you'll only receive notifications relative to
    # what you already saw.
    #
    # Either way, after `observeChanges()` is called, other methods will behave as if the cursor
    # has already seeked to the end of the stream.
    #
    # If `observer` is persistent, `handle` is persistent, meaning you can observe changes to this
    # collection over the long haul.
  }

  interface Observer {
    inserted @0 (values :List(Box(T)));
    # One or more items have been inserted into the collection.

    removed @1 (values :List(Box(T)));
    # One or more items have been removed from the collection.
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

  getSize @1 () -> (totalBytes :UInt64);
  # Get the total storage space consumed by this object, including owned sub-objects.

  # TODO(someday): Observe the total size of this object (including children). Use cases:
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
# TODO(soon): This inheritance heirarchy turns out to be unruly. It would be better to change
#   OwnedStorage.getPersistentWeakRef() to just get() and require callers to call that in order
#   to use the object.

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

  newAssignableCollection @5 [T] () -> (collection :OwnedCollection(OwnedAssignable(T)));
  newImmutableCollection @8 [T] () -> (collection :OwnedCollection(OwnedImmutable(T)));
  # Create a new collection of assignable or immutable values. The performance characteristics of
  # these two may differ; assignable collections are optimized to allow item sizes to change,
  # whereas immutable collections are not. This affects things like allocation strategies and
  # indexing complexity.

  newOpaque @6 [T] (object :OwnedStorage(T)) -> (opaque :OwnedStorage(Opaque));
  # Create an opaque object wrapping some other object.

  newTransaction @7 () -> (transaction :Transaction);
  # Start a transaction.
}

interface StorageRootSet {
  # Manages the set of "root" objects in storage. These are objects which are logically "owned" by
  # entities outside of the storage system (typically, the frontend's MongoDB).
  #
  # Root objects have names which are valid filenames. By convention, for objects corresponding
  # to MongoDB objects, there should be "<collection-name>-<row-id>".
  #
  # This is meant as somewhat of a temporary solution. Long-term, storage should have a single
  # root object which contains everything else in collections.

  set @0 [T] (name :Text, object :OwnedStorage(T));
  # Turn `object` into a root object with the given name. Overwrites any existing root with the
  # same name (NOT ATOMIC).

  get @1 [T] (name :Text) -> (object :OwnedStorage(T));
  # Get the named root object.

  getOrCreateAssignable @4 [T] (name :Text, defaultValue :T) -> (object :OwnedAssignable(T));
  # Get the named root object, creating it if it doesn't already exist.

  remove @2 (name :Text);
  # Recursively delete the named root object.

  getFactory @3 () -> (factory :StorageFactory);
  # Convenience.
}

interface Transaction {
  # Allows several operations within the same storage system to be performed atomically.

  commit @0 ();
  # Commit the transaction atomically. Throws a DISCONNECTED exception if the transaction has been
  # invalidated by concurrent writes.

  getTransactional @1 [T] (object :T) -> (transactionalObject :T);
  # Given any storage object capability `object`, get an alternate version of the object where any
  # methods called will be added to the transaction rather than executed directly. Note that any
  # methods which cannot be executed transactionally will throw UNIMPLEMENTED exceptions.
  #
  # The following operations can be performed transactionally:
  #   Assignable.get()
  #   Assignable.Setter.set()
  #   Collection.insert()
  #   Collection.remove()
  #   Collection.getAll()
  #   Collection.Index.find()
  #   Collection.OrderedIndex.findRange()
  #   OwnedStorage.disconnectAllClients() (returned capability is a promise not resolved until
  #                                        transaction is committed)
  #
  # The following explicitly do not support transactions:
  #   Volumes (you must implement your own journal on top of Volume.sync(); or, more likely, use
  #            an existing journaling filesystem)
  #   Blob and Immutable (because there's no point)
  #   Opaque (no methods)
  #
  # All methods return immediately despite not having acctually occurred yet. If any read methods
  # (get, find) are included in the transaction, the transaction fails if the values change before
  # the transaction is committed. The transaction may start throwing DISCONNECTED ecxeptions before
  # `commit()` if it has already become apparent that the transaction will fail.
}
