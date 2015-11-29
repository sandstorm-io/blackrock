# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xa045562b2c14fc88;

$import "/capnp/c++.capnp".namespace("blackrock::storage");
using ObjectId = import "/blackrock/fs-storage.capnp".StoredObjectId;

# TODO(cleanup): Move ObjectId and friends into this file from fs-storage.capnp.

struct ChangeSet {
  create @0 :UInt8 = 0;
  # Type is ObjectType enum from basics.h. 0 means to open, not create.

  setContent @1 :Data;
  # Overwrite content. null = don't overwrite.

  shouldBecomeReadOnly @2 :Bool = false;

  shouldDelete @3 :Bool = false;
  shouldMarkForDeletion @4 :Bool = false;

  shouldMarkTransitiveBlockCountDirty @5 :Bool = false;
  shouldSetTransitiveBlockCount @6 :Bool = false;
  setTransitiveBlockCount @7 :UInt64;
  # Setting transitive block count implies making it non-dirty, unless
  # `shouldMarkTransitiveBlockCountDirty` is true at the same time.

  setOwner @8 :ObjectId;
  # null = don't change
}
