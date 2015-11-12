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

  shouldDelete @2 :Bool = false;

  shouldBecomeReadOnly @3 :Bool = false;

  adjustTransitiveBlockCount @4 :Int64 = 0;
  # Delta vs. current value.

  setOwner @5 :ObjectId;
  # null = don't change

  backburnerModifyTransitiveBlockCount @6 :Int64 = 0;
  # Schedule backburner task to modify this object's transitive block count and recurse to its
  # parent.

  backburnerRecursivelyDelete @7 :Bool = false;
  # Schedule backburner task to eventually delete this object and recurse to its children.

  shouldClearBackburner @8 :Bool = false;
  # Remove all backburner tasks attached to this object.
}
