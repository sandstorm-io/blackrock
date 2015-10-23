# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xfa4891583fc22051;

$import "/capnp/c++.capnp".namespace("blackrock::storage");

using FsStorage = import "../fs-storage.capnp";
using StoredObjectId = FsStorage.StoredObjectId;
using StoredObjectKey = FsStorage.StoredObjectKey;

interface TestJournal {
  interface Object {
    read @0 () -> (xattr :Data, content :Data);
  }

  interface Transaction {
    createObject @0 (id :StoredObjectId, xattr :Data, content :Data) -> (object :Object);
    createTemp @1 (xattr :Data, content :Data) -> (object :Object);
    update @2 (object :Object, xattr :Data, content :Data);  # both inputs optional
    remove @3 (object :Object);

    commit @4 (tempToConsume :Object);
  }

  openObject @0 (id :StoredObjectId) -> (object :Object);
  newTransaction @1 () -> (transaction :Transaction);

  interface Recovery {
    # The bootstrap interfacce exported by the test storage process.

    struct Temp {
      object @0 :Object;
      xattr @1 :Data;
    }

    recover @0 () -> (recoveredTemps :List(Temp), journal :TestJournal);
  }
}
