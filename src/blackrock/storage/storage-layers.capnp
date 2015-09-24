# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xb68b9ef8966c31c9;

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

interface File {
  read @0 (offset :UInt64, size :UInt64) -> (data :Data);
  write @1 (offset :UInt64, data :Data);
  sync @2 ();
}

interface Filesystem {
  newFile @0 () -> (file :FileWithAttributes);
  openFile @1 (id :StoredObjectId) -> (file :FileWithAttributes);

  interface FileWithAttributes extends(File) {
    getAttributes @0 () -> (attributes :Attributes);
    setAttributes @1 (attributes :Attributes);

    setId @2 (id :StoredObjectId);
    # Called once after newFile() to promote the file from staging to main.

    getSize @3 () -> (length :UInt64, blockCount :UInt32);
  }

  struct Attributes {
    type @0 :UInt8;
    readOnly @1 :Bool;
    accountedBlockCount @2 :UInt32;
    transitiveBlockCount @3 :UInt64;
    ownerId0 @4 :UInt64;
    ownerId1 @5 :UInt64;
  }
}

interface Journal {
  newStaging @0 () -> (file :StagingFile);
  # Create a new staging file which can later be passed into createObject or replaceObjectData.

  openDirect @1 (id :StoredObjectId) -> (file :File);

  interface StagingFile extends(File) {}

  interface ObjectHandle {
    openDirect @0 () -> (file :File);
    # Get direct access to the underlying data. Reads and writes will not be transactional; it is
    # up to the client to use sync() and maintain their own journal. The Journal will automatically
    # perform transactions to update the file size when the file is dropped and periodically as
    # the file is modified.

    replaceContent @1 (txn :Transaction, staging :StagingFile);
    # Replaces the content of the file with a new staging file.

    freeze @2 (txn :Transaction);
    # Sets the file to read-only mode.

    addChild @3 (txn :Transaction, child :ObjectHandle);
    removeChild @4 (txn :Transaction, child :StoredObjectId);
  }

  interface Transaction {
    commit @0 ();
  }
}
