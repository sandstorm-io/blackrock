# Sandstorm Enterprise Tools
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xbdcb3e9621f08052;

$import "/capnp/c++.capnp".namespace("sandstorm::et");

interface Datastore {
  createBlob @0 (content :Data) -> (blob :Blob);
  # Create a new blob from some bytes.

  uploadBlob @1 () -> (blob :Blob, sink :ByteSink);
  # Begin uploading a large blob. The content should be written to `sink`. The blob is returned
  # immediately, but any attempt to read from it will block waiting for bytes to be uploaded.
  # If an error later occurs during upload, the blob will be left broken, and attempts to read it
  # may throw exceptions.

  # TODO(someday): Storage of AnyPointer, possibly containing capabilities.
}

struct StoredObjectId {
  # SturdyRefObjectId for persisted objects.

  union {
    blob @0 :Void;
    immutable @1 :Void;
    assignable @2 :Void;
  }

  key0 @3 :UInt64;
  key1 @4 :UInt64;
  key2 @5 :UInt64;
  key3 @6 :UInt64;
  # 256-bit object key.
  #
  # Even if you have access to the raw storage, it is not possible to extract this key. Objects
  # in actual storage are identified by some hash of this key, and are encrypted by a symmetric
  # key based on some other hash.
}

interface ByteSink {
  write @0 (data :Data);
  # Add bytes.
  #
  # It's safe to make overlapping calls to `write()`, since Cap'n Proto enforces E-Order and so
  # the calls will be delivered in order. However, it is a good idea to limit how much data is
  # in-flight at a time, so that it doesn't fill up buffers and block other traffic. On the other
  # hand, having only one `write()` in flight at a time will not fully utilize the available
  # bandwidth if the connection has any significant latency, so parallelizing a few `write()`s is
  # a good idea.
  #
  # Similarly, the implementation of `ByteSink` can delay returning from `write()` as a way to
  # control flow.

  done @1 ();
  # Call after the last write to indicate that there is no more data. If the `ByteSink` is
  # discarded without a call to `done()`, the callee must assume that an error occurred and that
  # the data is incomplete.
  #
  # This will not return until all bytes are successfully written to their final destination.
  # It will throw an exception if any error occurs, including if the total number of bytes written
  # did not match `expectSize()`.

  expectSize @2 (size :UInt64);
  # Optionally called to let the receiver know exactly how much data will be written. This should
  # normally be called before the first write(), but if called later, `size` indicates how many
  # more bytes to expect _after_ the call. It is an error by the caller if more or fewer bytes are
  # actually written before `done()`; this also implies that all calls to `expectSize()` must be
  # consistent. The caller will ignore any exceptions thrown from this method, therefore it
  # is not necessary for the callee to actually implement it.
}

interface Blob {
  # Represents a large byte blob living in long-term storage.

  getSize @0 () -> (size :UInt64);
  # Get the total size of the blob. May block if the blob is still being uploaded and the size is
  # not yet known.

  writeTo @1 (sink :ByteSink, startAtOffset :UInt64 = 0) -> ();
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
  get @0 () -> (value :AnyPointer);
}
