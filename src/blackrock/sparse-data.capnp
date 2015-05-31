# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xb4ec463ef590911d;

$import "/capnp/c++.capnp".namespace("blackrock");

struct SparseData {
  # Represents a chunk of "sparse" data, i.e. bytes with a lot of long runs of zeros. We only
  # include the non-zero bytes.
  #
  # This is used in particular to store a blank ext4 filesystem template directly into the
  # Blackrock binary so that we can quickly format new volumes.

  chunks @0 :List(Chunk);
  struct Chunk {
    offset @0 :UInt64;
    data @1 :Data;
  }
}
