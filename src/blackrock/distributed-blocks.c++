// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// ************************************
// **** INCOMPLETE **** INCOMPLETE ****
// ************************************
//
// This file contains some initial ideas for data structures relating to a distributed block
// storage system. At present we are not implementing this, but we might in the future.

#include "common.h"
#include <inttypes.h>

namespace blackrock {
namespace {

struct UInt128 {
  uint64_t value[2];
};

struct UInt256 {
  uint64_t value[4];
};

struct Superblock {
  // First block of a physical disk which is part of the distributed block storage system.

  static constexpr UInt128 MAGIC = { { 0xcdf365304999bb98ull, 0xfd641ba781b04071ull } };
  static constexpr uint16_t VERSION = 0;

  UInt128 magic;
  // Magic number indicating a Blackrock disk. Always set to MAGIC.

  UInt128 clusterId;
  // Number identifying the Blackrock cluster of which this disk is a part.

  uint16_t version;
  // Storage format version. Set to VERSION.

  uint8_t replicaId;
  // Each disk is part of one replica of the cluster's storage. If the underlying disk is already
  // considered robust, then there may be only one replica (replica zero). Otherwise, 2-3 replicas
  // are typical. All shards on one disk -- and preferrably all disks in one machine -- are
  // required to be in the same replica because otherwise it would defeat the purpose of replicas.

  uint8_t lgBucketCount;
  // Log base 2 of the number of hash table buckets in each local shard.

  uint8_t lgJournalSize;
  // Log base 2 of size of the journal, in multiples of sizeof(Transaction). (Technically a
  // Transaction is variable-width due to the trailing ref array.)

  uint8_t lgBlockCount;
  // Log base 2 of number of blocks (content) in each local shard.

  uint8_t shardCount;
  // Number of shards in this block device, minus 1 since there are never 0 shards.

  uint32_t shardIds[256];
  // Each shard's location in the key space. shardIds of shards in a replica should be uniformly
  // distributed in the space of 32-bit integers. Each shard "owns" the IDs between its shardId
  // and the next higher shardId in the replica, using modular (wrap-around) arithmetic.
  //
  // The shardId for a particular block is (blockId >> (replicaId * 32)) % (1 << 32). Or, in
  // other words, if you defined it as uint32_t blockId[8], then shardId = blockId[replicaId].
  // This is valid up to 8 replicas, which should be enough for anyone.
  //
  // Properties of this algorithm:
  // - Consistent hashing: adding a new shard only requires moving data from one other shard.
  //   (But usually lots of shards are added at once.)
  // - Sharding of blocks is totally different between replicas, to avoid common hot spots.
};

static_assert(sizeof(Superblock) < 4096, "Superblock is more than one block.");

struct Bucket {
  // One bucket in the hashtable mapping block IDs to locally-stored blocks.

  UInt256 blockId;
  // Key. 0 = empty bucket. (The actual block 0 is never stored since it is known to map to the
  // block containing entirely zeros.)

  unsigned isMutable :1;
  // If true, this is a mutable block.
  //
  // TODO(someday): Unclear if this flag is strictly necessary.

  unsigned reserved0 :3;
  // Must be zero.

  unsigned offset :28;
  // Location (index) of block content within the content table. With 4k blocks and 28 bits this
  // can address 1TB of data.

  uint32_t refcount;
  // Number of references to this block. Usually always one for mutable blocks.

  uint32_t revision;
  // Revision counter. Incremented whenever the Bucket changes, which for mutable blocks includes
  // when the block is overwritten since this is always accomplished by writing the new data to
  // a new location and then updating `offset`.

  uint32_t reserved1[5];
  // Must be zero.
  //
  // TODO(someday):
  // - Record crypto nonce? (Could union with refcount.)
  // - Record location of the block in long-term storage.
  // - Implement policy for pushing blocks to long-term storage.
  // - Implement policy for purging blocks from local storage once they are in long-term storage.
};

static_assert(sizeof(Bucket) == 64, "Bucket size changed!");

struct Transaction {
  uint64_t id;
  // Transaction ID. Assigned sequentially per-shard.

  uint64_t firstIncompleteTx;
  // The transaction ID of the first incomplete transaction at the time

  uint32_t bucketIndex;
  // Which bucket to overwrite.

  unsigned trim :1;
  // Whether to perform a trim of `trimIndex`.

  unsigned reserved0 :3;
  // Must be zero.

  unsigned trimIndex :28;
  // Block index (in block content table) which can be freed after this transaction.

  uint64_t parentTxnId;
  uint32_t parentTxnShardId;
  // If `parentTxnId` is not ~0 then this transaction is occurring as a dependent of some other
  // transaction possibly occurring on a different shard. Until the parent transaction
  // completes, it's possible that we'll receive repeat requests to perform the child
  // transaction, which we'll need to de-dupe by noticing that it matches this journal entry.
  // Once we know the trigger transaction has completed, we can delete this journal entry.

  uint8_t refsAddedCount;
  uint8_t refsRemovedCount;
  // Number of block references added to or removed from this block as a result of this
  // transaction. The actual references are listed in the `refs` array.

  uint8_t reserved1[2];

  uint32_t reserved2[6];
  // Must be zero.
  //
  // TODO(someday):
  // - Verify valid transaction, e.g. with a checksum/hash, so that we can reliably find the end of
  //   the journal after power failure.

  Bucket newBucket;
  // New bucket contents.

  UInt256 refs[];
  // Array of references that were added to or removed from this block as a result of this
  // transaction. The referenced blocks will need to have their refcounts adjusted as part of this
  // transaction. The size of the array is `refsAddedCount + refsRemovedCount`, padded up to the
  // next `sizeof(Transaction)` boundary, so that all Transactions reside on such a boundary.
};

static_assert(sizeof(Transaction) == 128, "Journal size changed!");

struct Block {
  // One block. Note that the content is normally encrypted by XORing with a ChaCha20 stream whose
  // key and nonce are determined differently depending on the block type. This struct defines
  // what the block looks like after decryption.

  union {
    byte data[4096];
    // A regular data block containing bytes.
    //
    // The block is encrypted using its own 256-bit BLAKE2b hash (salted with the cluster ID) as
    // the key, and a nonce of zero.

    UInt256 blockTableSegment[128];
    // A block which contains a list of references to other blocks.
    //
    // Each element is the salted 256-bit BLAKE2b hash of the plaintext of a block, XORed with the
    // hash if the block contents were all-zero, so that the blockRef for an all-zero block is
    // all-zero.
    //
    // To get the block ID, hash this value again, then XOR that with the hash of an all-zero
    // blockRef, so that again the block ID of an all-zero block is all-zero.
    //
    // The block is encrypted using
  };
};

static_assert(sizeof(Block) == 4096, "Block size changed!");

}  // namespace
}  // namespace blackrock
