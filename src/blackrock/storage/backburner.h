// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_BACKBURNER_H_
#define BLACKROCK_STORAGE_BACKBURNER_H_

#include "journal-layer.h"

namespace blackrock {
namespace storage {

class Backburner {
public:
  void updateSize(JournalLayer::Transaction& txn, ObjectId target, int64_t delta);
  // Schedule to apply the given delta to `target`'s transitiveBlockCount. Normally `target` is the
  // parent of an object being modified in `txn` itself. On updating `target`, a new updateSize()
  // will be scheduled for its parent.
  //
  // If target == nullptr, this is a no-op.

  void removeTree(JournalLayer::Transaction& txn, ObjectId target);
  // Schedule to delete the given object and all its children. Normally `target` is a
  // former child of an object being modified in `txn` itself. On deleting `target`, a new
  // removeTree() will be scheduled for each of its children.

};

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_BACKBURNER_H_
