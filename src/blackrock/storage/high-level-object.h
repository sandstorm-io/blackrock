// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_HIGH_LEVEL_OBJECT_H_
#define BLACKROCK_STORAGE_HIGH_LEVEL_OBJECT_H_

#include "basics.h"
#include <blackrock/storage/sibling.capnp.h>
#include "mid-level-object.h"

namespace blackrock {
namespace storage {

class OwnedStorageBase: public virtual OwnedStorage<>::Server {
public:
  using OwnedStorage<>::Server::thisCap;
};

OwnedStorage<>::Client makeHighLevelObject(
    MidLevelReader& reader, MidLevelWriter& writer, ObjectKey key,
    capnp::Capability::Client capToHold, kj::Maybe<OwnedStorageBase&>& weakref);

} // namespace storage
} // namespace blackrock

#endif // BLACKROCK_STORAGE_HIGH_LEVEL_OBJECT_H_
