// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_BLOB_LOCAL_H_
#define BLACKROCK_STORAGE_BLOB_LOCAL_H_

#include "blob-layer.h"

namespace blackrock {
namespace storage {

class LocalBlobLayer: public BlobLayer::Recovery, private BlobLayer {
  // BlobLayer implementation backed by local disk.
  //
  // Directory tree layout:
  //   main: Contains object contents, named "o<objectId>" where <objectId> is the base64-encoded
  //     object ID (with '-' and '_' as digits 62 and 63). The 'o' prefix avoids filenames that
  //     start with '-', which are awkward to manipulate from the command-line.
  //   staging, backburner, switch, sync-state: Contains recovery files of the respective types.
  //     File names are decimal-encoded ID numbers.
  //   recovery: If this exists, then the process is currently in the recovery phase of startup, OR
  //     the process terminated abnormally during a recovery attempt and has not started a new
  //     recovery attempt yet. The directory contains the old copies of `staging`, etc. from before
  //     the recovery. (If any particular directory is missing, then the process terminated
  //     abnormally during the act of moving the temporary directories into `recovery`; it will
  //     try again next time.)
  //   recovered: The directory `recovery` is moved to `recovered` after recovery completes, and
  //     can safely be deleted once the process has exited (abnormally or not). (The reason this
  //     directory must stick around in the meantime is in case it contains temporaries that
  //     haven't otherwise been reassigned a new recovery ID -- Linux does not allow a file that
  //     had links, then was unlinked, to be linked again, so we have to keep the original links
  //     around. Probably this doesn't actually matter for any real use case; it's just to satisfy
  //     the API contract.)

public:
  LocalBlobLayer(kj::UnixEventPort& eventPort, int directoryFd);

  void testDirtyExit();
  // For testing purposes, simulates a dirty exit by causing all Object and Temporary instances
  // created through this BlobLayer *not* to clean up after themselves in their destructors. After
  // calling this, you should not call any other methods on any of these objects; you should
  // proceed to destroy them and then destroy the LocalBlobLayer. Recoverable temporaries will be
  // left on disk, so if you create a new LocalBlobLayer against the same directory it will be
  // able to recover them.

  kj::Maybe<kj::Own<BlobLayer::Object>> getObject(ObjectId id) override;
  kj::Array<kj::Own<RecoveredTemporary>> recoverTemporaries(RecoveryType type) override;
  BlobLayer& finish() override;

private:
  kj::UnixEventPort& eventPort;
  int directoryFd;
  bool recoveryFinished = false;
  bool testingDirtyExit = false;

  kj::Own<Object> createObject(ObjectId id, Xattr xattr, kj::Own<Temporary> content) override;
  kj::Promise<kj::Maybe<kj::Own<Object>>> openObject(ObjectId id) override;
  kj::Own<Temporary> newTemporary() override;

  kj::Maybe<kj::Own<Object>> getObjectInternal(ObjectId id);

  class ContentImpl;
  class ObjectImpl;
  class TemporaryImpl;
  class RecoveredTemporaryImpl;
};

}  // namespace storage
}  // namespace blackrock

#endif // BLACKROCK_STORAGE_BLOB_LOCAL_H_
