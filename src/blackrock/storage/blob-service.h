// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_BLOB_SERVICE_H_
#define BLACKROCK_STORAGE_BLOB_SERVICE_H_

#include "blob-layer.h"

namespace blackrock {
namespace storage {

class BlobStorageService {
  // Represents a slow blob storage service, like Amazon S3 or Google Cloud Storage, which we use
  // for long-term storage.

public:
  typedef kj::ArrayPtr<const byte> Key;
  class UploadStream;

  virtual kj::Promise<kj::Maybe<kj::Own<kj::AsyncInputStream>>> get(Key key) = 0;
  // Download the blob with the given key, or resolve to null if no such blob exists.

  virtual kj::Own<UploadStream> put(Key key) = 0;
  // Upload a blob with the given key, overwriting any existing value.
  //
  // It's technically safe to clobber the old value when put() starts even if the upload ultimately
  // fails, as the calling machine has a reliable copy of the blob on its own disk. However, as an
  // extra precaution, it's a good idea to utilize any features the object storage system might
  // have to store a new "version" of the file while keeping old versions around for disaster
  // recovery.

  virtual kj::Promise<void> remove(Key key) = 0;
  // Delete the given object, or at least mark it for eventual deletion. (Depending on the use
  // case, it may be desirable to keep the blob around as a backup, or may be desirable to delete
  // immediately.)

  class UploadStream: public kj::AsyncOutputStream {
  public:
    kj::Promise<void> finish() = 0;
    // Indicates that no more data will be written and resolves once the upload has successfully
    // completed.
  };
};

class StorageServiceBlobLayer: public BlobLayer::Recovery {
  // BlobLayer implementation that uses a blob storage service like Amazon S3 or Google Cloud
  // Storage as its backing store and uses a local BlobLayer as a cache on top of that.

public:
  StorageServiceBlobLayer(BlobLayer::Recovery& localCache, BlobStorageService& service);

  kj::Promise<void> sync();
  // Stores all dirty objects back to the storage service. By the time that the returned promise
  // resolves, all data which existed at the time sync() was initially called is guaranteed to be
  // in the storage service.
};

}  // namespace storage
}  // namespace blackrock

#endif // BLACKROCK_STORAGE_BLOB_SERVICE_H_
