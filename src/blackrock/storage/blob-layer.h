// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_STORAGE_BLOB_LAYER_H_
#define BLACKROCK_STORAGE_BLOB_LAYER_H_

#include "basics.h"
#include <kj/async-io.h>
#include <kj/io.h>
#include <kj/one-of.h>

namespace blackrock {
namespace storage {

class BlobLayer {
public:
  class Object;
  class Temporary;

  virtual kj::Own<Object> createObject(ObjectId id, Xattr xattr, kj::Own<Temporary> content) = 0;
  // Create a new object. `content` contains the object's initial content; it must have
  // been created using this BlobLayer.

  virtual kj::Promise<kj::Maybe<kj::Own<Object>>> openObject(ObjectId id) = 0;
  // Open an existing object, returning null if no such object exists. This is async because
  // it may need to fetch metadata and/or content from a remote storage service.
  //
  // It is an error to re-open an object that is already open, but this is not necessarily checked
  // by the BlobLayer.

  virtual kj::Own<Temporary> newTemporary() = 0;
  // Create a new empty temporary content, to be used in the future to update an object's content.

  class Content {
    // Represents the contents of a byte blob, absent any metadata.

  public:
    struct Size {
      uint64_t endMarker;
      // Byte position where the file ends.

      uint32_t blockCount;
      // Number of 4k blocks consumed by the file, not counting holes.
    };

    virtual Size getSize() = 0;
    // Get the content's end marker position and storage usage.

    virtual uint64_t getStart() = 0;
    // Return the location of the first non-hole byte.

    virtual kj::Promise<void> read(uint64_t offset, kj::ArrayPtr<byte> buffer) = 0;
    // Read data starting at the given offset into the buffer. If the read goes past the end of
    // the file, the remaining bytes are zero'd.

    virtual void write(uint64_t offset, kj::ArrayPtr<const byte> data) = 0;
    // Write data to the file at the given offset. This returns immediately, but you must call
    // `sync()` to ensure the data is actually written. If a write extend beyond the file-end
    // marker, the marker will be moved to the end of the write.

    virtual void zero(uint64_t offset, uint64_t size) = 0;
    // Semantically equivalent to writing a buffer of the given size full of zeros at the given
    // offset, but whole blocks of zeros do not consume actual storage space; they become "holes".

    virtual kj::Promise<void> sync() = 0;
    // Resolves once all preceding calls to `write()` or `zero()` have reached disk.
  };

  class Temporary {
    // A "temporary file" stored on local disk which may be used for bookkeeping or may eventually
    // become persistent.
    //
    // Temporaries are deleted when `Temporary` is destroyed, even if they are assigned a recovery
    // ID. However, if the process terminates uncleanly (without running destructors), then
    // temporaries with a recovery ID can be recovered on the next run.

  public:
    virtual void setRecoveryId(RecoveryId id) = 0;
    // Make this content able to be recovered after a crash.
    //
    // If the content already has an ID, this atomically changes the ID.

    virtual void setRecoveryId(RecoveryId id, TemporaryXattr xattr) = 0;
    // Same as above but also assigns TemporaryXattr into the recovery file.
    //
    // Note that some recovery types have attributes while others do not. (In particular, staging
    // files do not have them since staging files may become regular objects which have a different
    // kind of xattr.)

    virtual void overwrite(TemporaryXattr xattr, kj::Own<Temporary> replacement) = 0;
    // Atomically replace this temporary's content with `replacement`'s content, consuming
    // `replacement` in the process. Any `Content` previously returned by `getContent()` is now
    // invalid.
    //
    // After this call, getContent() returns exactly the object replacement->getContent() returned
    // before the call.

    virtual TemporaryXattr getXattr() = 0;
    // Get this object's current attributes.

    virtual void setXattr(TemporaryXattr xattr) = 0;
    // Overwrite the attributes on this object.

    virtual Content& getContent() = 0;
    // Get the actual content body. The returned content is invalidated by replaceContent() (and on
    // destruction).
  };

  class Object {
    // Represents a blob object that has been "locked" by this shard. It is guaranteed that no
    // other shard can be modiying it concurrently.

  public:
    virtual void overwrite(Xattr xattr, kj::Own<Temporary> content) = 0;
    // Atomically replace the object's content with the given temporary content, which must have
    // been created using this BlobLayer.
    //
    // After this call, getContent() returns exactly the object content->getContent() returned
    // before the call.

    virtual Xattr getXattr() = 0;
    // Get this object's current attributes.

    virtual void setXattr(Xattr xattr) = 0;
    // Overwrite the attributes on this object.

    virtual void remove() = 0;
    // Delete this object permanently.

    virtual uint64_t getGeneration() = 0;
    // Returns a value that is incremented every time a change is made to this object handle, not
    // counting direct changes to content. Useful for optimistic concurrency. (The counter is not
    // persisted, so can only be compared to other values returned by `getGeneration()` on this
    // same `Object` instance.)

    virtual Content& getContent() = 0;
    // Get the object's content. The returned content is invalidated by overwrite() (and on
    // destruction).
  };

  class RecoveredTemporary {
    // A temporary recovered from a previous execution (that crashed before the temporary was
    // deleted).

  public:
    virtual RecoveryId getOldId() = 0;
    // Get the ID that this temporary had during the previous run, from which it is being
    // recovered.

    virtual TemporaryXattr getTemporaryXattr() = 0;
    virtual Content& getContent() = 0;
    // Read the recovered temporary.

    virtual kj::Own<Temporary> keepAs(RecoveryId newId) = 0;
    // Keep this temporary beyond the recovery stage by assigning it a new ID. After calling this,
    // the RecoveredTemporary object is no longer valid; proceed with the returned Temporary
    // object.

    virtual kj::Own<Temporary> keepAs(RecoveryId newId, TemporaryXattr newXattr) = 0;
    // Same as keepAs(RecoveryId) but assignes xattrs at the same time.

    virtual void keepAs(ObjectId newId, Xattr xattr) = 0;
    // Keep this temporary beyond the recovery stage by moving it into main storage such that
    // it becomes a full object. This will replace any existing object with the given ID.
  };

  class Recovery {
    // Recovery interface, for use in replying the journal after a crash. BlobLayer implementations
    // always start out in Recovery mode and then transition to normal operation when finish() is
    // called.

  public:
    virtual kj::Maybe<kj::Own<Object>> getObject(ObjectId id) = 0;
    // Get an object synchronously, for replaying a journal transaction against it. Returns null
    // if the object is not present locally.
    //
    // Since there is a journal entry to replay, it can be assumed that the object had been synced
    // to this machine at the time the journal entry was created. So, if the object is not present
    // now, then we can assume that this journal entry completed previously and the object was
    // subsequently deleted or moved off this machine. In this case there's no need to apply the
    // journaled changes at all, and there's never a need to bring a remote object local during
    // recovery.

    virtual kj::Array<kj::Own<RecoveredTemporary>> recoverTemporaries(RecoveryType type) = 0;
    // Recover all temporaries of the given type. This can only be called once per type.

    virtual BlobLayer& finish() = 0;
    // Finish recovery and transition into normal operation. No other methods on Recovery may be
    // called after this point.
  };
};

}  // namespace storage
}  // namespace blackrock

#endif // BLACKROCK_STORAGE_BLOB_LAYER_H_
