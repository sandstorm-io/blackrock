// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "high-level-object.h"

namespace blackrock {
namespace storage {

class StorageFactoryImpl: public StorageFactory::Server {
protected:
  kj::Promise<void> newBlob(NewBlobContext context) override {
    #error todo
  }

  kj::Promise<void> uploadBlob(UploadBlobContext context) override {
    #error todo
  }

  kj::Promise<void> newVolume(NewVolumeContext context) override {
    #error todo
  }

  kj::Promise<void> newImmutable(NewImmutableContext context) override {
    #error todo
  }

  kj::Promise<void> newAssignable(NewAssignableContext context) override {
    #error todo
  }

  kj::Promise<void> newAssignableCollection(NewAssignableCollectionContext context) override {
    #error todo
  }

  kj::Promise<void> newOpaque(NewOpaqueContext context) override {
    #error todo
  }

  kj::Promise<void> newTransaction(NewTransactionContext context) override {
    #error todo
  }

  kj::Promise<void> newImmutableCollection(NewImmutableCollectionContext context) override {
    #error todo
  }

private:
};

} // namespace storage
} // namespace blackrock

