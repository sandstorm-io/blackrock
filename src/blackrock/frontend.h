// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_FRONTEND_H_
#define BLACKROCK_FRONTEND_H_

#include "common.h"
#include <blackrock/frontend.capnp.h>
#include <blackrock/storage.capnp.h>
#include <blackrock/cluster-rpc.capnp.h>
#include <blackrock/worker.capnp.h>
#include <sandstorm/backend.capnp.h>
#include <capnp/message.h>
#include <sandstorm/util.h>
#include <kj/async-io.h>
#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include "backend-set.h"

namespace blackrock {

class FrontendImpl: public Frontend::Server, private kj::TaskSet::ErrorHandler {
public:
  FrontendImpl(kj::AsyncIoProvider& ioProvider, sandstorm::SubprocessSet& subprocessSet,
               FrontendConfig::Reader config);

  void setConfig(FrontendConfig::Reader config);

  BackendSet<StorageRootSet>::Client getStorageRootBackendSet();
  BackendSet<StorageFactory>::Client getStorageFactoryBackendSet();
  BackendSet<Worker>::Client getWorkerBackendSet();

private:
  sandstorm::SubprocessSet& subprocessSet;
  kj::Own<capnp::MallocMessageBuilder> configMessage;
  FrontendConfig::Reader config;
  capnp::TwoPartyServer capnpServer;

  kj::Own<BackendSetImpl<StorageRootSet>> storageRoots;
  kj::Own<BackendSetImpl<StorageFactory>> storageFactories;
  kj::Own<BackendSetImpl<Worker>> workers;

  pid_t frontendPid = 0;
  kj::TaskSet tasks;

  kj::Promise<void> execLoop();

  void taskFailed(kj::Exception&& exception) override;

  class BackendImpl;
};

} // namespace blackrock

#endif // BLACKROCK_FRONTEND_H_
