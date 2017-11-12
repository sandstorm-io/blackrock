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
#include "cluster-rpc.h"

namespace blackrock {

class FrontendImpl: public Frontend::Server {
public:
  FrontendImpl(kj::LowLevelAsyncIoProvider& llaiop,
               sandstorm::SubprocessSet& subprocessSet,
               FrontendConfig::Reader config, uint replicaNumber,
               SimpleAddress bindAddress);

  void setConfig(FrontendConfig::Reader config);

  BackendSet<StorageRootSet>::Client getStorageRootBackendSet();
  BackendSet<StorageFactory>::Client getStorageFactoryBackendSet();
  BackendSet<Worker>::Client getWorkerBackendSet();
  BackendSet<Mongo>::Client getMongoBackendSet();

protected:
  kj::Promise<void> getInstances(GetInstancesContext context) override;

private:
  class BackendImpl;
  struct MongoInfo;
  class Instance;

  kj::Own<capnp::MallocMessageBuilder> configMessage;
  FrontendConfig::Reader config;

  kj::Own<BackendSetImpl<StorageRootSet>> storageRoots;
  kj::Own<BackendSetImpl<StorageFactory>> storageFactories;
  kj::Own<BackendSetImpl<Worker>> workers;
  kj::Own<BackendSetImpl<Mongo>> mongos;

  kj::Vector<kj::Own<Instance>> instances;

  class Instance: private kj::TaskSet::ErrorHandler {
  public:
    Instance(FrontendImpl& frontend, kj::LowLevelAsyncIoProvider& llaiop,
             sandstorm::SubprocessSet& subprocessSet, uint frontendNumber, uint instanceNumber,
             SimpleAddress bindAddress,
             kj::PromiseFulfillerPair<sandstorm::Backend::Client> paf =
                   kj::newPromiseAndFulfiller<sandstorm::Backend::Client>());

    void restart(FrontendConfig::Reader config);

    void getInfo(Frontend::Instance::Builder info);

  private:
    kj::Timer& timer;
    sandstorm::SubprocessSet& subprocessSet;
    FrontendConfig::Reader config;
    uint replicaNumber;
    uint httpPort;
    uint smtpPort;
    SimpleAddress bindAddress;

    sandstorm::TwoPartyServerWithClientBootstrap capnpServer;
    pid_t pid = 0;
    kj::TaskSet tasks;

    kj::Promise<void> startExecLoop(MongoInfo&& mongoInfo, kj::AutoCloseFd&& backendClientFd);

    kj::Promise<void> execLoop(MongoInfo&& mongoInfo, kj::AutoCloseFd&& http,
                               kj::AutoCloseFd&& backendClientFd, kj::AutoCloseFd&& smtp);

    void taskFailed(kj::Exception&& exception) override;
  };
};

class MongoImpl: public Mongo::Server {
public:
  explicit MongoImpl(
      kj::Timer& timer, sandstorm::SubprocessSet& subprocessSet, SimpleAddress bindAddress,
      kj::PromiseFulfillerPair<void> passwordPaf = kj::newPromiseAndFulfiller<void>());

protected:
  kj::Promise<void> getConnectionInfo(GetConnectionInfoContext context) override;

private:
  kj::Timer& timer;
  sandstorm::SubprocessSet& subprocessSet;
  SimpleAddress bindAddress;
  kj::Maybe<kj::String> password;
  kj::ForkedPromise<void> passwordPromise;
  kj::Promise<void> execTask;

  kj::Promise<void> startExecLoop(kj::Own<kj::PromiseFulfiller<void>> passwordFulfiller);
  kj::Promise<void> execLoop(kj::PromiseFulfiller<void>& passwordFulfiller);
  kj::Promise<kj::String> initializeMongo();
  kj::Promise<void> mongoCommand(kj::String command, kj::StringPtr dbName = "meteor");
};

} // namespace blackrock

#endif // BLACKROCK_FRONTEND_H_
