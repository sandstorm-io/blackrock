// Sandstorm Blackrock
// Copyright (c) 2017 Sandstorm Development Group, Inc.
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

#ifndef BLACKROCK_GATEWAY_H_
#define BLACKROCK_GATEWAY_H_

#include "common.h"
#include <blackrock/machine.capnp.h>
#include "backend-set.h"
#include "cluster-rpc.h"
#include <sandstorm/gateway.h>
#include <kj/compat/http.h>
#include <kj/debug.h>

namespace blackrock {

class GatewayImpl: public GatewayImplBase::Server, private kj::HttpService,
                   private kj::TaskSet::ErrorHandler {
public:
  GatewayImpl(kj::Timer& timer, kj::Network& network, FrontendConfig::Reader config);

  void setConfig(FrontendConfig::Reader config);

protected:
  kj::Promise<void> reset(ResetContext context) override;
  kj::Promise<void> add(AddContext context) override;
  kj::Promise<void> remove(RemoveContext context) override;
  // We implement BackendSet<Frontend> directly rather than use BackendSetImpl because we want to
  // implement session affinity.

  kj::Promise<void> request(
      kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody, Response& response) override;

  kj::Promise<void> openWebSocket(
      kj::StringPtr url, const kj::HttpHeaders& headers, WebSocketResponse& response) override;

private:
  struct ShellReplica: kj::Refcounted {
    uint64_t backendId;
    kj::Own<kj::NetworkAddress> httpAddress;
    kj::Own<kj::NetworkAddress> smtpAddress;
    kj::Own<kj::HttpClient> shellHttp;
    sandstorm::GatewayRouter::Client router;
    sandstorm::GatewayService service;
    kj::Promise<void> cleanupLoop;

    ShellReplica(GatewayImpl& gateway, uint64_t backendId, Frontend::Instance::Reader instance);
  };

  class EntropySourceImpl: public kj::EntropySource {
  public:
    void generate(kj::ArrayPtr<byte> buffer) override;
  };

  class SmtpNetworkAddressImpl: public kj::NetworkAddress {
  public:
    SmtpNetworkAddressImpl(GatewayImpl& gateway): gateway(gateway) {}

    kj::Promise<kj::Own<kj::AsyncIoStream>> connect() override;
    kj::Own<kj::ConnectionReceiver> listen() override { KJ_UNIMPLEMENTED("fake address"); }
    kj::Own<kj::NetworkAddress> clone() override { KJ_UNIMPLEMENTED("fake address"); }
    kj::String toString() override { KJ_UNIMPLEMENTED("fake address"); }

  private:
    GatewayImpl& gateway;
  };

  kj::Timer& timer;
  kj::Network& network;

  sandstorm::GatewayService::Tables gatewayServiceTables;

  kj::Own<capnp::MallocMessageBuilder> configMessage;
  FrontendConfig::Reader config;
  sandstorm::WildcardMatcher wildcardHost;

  kj::Vector<kj::Maybe<kj::Own<ShellReplica>>> shellReplicas;
  // Maps replica number -> ShellReplica. Used as hash buckets when load balancing with affinity.
  // If a shell is down, its bucket will be null, and we have to search for an alternative.

  kj::Own<kj::ConnectionReceiver> httpReceiver;

  EntropySourceImpl entropySource;
  kj::HttpClientSettings clientSettings;

  kj::Own<kj::HttpHeaderTable> headerTable;
  kj::HttpServer httpServer;
  sandstorm::AltPortService altPortService;
  kj::HttpServer altPortHttpServer;
  SmtpNetworkAddressImpl smtpServer;
  sandstorm::GatewayTlsManager tlsManager;

  uint roundRobinCounter = 0;

  struct ReadyPair {
    kj::ForkedPromise<void> promise;
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
  };
  kj::Maybe<ReadyPair> readyPaf;

  kj::TaskSet tasks;

  GatewayImpl(kj::Timer& timer, kj::Network& network, FrontendConfig::Reader config,
              kj::HttpHeaderTable::Builder headerTableBuilder);

  kj::Promise<void> addFrontend(uint64_t backendId, Frontend::Client frontend);

  void addReplica(kj::Own<ShellReplica> newReplica);

  void setReplica(uint replicaNumber, kj::Maybe<kj::Own<ShellReplica>> newReplica,
                  kj::Maybe<uint64_t> requireBackendId = nullptr);
  kj::Promise<kj::Own<ShellReplica>> chooseReplica(uint64_t hash);

  uint64_t urlSessionHash(kj::StringPtr url, const kj::HttpHeaders& headers);

  void taskFailed(kj::Exception&& exception) override;
};

}  // namespace blackrock

#endif // BLACKROCK_GATEWAY_H_
