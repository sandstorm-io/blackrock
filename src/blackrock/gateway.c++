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

#include "gateway.h"
#include <sodium/randombytes.h>

namespace blackrock {

void GatewayImpl::EntropySourceImpl::generate(kj::ArrayPtr<byte> buffer) {
  randombytes(buffer.begin(), buffer.size());
}

GatewayImpl::GatewayImpl(kj::Timer& timer, kj::Network& network, FrontendConfig::Reader config)
    : GatewayImpl(timer, network, config, kj::HttpHeaderTable::Builder()) {}

GatewayImpl::GatewayImpl(kj::Timer& timer, kj::Network& network, FrontendConfig::Reader config,
                         kj::HttpHeaderTable::Builder headerTableBuilder)
    : timer(timer), network(network),
      gatewayServiceTables(headerTableBuilder),
      hXRealIp(headerTableBuilder.add("X-Real-IP")),
      headerTable(headerTableBuilder.build()),
      httpServer(timer, *headerTable, [this](kj::AsyncIoStream& conn) {
        return kj::heap<sandstorm::RealIpService>(static_cast<HttpService&>(*this), hXRealIp, conn);
      }),
      altPortService(*this, *headerTable, config.getBaseUrl(), config.getWildcardHost()),
      altPortHttpServer(timer, *headerTable, altPortService),
      smtpServer(*this),
      tlsManager(httpServer, smtpServer, config.hasPrivateKeyPassword()
          ? kj::Maybe<kj::StringPtr>(config.getPrivateKeyPassword())
          : kj::Maybe<kj::StringPtr>(nullptr)),
      tasks(*this) {
  clientSettings.entropySource = entropySource;

  setConfig(config);

  if (config.getBaseUrl().startsWith("https://")) {
    tasks.add(network.parseAddress("*", 80)
        .then([this](kj::Own<kj::NetworkAddress>&& addr) {
      auto listener = addr->listen();
      auto promise = altPortHttpServer.listenHttp(*listener);
      return promise.attach(kj::mv(listener));
    }));

    tasks.add(network.parseAddress("*", 443)
        .then([this](kj::Own<kj::NetworkAddress>&& addr) {
      auto listener = addr->listen();
      auto promise = tlsManager.listenHttps(*listener);
      return promise.attach(kj::mv(listener));
    }));
  } else {
    tasks.add(network.parseAddress("*", 80)
        .then([this](kj::Own<kj::NetworkAddress>&& addr) {
      auto listener = addr->listen();
      auto promise = httpServer.listenHttp(*listener);
      return promise.attach(kj::mv(listener));
    }));
  }

  tasks.add(network.parseAddress("*", 25)
      .then([this](kj::Own<kj::NetworkAddress>&& addr) {
    auto listener = addr->listen();
    auto promise = tlsManager.listenSmtp(*listener);
    return promise.attach(kj::mv(listener));
  }));

  tasks.add(network.parseAddress("*", 465)
      .then([this](kj::Own<kj::NetworkAddress>&& addr) {
    auto listener = addr->listen();
    auto promise = tlsManager.listenSmtps(*listener);
    return promise.attach(kj::mv(listener));
  }));

  capnp::Capability::Client masterGateway = kj::refcounted<sandstorm::CapRedirector>([this]() {
    return chooseReplica(roundRobinCounter++)
        .then([](kj::Own<ShellReplica> replica) -> capnp::Capability::Client {
      return replica->router;
    });
  });

  tasks.add(tlsManager.subscribeKeys(masterGateway.castAs<sandstorm::GatewayRouter>()));
}

void GatewayImpl::setConfig(FrontendConfig::Reader config) {
  configMessage = kj::heap<capnp::MallocMessageBuilder>();
  configMessage->setRoot(config);
  this->config = configMessage->getRoot<FrontendConfig>();
  wildcardHost = sandstorm::WildcardMatcher(config.getWildcardHost());

  // TODO(soon): Update all GatewayService instances to new config.
}

kj::Promise<void> GatewayImpl::request(
    kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
    kj::AsyncInputStream& requestBody, Response& response) {
  return chooseReplica(urlSessionHash(url, headers))
      .then([this,method,url,&headers,&requestBody,&response](kj::Own<ShellReplica> replica) {
    auto promise = replica->service.request(method, url, headers, requestBody, response);
    return promise.attach(kj::mv(replica));
  });
}

kj::Promise<void> GatewayImpl::openWebSocket(
    kj::StringPtr url, const kj::HttpHeaders& headers, WebSocketResponse& response) {
  return chooseReplica(urlSessionHash(url, headers))
      .then([this,url,&headers,&response](kj::Own<ShellReplica> replica) {
    auto promise = replica->service.openWebSocket(url,headers, response);
    return promise.attach(kj::mv(replica));
  });
}

kj::Promise<void> GatewayImpl::reset(ResetContext context) {
  shellReplicas.clear();

  auto params = context.getParams();
  auto promises = KJ_MAP(backend, params.getBackends()) {
    return addFrontend(backend.getId(), backend.getBackend());
  };
  context.releaseParams();
  return kj::joinPromises(kj::mv(promises));
}

kj::Promise<void> GatewayImpl::add(AddContext context) {
  auto params = context.getParams();
  auto promise = addFrontend(params.getId(), params.getBackend());
  context.releaseParams();
  return promise;
}

kj::Promise<void> GatewayImpl::remove(RemoveContext context) {
  uint64_t backendId = context.getParams().getId();

  for (auto& replica: shellReplicas) {
    KJ_IF_MAYBE(r, replica) {
      if (r->get()->backendId == backendId) {
        replica = nullptr;
      }
    }
  }

  return kj::READY_NOW;
}

kj::Promise<kj::Own<kj::AsyncIoStream>> GatewayImpl::SmtpNetworkAddressImpl::connect() {
  return gateway.chooseReplica(gateway.roundRobinCounter++)
      .then([this](kj::Own<GatewayImpl::ShellReplica>&& replica) {
    auto promise = replica->smtpAddress->connect();
    return promise.attach(kj::mv(replica));
  });
}

GatewayImpl::ShellReplica::ShellReplica(
    GatewayImpl& gateway, uint64_t backendId, Frontend::Instance::Reader instance)
    : backendId(backendId),
      httpAddress(SimpleAddress(instance.getHttpAddress()).onNetwork(gateway.network)),
      smtpAddress(SimpleAddress(instance.getSmtpAddress()).onNetwork(gateway.network)),
      shellHttp(kj::newHttpClient(gateway.timer, *gateway.headerTable, *httpAddress,
                                  gateway.clientSettings)),
      router(instance.getRouter()),
      service(gateway.timer, *shellHttp, router, gateway.gatewayServiceTables,
              gateway.config.getBaseUrl(), gateway.config.getWildcardHost(),
              gateway.config.hasTermsPublicId()
                  ? kj::Maybe<kj::StringPtr>(gateway.config.getTermsPublicId())
                  : kj::Maybe<kj::StringPtr>(nullptr)),
      cleanupLoop(service.cleanupLoop().eagerlyEvaluate([](kj::Exception&& e) {
        KJ_LOG(FATAL, "cleanupLoop() threw", e);
        abort();
      })) {}

kj::Promise<void> GatewayImpl::addFrontend(uint64_t backendId, Frontend::Client frontend) {
  return frontend.getInstancesRequest().send()
      .then([this,backendId](capnp::Response<Frontend::GetInstancesResults>&& response) {
    auto newInstances = response.getInstances();
    for (auto instance: newInstances) {
      kj::Maybe<kj::Own<ShellReplica>> replica =
          kj::refcounted<ShellReplica>(*this, backendId, instance);
      for (auto& slot: shellReplicas) {
        if (slot == nullptr) {
          slot = kj::mv(replica);
          break;
        }
      }
      if (replica != nullptr) {
        shellReplicas.add(kj::mv(replica));
      }
    }

    KJ_IF_MAYBE(r, readyPaf) {
      r->fulfiller->fulfill();
      readyPaf = nullptr;
    }
  });
}

kj::Promise<kj::Own<GatewayImpl::ShellReplica>> GatewayImpl::chooseReplica(uint64_t hash) {
  std::set<size_t> eliminated;
  while (eliminated.size() < shellReplicas.size()) {
    size_t bucket = hash % (shellReplicas.size() - eliminated.size());
    for (auto e: eliminated) {
      if (bucket >= e) {
        ++bucket;
      } else {
        break;
      }
    }

    KJ_ASSERT(bucket < shellReplicas.size());
    KJ_IF_MAYBE(replica, shellReplicas[bucket]) {
      return kj::addRef(**replica);
    }

    KJ_ASSERT(eliminated.insert(bucket).second);
  }

  if (readyPaf == nullptr) {
    auto paf = kj::newPromiseAndFulfiller<void>();
    readyPaf = ReadyPair { paf.promise.fork(), kj::mv(paf.fulfiller) };
  }

  return KJ_ASSERT_NONNULL(readyPaf).promise.addBranch().then([this,hash]() {
    return chooseReplica(hash);
  });
}

static bool isAllHex(kj::StringPtr text) {
  for (char c: text) {
    if ((c < '0' || '9' < c) &&
        (c < 'a' || 'f' < c) &&
        (c < 'A' || 'F' < c)) {
      return false;
    }
  }

  return true;
}

uint64_t GatewayImpl::urlSessionHash(kj::StringPtr url, const kj::HttpHeaders& headers) {
  KJ_IF_MAYBE(hostId, wildcardHost.match(headers)) {
    if (hostId->startsWith("ui-") || hostId->startsWith("api-") ||
        (hostId->size() == 20 && isAllHex(*hostId))) {
      // These cases are really served by a grain, and we only use a shell to connect to the right
      // grain. We bucket on hostname so that a particular grain is always looked up from the same
      // shell and through the same local grain capability cache. The hostname ends in hex, so we
      // can just parse it.
      KJ_ASSERT(hostId->size() >= 20);
      auto hex = hostId->slice(hostId->size() - 16);
      char* end;
      auto result = strtoull(hex.begin(), &end, 16);
      KJ_REQUIRE(end == hex.end(), "invalid hostname", *hostId);
      return result;
    }
  }

  // Recognize paths beginning with `sockjs` as probably being Meteor DDP connections.
  //
  // TODO(cleanup): Currently every installation can configure DDP to happen on an arbitrary host,
  //   as long as it maps to the server and doesn't already have some other designated purpose. We
  //   should probably standardize on the wildcard host ID "ddp" instead.
  auto parsedUrl = kj::Url::parse(url, kj::Url::HTTP_REQUEST);
  if (parsedUrl.path.size() >= 2 &&
      parsedUrl.path[0] == "sockjs") {
    // SockJS connections provide a 3-decimal-digit server ID in the path. BUT, it also has some
    // other endpoints like "info", so parse carefully.
    char* end;
    auto result = strtoul(parsedUrl.path[1].cStr(), &end, 10);
    if (end == parsedUrl.path[1].end()) {
      return result;
    }
  }

  // Anything else is probably a static asset. We hash the URL to make upstream caching more
  // efficient -- but probably these requests don't need to be load balanced anyway because CDN
  // caching ought to kick in here.

  // djb hash with xor
  // TODO(someday):  Add hashing library to KJ.
  uint64_t result = 5381;
  for (char c: url) {
    result = (result * 33) ^ c;
  }
  return result;
}

void GatewayImpl::taskFailed(kj::Exception&& exception) {
  KJ_LOG(FATAL, exception);

  // Better restart since we may be in a degraded state.
  abort();
}

}  // namespace blackrock
