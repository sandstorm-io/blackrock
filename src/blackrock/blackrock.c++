// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include <kj/main.h>
#include <kj/async-io.h>
#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <sandstorm/version.h>
#include <sandstorm/util.h>
#include <blackrock/machine.capnp.h>
#include "cluster-rpc.h"

namespace blackrock {

static struct sockaddr_in ip4Wildcard() {
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  return addr;
}

class MachineImpl: public Machine::Server {
  // TODO(security): For most become*() methods, we should probably actually spawn a child process.

public:
  kj::Promise<void> becomeWorker(BecomeWorkerContext context) override {

  }

private:
};

class MasterImpl: public Master::Server {
public:
  kj::Promise<void> addMachine(AddMachineContext context) override {
  }

private:
};

class Main {
public:
  Main(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock.")
        .addSubCommand("master", KJ_BIND_METHOD(*this, getMasterMain), "run as master node")
        .addSubCommand("slave", KJ_BIND_METHOD(*this, getSlaveMain), "run as slave node")
        .build();
  }

  kj::MainFunc getMasterMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock master.")
        .callAfterParsing(KJ_BIND_METHOD(*this, runMaster))
        .build();
  }

  kj::MainFunc getSlaveMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock slave, taking commands from the given master. "
                           "<master-path> is the base64-encoded serialized VatPath of the master.")
        .expectArg("<master-path>", KJ_BIND_METHOD(*this, runSlave))
        .build();
  }

private:
  kj::ProcessContext& context;

  bool runMaster() {
    auto ioContext = kj::setupAsyncIo();

    VatNetwork network(ioContext.provider->getNetwork(),
        ioContext.provider->getTimer(), ip4Wildcard());
    auto rpcSystem = capnp::makeRpcClient(network);

    return true;
  }

  bool runSlave(kj::StringPtr masterAddr) {
    auto ioContext = kj::setupAsyncIo();

    VatNetwork network(ioContext.provider->getNetwork(),
        ioContext.provider->getTimer(), ip4Wildcard());
    auto rpcSystem = capnp::makeRpcClient(network);

    auto bytes = sandstorm::base64Decode(masterAddr);
    auto words = kj::heapArray<capnp::word>(bytes.size() / 8);
    memcpy(words.begin(), bytes.begin(), words.asBytes().size());
    capnp::FlatArrayMessageReader masterAddrReader(words);
    auto masterPath = masterAddrReader.getRoot<VatPath>();

    auto master = rpcSystem.bootstrap(masterPath).castAs<Master>();
    auto request = master.addMachineRequest();
    request.setMachine(kj::heap<MachineImpl>());
    request.send().wait(ioContext.waitScope);

    // Loop forever handling messages.
    kj::NEVER_DONE.wait(ioContext.waitScope);

    return true;
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::Main)
