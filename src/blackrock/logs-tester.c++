// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "logs.h"
#include <kj/main.h>
#include <sandstorm/util.h>
#include "cluster-rpc.h"

namespace blackrock {

class LogsTester {
  // A test program for the logging system.

public:
  LogsTester(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Blackrock logs tester", "Tests logs.")
        .addSubCommand("server", KJ_BIND_METHOD(*this, getServerMain), "run a logs server")
        .addSubCommand("client", KJ_BIND_METHOD(*this, getClientMain), "run a logs client")
        .addSubCommand("fake", KJ_BIND_METHOD(*this, getFakeMain), "run a fake log server")
        .build();
  }

  kj::MainFunc getServerMain() {
    return kj::MainBuilder(context, "Blackrock logs tester",
                           "Runs a log server locally and arranges for clients to be able "
                           "to connect to it. Prints all logs to stdout unless a log directory "
                           "is provided.")
        .addOptionWithArg({'d', "dir"}, KJ_BIND_METHOD(*this, setLogDir), "<path>",
                          "save logs to a directory")
        .callAfterParsing(KJ_BIND_METHOD(*this, runServer))
        .build();
  }

  kj::MainFunc getClientMain() {
    return kj::MainBuilder(context, "Blackrock logs tester",
                           "Runs a client with the given name connecting to the local server. "
                           "Whatever you enter on stdin will be logged.")
        .expectArg("<name>", KJ_BIND_METHOD(*this, setName))
        .callAfterParsing(KJ_BIND_METHOD(*this, runClient))
        .build();
  }

  kj::MainFunc getFakeMain() {
    return kj::MainBuilder(context, "Blackrock logs tester",
                           "Runs a fake server that closes connections immediately upon receipt.")
        .callAfterParsing(KJ_BIND_METHOD(*this, runFake))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::Maybe<kj::AutoCloseFd> logDir;
  kj::StringPtr name;
  kj::StringPtr addrFile = "/tmp/blackrock-logs-tester-addr";

  bool setLogDir(kj::StringPtr arg) {
    logDir = sandstorm::raiiOpen(arg, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    return true;
  }

  bool setName(kj::StringPtr arg) {
    name = arg;
    return true;
  }

  bool runServer() {
    auto io = kj::setupAsyncIo();
    LogSink sink(logDir.map([](auto& fd) { return fd.get(); }));
    sink.acceptLoop(listen(io.provider->getNetwork())).wait(io.waitScope);
    return true;
  }

  bool runClient() {
    runLogClient(name, addrFile, "/tmp");
    return true;
  }

  bool runFake() {
    auto io = kj::setupAsyncIo();
    auto listener = listen(io.provider->getNetwork());
    for (;;) {
      // Accept connections and just close them right away.
      listener->accept().wait(io.waitScope);
    }
  }

  kj::Own<kj::ConnectionReceiver> listen(kj::Network& network) {
    auto addr = SimpleAddress::getLocalhost(AF_INET);
    auto listener = addr.onNetwork(network)->listen();
    addr.setPort(listener->getPort());
    kj::FdOutputStream(sandstorm::raiiOpen(addrFile, O_WRONLY | O_CREAT | O_TRUNC))
        .write(&addr, sizeof(addr));
    return listener;
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::LogsTester);
