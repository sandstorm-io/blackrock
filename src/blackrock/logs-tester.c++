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
                           "Runs a log server on the given local address. Prints all logs to "
                           "stdout unless a log directory is provided.")
        .addOptionWithArg({'d', "dir"}, KJ_BIND_METHOD(*this, setLogDir), "<path>",
                          "save logs to a directory")
        .expectArg("<address>", KJ_BIND_METHOD(*this, runServer))
        .build();
  }

  kj::MainFunc getClientMain() {
    return kj::MainBuilder(context, "Blackrock logs tester",
                           "Runs a client with the given name connecting to the server at the "
                           "given address. Whatever you enter on stdin will be logged.")
        .expectArg("<name>", KJ_BIND_METHOD(*this, setName))
        .expectArg("<address>", KJ_BIND_METHOD(*this, runClient))
        .build();
  }

  kj::MainFunc getFakeMain() {
    return kj::MainBuilder(context, "Blackrock logs tester",
                           "Runs a fake server that closes connections immediately upon receipt.")
        .expectArg("<address>", KJ_BIND_METHOD(*this, runFake))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::Maybe<kj::AutoCloseFd> logDir;
  kj::StringPtr name;

  bool setLogDir(kj::StringPtr arg) {
    logDir = sandstorm::raiiOpen(arg, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    return true;
  }

  bool setName(kj::StringPtr arg) {
    name = arg;
    return true;
  }

  bool runServer(kj::StringPtr arg) {
    auto io = kj::setupAsyncIo();
    LogSink sink(logDir.map([](auto& fd) { return fd.get(); }));
    sink.acceptLoop(io.provider->getNetwork().parseAddress(arg).wait(io.waitScope)->listen())
        .wait(io.waitScope);
    return true;
  }

  bool runClient(kj::StringPtr arg) {
    sendStderrToLogSink(name, SimpleAddress::lookup(arg), ".");
    KJ_SYSCALL(dup2(STDERR_FILENO, STDOUT_FILENO));
    sandstorm::Subprocess({"cat"}).waitForSuccess();
    return true;
  }

  bool runFake(kj::StringPtr arg) {
    auto io = kj::setupAsyncIo();
    auto listener = io.provider->getNetwork().parseAddress(arg).wait(io.waitScope)->listen();
    for (;;) {
      // Accept connections and just close them right away.
      listener->accept().wait(io.waitScope);
    }
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::LogsTester);
