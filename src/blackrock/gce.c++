// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "gce.h"
#include <kj/debug.h>
#include <unistd.h>
#include <fcntl.h>
#include <sandstorm/util.h>
#include <capnp/serialize-async.h>

namespace blackrock {

namespace {

// TODO(cleanup): Share this code with version in master.c++.
kj::Promise<kj::String> readAllAsync(kj::AsyncInputStream& input,
                                     kj::Vector<char> buffer = kj::Vector<char>()) {
  buffer.resize(buffer.size() + 4096);
  auto promise = input.tryRead(buffer.end() - 4096, 4096, 4096);
  return promise.then([KJ_MVCAP(buffer),&input](size_t n) mutable -> kj::Promise<kj::String> {
    if (n < 4096) {
      buffer.resize(buffer.size() - 4096 + n);
      buffer.add('\0');
      return kj::String(buffer.releaseAsArray());
    } else {
      return readAllAsync(input, kj::mv(buffer));
    }
  });
}

}  // namespace

GceDriver::GceDriver(sandstorm::SubprocessSet& subprocessSet,
                     kj::LowLevelAsyncIoProvider& ioProvider,
                     kj::StringPtr imageName, kj::StringPtr zone)
    : subprocessSet(subprocessSet), ioProvider(ioProvider), imageName(imageName), zone(zone),
      masterBindAddress(SimpleAddress::getInterfaceAddress(AF_INET, "eth0")),
      logSink(nullptr), logTask(nullptr), logSinkAddress(masterBindAddress) {
  // Create socket for the log sink acceptor.
  int sock;
  KJ_SYSCALL(sock = socket(masterBindAddress.family(),
      SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
  {
    KJ_ON_SCOPE_FAILURE(close(sock));
    logSinkAddress.setPort(0);
    KJ_SYSCALL(bind(sock, logSinkAddress.asSockaddr(), logSinkAddress.getSockaddrSize()));
    KJ_SYSCALL(listen(sock, SOMAXCONN));

    // Read back the assigned port number.
    logSinkAddress = SimpleAddress::getLocal(sock);
  }

  // Accept log connections.
  auto listener = ioProvider.wrapListenSocketFd(sock,
      kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
      kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

  logTask = logSink.acceptLoop(kj::mv(listener))
      .eagerlyEvaluate([](kj::Exception&& exception) {
    KJ_LOG(ERROR, "LogSink accept loop failed", exception);
  });
}

GceDriver::~GceDriver() noexcept(false) {}

SimpleAddress GceDriver::getMasterBindAddress() {
  return masterBindAddress;
}

auto GceDriver::listMachines() -> kj::Promise<kj::Array<MachineId>> {
  int fds[2];
  KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
  kj::AutoCloseFd writeEnd(fds[1]);
  auto input = ioProvider.wrapInputFd(fds[0],
      kj::LowLevelAsyncIoProvider::Flags::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::Flags::ALREADY_CLOEXEC);

  // TODO(cleanup): Use `--format json` here (but then we need a json parser).
  sandstorm::Subprocess::Options options({
      "gcloud", "compute", "instances", "list", "--format", "text", "-q"});
  options.stdout = writeEnd;
  auto exitPromise = subprocessSet.waitForSuccess(kj::mv(options));

  auto outputPromise = readAllAsync(*input);
  return outputPromise.attach(kj::mv(input))
      .then([KJ_MVCAP(exitPromise)](kj::String allText) mutable {
    kj::Vector<MachineId> result;

    kj::StringPtr text = allText;

    // Parse lines until there are no more.
    while (text.size() > 0) {
      uint eol = KJ_ASSERT_NONNULL(text.findFirst('\n'));

      // Look for "name:" lines, which are instance names. Ignore everything else.
      if (text.startsWith("name:")) {
        result.add(MachineId(sandstorm::trim(text.slice(strlen("name:"), eol))));
      }

      text = text.slice(eol + 1);
    }

    return exitPromise.then([KJ_MVCAP(result)]() mutable { return result.releaseAsArray(); });
  });
}

kj::Promise<void> GceDriver::boot(MachineId id) {
  kj::Vector<kj::StringPtr> args;
  kj::Vector<kj::String> scratch;
  auto idStr = kj::str(id);
  args.addAll(std::initializer_list<const kj::StringPtr>
      { "gcloud", "compute", "instances", "create", idStr,
        "--zone", zone, "--image", imageName, "-q" });
  switch (id.type) {
    case ComputeDriver::MachineType::STORAGE: {
      // Attach necessary disk.
      auto param = kj::str("name=", id, "-data,mode=rw,device-name=blackrock-storage");
      scratch.add(kj::mv(param));
      args.add("--disk");
      args.add(param);
      break;
    }

    case ComputeDriver::MachineType::MONGO: {
      // Attach necessary disk.
      auto param = kj::str("name=", id, "-data,mode=rw,device-name=blackrock-mongo");
      scratch.add(kj::mv(param));
      args.add("--disk");
      args.add(param);
      break;
    }

    case ComputeDriver::MachineType::WORKER:
      // Workers need the RAM.
      args.add("--machine-type");
      args.add("n1-highmem-2");
      break;

    default:
      break;
  }

  return subprocessSet.waitForSuccess(sandstorm::Subprocess::Options(args.asPtr()));
}

kj::Promise<VatPath::Reader> GceDriver::run(
    MachineId id, blackrock::VatId::Reader masterVatId, bool requireRestartProcess) {
  kj::String name = kj::str(id);

  int fds[2];
  KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
  kj::AutoCloseFd stdinReadEnd(fds[0]);
  auto stdinWriteEnd = ioProvider.wrapOutputFd(fds[1],
      kj::LowLevelAsyncIoProvider::Flags::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::Flags::ALREADY_CLOEXEC);
  KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
  kj::AutoCloseFd stdoutWriteEnd(fds[1]);
  auto stdoutReadEnd = ioProvider.wrapInputFd(fds[0],
      kj::LowLevelAsyncIoProvider::Flags::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::Flags::ALREADY_CLOEXEC);

  auto addr = kj::str(logSinkAddress, '/', name);
  kj::Vector<kj::StringPtr> args;
  args.addAll(kj::ArrayPtr<const kj::StringPtr>({
      "gcloud", "compute", "ssh", kj::str("root@", name), "--", "/blackrock/bin/blackrock",
      "slave", "--log", addr, "if4:eth0", "-q"}));
  if (requireRestartProcess) args.add("-r");
  sandstorm::Subprocess::Options options(args.asPtr());
  options.stdin = stdinReadEnd;
  options.stdout = stdoutWriteEnd;
  auto exitPromise = subprocessSet.waitForSuccess(kj::mv(options));

  auto message = kj::heap<capnp::MallocMessageBuilder>(masterVatId.totalSize().wordCount + 4);
  message->setRoot(masterVatId);

  auto& stdoutReadEndRef = *stdoutReadEnd;
  return capnp::writeMessage(*stdinWriteEnd, *message)
      .attach(kj::mv(stdinWriteEnd), kj::mv(message))
      .then([&stdoutReadEndRef]() {
    return capnp::readMessage(stdoutReadEndRef);
  }).then([this,id,KJ_MVCAP(exitPromise),KJ_MVCAP(stdoutReadEnd)](
      kj::Own<capnp::MessageReader> reader) mutable {
    auto path = reader->getRoot<VatPath>();
    vatPaths[id] = kj::mv(reader);
    return exitPromise.then([path]() { return path; });
  });
}

kj::Promise<void> GceDriver::stop(MachineId id) {
  return subprocessSet.waitForSuccess({
      "gcloud", "compute", "instances", "delete", kj::str(id), "-q"});
}

} // namespace blackrock

