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

static kj::String getImageName() {
  char buffer[256];
  ssize_t n;
  KJ_SYSCALL(n = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1));
  buffer[n] = '\0';
  kj::StringPtr exeName(buffer);
  return sandstorm::trim(exeName.slice(KJ_ASSERT_NONNULL(exeName.findLast('/')) + 1));
}

}  // namespace

GceDriver::GceDriver(sandstorm::SubprocessSet& subprocessSet,
                     kj::LowLevelAsyncIoProvider& ioProvider,
                     GceConfig::Reader config)
    : subprocessSet(subprocessSet), ioProvider(ioProvider), config(config), image(getImageName()),
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
  auto exitPromise = gceCommand({"instances", "list", "--format", "text", "-q"},
                                STDIN_FILENO, writeEnd);

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
        auto name = sandstorm::trim(text.slice(strlen("name:"), eol));
        if (!name.startsWith("master") &&
            !name.startsWith("build") &&
            !name.startsWith("nginx")) {
          result.add(MachineId(name));
        }
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
      { "instances", "create", idStr, "--image", image, "--no-scopes", "-q" });
  kj::StringPtr startupScript;
  kj::StringPtr instanceType;
  switch (id.type) {
    case ComputeDriver::MachineType::STORAGE: {
      instanceType = config.getInstanceTypes().getStorage();

      // Attach necessary disk.
      auto param = kj::str("--disk=name=", id, "-data,mode=rw,device-name=blackrock");
      args.add(param);
      scratch.add(kj::mv(param));
      startupScript =
          "#! /bin/sh\n"
          "mkdir -p /var/blackrock/bundle/storage\n"
          "mount /dev/disk/by-id/google-blackrock /var/blackrock/bundle/storage\n";
      break;
    }

    case ComputeDriver::MachineType::WORKER:
      instanceType = config.getInstanceTypes().getWorker();
      break;

    case ComputeDriver::MachineType::COORDINATOR:
      instanceType = config.getInstanceTypes().getCoordinator();
      break;

    case ComputeDriver::MachineType::FRONTEND:
      instanceType = config.getInstanceTypes().getFrontend();
      break;

    case ComputeDriver::MachineType::MONGO: {
      instanceType = config.getInstanceTypes().getMongo();

      // Attach necessary disk.
      auto param = kj::str("--disk=name=", id, "-data,mode=rw,device-name=blackrock");
      args.add(param);
      scratch.add(kj::mv(param));
      startupScript =
          "#! /bin/sh\n"
          "mkdir -p /var/blackrock/bundle/mongo\n"
          "mount /dev/disk/by-id/google-blackrock /var/blackrock/bundle/mongo\n";
      break;
    }
  }

  args.add("--machine-type");
  args.add(instanceType);

  if (startupScript == nullptr) {
    return gceCommand(args);
  } else {
    // We'll pass the startup script via stdin.
    args.add("--metadata-from-file=startup-script=/dev/stdin");

    // No need for async pipe since the startup script almost certainly won't fill the pipe buffer
    // anyhow, and even if it did, the tool immediately reads it before doing other stuff.
    auto pipe = sandstorm::Pipe::make();
    auto promise = gceCommand(args, pipe.readEnd);
    pipe.readEnd = nullptr;
    kj::FdOutputStream(kj::mv(pipe.writeEnd)).write(startupScript.begin(), startupScript.size());
    return kj::mv(promise);
  }
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
  auto target = kj::str("root@", name);
  kj::Vector<kj::StringPtr> args;
  auto command = kj::str("/blackrock/bin/blackrock slave --log ", addr, " if4:eth0");
  args.addAll(kj::ArrayPtr<const kj::StringPtr>({
      "ssh", target, "--command", command, "-q"}));
  if (requireRestartProcess) args.add("-r");

  auto exitPromise = gceCommand(args, stdinReadEnd, stdoutWriteEnd);

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
  return gceCommand({"instances", "delete", kj::str(id), "-q"});
}

kj::Promise<void> GceDriver::gceCommand(kj::ArrayPtr<const kj::StringPtr> args,
                                        int stdin, int stdout) {
  auto fullArgs = kj::heapArrayBuilder<const kj::StringPtr>(args.size() + 4);
  fullArgs.add("gcloud");
  fullArgs.add("--project");
  fullArgs.add(config.getProject());
  fullArgs.add("compute");
  fullArgs.addAll(args);

  kj::Vector<kj::StringPtr> env;
  auto newEnv = kj::str("CLOUDSDK_COMPUTE_ZONE=", config.getZone());
  env.add(newEnv);
  for (char** envp = environ; *envp != nullptr; ++envp) {
    kj::StringPtr e = *envp;
    if (!e.startsWith("CLOUDSDK_COMPUTE_ZONE=")) {
      env.add(e);
    }
  }

  sandstorm::Subprocess::Options options(fullArgs.finish());
  KJ_DBG(kj::strArray(options.argv, " "));
  options.stdin = stdin;
  options.stdout = stdout;
  options.environment = kj::ArrayPtr<const kj::StringPtr>(env);
  return subprocessSet.waitForSuccess(kj::mv(options));
}

} // namespace blackrock

