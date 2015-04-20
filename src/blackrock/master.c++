// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "master.h"
#include <map>
#include <set>
#include <kj/debug.h>
#include <kj/vector.h>
#include <blackrock/machine.capnp.h>
#include <signal.h>
#include <sandstorm/util.h>
#include <capnp/serialize-async.h>

namespace blackrock {

namespace {

struct TypeCounts {
  uint count = 0;
  uint maxIndex = 0;
};

class ErrorLogger: public kj::TaskSet::ErrorHandler {
public:
  void taskFailed(kj::Exception&& exception) override {
    KJ_LOG(ERROR, exception);
  }
};

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

void runMaster(kj::AsyncIoContext& ioContext, ComputeDriver& driver, MasterConfig::Reader config,
               bool shouldRestart) {
  KJ_REQUIRE(config.getWorkerCount() > 0, "need at least one worker");

  kj::Vector<kj::Promise<void>> startupTasks;

  uint workerCount = config.getWorkerCount();

  std::map<ComputeDriver::MachineType, uint> expectedCounts;
  expectedCounts[ComputeDriver::MachineType::STORAGE] = 1;
  expectedCounts[ComputeDriver::MachineType::WORKER] = workerCount;
  expectedCounts[ComputeDriver::MachineType::FRONTEND] = 1;

  VatPath::Reader storagePath;
  auto workerPaths = kj::heapArray<VatPath::Reader>(config.getWorkerCount());
  VatPath::Reader frontendPath;

  KJ_LOG(INFO, "examining currently-running machines...");

  // Shut down any machines that we don't need anymore.
  for (auto& machine: driver.listMachines().wait(ioContext.waitScope)) {
    if (machine.index >= expectedCounts[machine.type]) {
      KJ_LOG(INFO, "STOPPING", machine);
      startupTasks.add(driver.stop(machine));
    }
  }

  auto start = [&startupTasks,&driver,shouldRestart](
      ComputeDriver::MachineId id, VatPath::Reader& pathSlot) {
    KJ_LOG(INFO, "STARTING", id);
    startupTasks.add(driver.start(id, shouldRestart).then([id,&pathSlot](auto path) {
      KJ_LOG(INFO, "READY", id);
      pathSlot = path;
    }));
  };

  start({ ComputeDriver::MachineType::STORAGE, 0 }, storagePath);

  {
    for (uint i = 0; i < workerCount; i++) {
      start({ ComputeDriver::MachineType::WORKER, i }, workerPaths[i]);
    }
  }

  start({ ComputeDriver::MachineType::FRONTEND, 0 }, frontendPath);

  KJ_LOG(INFO, "waiting for startup tasks...");
  kj::joinPromises(startupTasks.releaseAsArray()).wait(ioContext.waitScope);

  // -------------------------------------------------------------------------------------

  VatNetwork network(ioContext.provider->getNetwork(), ioContext.provider->getTimer(),
                     driver.getMasterBindAddress());
  auto rpcSystem = capnp::makeRpcClient(network);

  ErrorLogger logger;
  kj::TaskSet tasks(logger);

  // Start storage.
  auto storage = rpcSystem.bootstrap(storagePath).castAs<Machine>()
      .becomeStorageRequest().send();

  // For now, tell the storage that it has no back-ends.
  tasks.add(storage.getSiblingSet().resetRequest().send().then([](auto){}));
  tasks.add(storage.getHostedRestorerSet().resetRequest().send().then([](auto){}));
  tasks.add(storage.getGatewayRestorerSet().resetRequest().send().then([](auto){}));

  // Start workers (the ones that are booted, anyway).
  kj::Vector<Worker::Client> workers;
  for (auto& workerPath: workerPaths) {
    workers.add(rpcSystem.bootstrap(workerPath).castAs<Machine>()
        .becomeWorkerRequest().send().getWorker());
  }

  // Start front-end.
  auto frontend = ({
    auto req = rpcSystem.bootstrap(frontendPath).castAs<Machine>().becomeFrontendRequest();
    req.setConfig(config.getFrontendConfig());
    req.send();
  });

  // Set up backends for frontend.
  tasks.add(frontend.getStorageRestorerSet().resetRequest().send().then([](auto){}));
  {
    auto req = frontend.getStorageRootSet().resetRequest();
    auto backend = req.initBackends(1)[0];
    backend.setId(0);
    backend.setBackend(storage.getRootSet());
    tasks.add(req.send().then([](auto){}));
  }
  {
    auto req = frontend.getStorageFactorySet().resetRequest();
    auto backend = req.initBackends(1)[0];
    backend.setId(0);
    backend.setBackend(storage.getStorageFactory());
    tasks.add(req.send().then([](auto){}));
  }
  tasks.add(frontend.getHostedRestorerSet().resetRequest().send().then([](auto){}));
  {
    auto req = frontend.getWorkerSet().resetRequest();
    auto list = req.initBackends(workers.size());
    for (auto i: kj::indices(workers)) {
      auto backend = list[i];
      backend.setId(i);
      backend.setBackend(workers[i]);
    }
    tasks.add(req.send().then([](auto){}));
  }

  // Loop forever handling messages.
  KJ_LOG(INFO, "Blackrock READY");
  kj::NEVER_DONE.wait(ioContext.waitScope);
  KJ_UNREACHABLE;
}

// =======================================================================================

ComputeDriver::MachineId::MachineId(kj::StringPtr name) {
  kj::StringPtr indexStr;

#define HANDLE_CASE(TYPE, NAME) \
  if (name.startsWith(NAME)) { \
    type = MachineType::TYPE; \
    indexStr = name.slice(strlen(NAME)); \
  }

  HANDLE_CASE(STORAGE, "storage")
  else HANDLE_CASE(WORKER, "worker")
  else HANDLE_CASE(COORDINATOR, "coordinator")
  else HANDLE_CASE(FRONTEND, "frontend")
  else KJ_FAIL_ASSERT("couldn't parse machine ID", name);
#undef HANDLE_CASE

  char* end;
  index = strtoul(indexStr.cStr(), &end, 10);
  KJ_ASSERT(*end == '\0' && indexStr.size() > 0 &&
            (indexStr[0] != '0' || indexStr.size() == 1),
            "could not parse machine ID", name);
}

kj::String ComputeDriver::MachineId::toString() const {
  kj::StringPtr typeName;
  switch (type) {
    case MachineType::STORAGE    : typeName = "storage"    ; break;
    case MachineType::WORKER     : typeName = "worker"     ; break;
    case MachineType::COORDINATOR: typeName = "coordinator"; break;
    case MachineType::FRONTEND   : typeName = "frontend"   ; break;
  }

  return kj::str(typeName, index);
}

// =======================================================================================

VagrantDriver::VagrantDriver(sandstorm::SubprocessSet& subprocessSet,
                             kj::LowLevelAsyncIoProvider& ioProvider)
    : subprocessSet(subprocessSet), ioProvider(ioProvider),
      masterBindAddress(SimpleAddress::getInterfaceAddress(AF_INET, "vboxnet0")),
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

VagrantDriver::~VagrantDriver() noexcept(false) {}

SimpleAddress VagrantDriver::getMasterBindAddress() {
  return masterBindAddress;
}

auto VagrantDriver::listMachines() -> kj::Promise<kj::Array<MachineId>> {
  char* cwd = get_current_dir_name();
  KJ_DEFER(free(cwd));

  int fds[2];
  KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
  kj::AutoCloseFd writeEnd(fds[1]);
  auto input = ioProvider.wrapInputFd(fds[0],
      kj::LowLevelAsyncIoProvider::Flags::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::Flags::ALREADY_CLOEXEC);

  sandstorm::Subprocess::Options options({"vagrant", "global-status"});
  options.stdout = writeEnd;
  auto exitPromise = subprocessSet.waitForSuccess(kj::mv(options));

  // Unfortunately, `vagrant global-status` does not appear to support `--machine-readable`; the
  // output is simply empty. So... we parse.
  auto outputPromise = readAllAsync(*input);
  return outputPromise.attach(kj::mv(input))
      .then([KJ_MVCAP(exitPromise)](kj::String allText) mutable {
    kj::Vector<MachineId> result;

    kj::StringPtr text = allText;

    text = text.slice(KJ_ASSERT_NONNULL(text.findFirst('\n')) + 1);
    KJ_ASSERT(text.startsWith("----------------"));
    text = text.slice(KJ_ASSERT_NONNULL(text.findFirst('\n')) + 1);

    if (!text.startsWith("There are no active")) {
      while (!text.startsWith("\n") && !text.startsWith("\r\n")) {
        text = text.slice(KJ_ASSERT_NONNULL(text.findFirst(' ')));
        while (text.startsWith(" ")) text = text.slice(1);

        auto name = kj::str(text.slice(0, KJ_ASSERT_NONNULL(text.findFirst(' '))));
        result.add(MachineId(name));

        text = text.slice(KJ_ASSERT_NONNULL(text.findFirst('\n')) + 1);
        while (text.startsWith(" ")) text = text.slice(1);
      }
    }

    return exitPromise.then([KJ_MVCAP(result)]() mutable { return result.releaseAsArray(); });
  });
}

kj::Promise<VatPath::Reader> VagrantDriver::start(MachineId id, bool requireRestartProcess) {
  return subprocessSet.waitForSuccess({"vagrant", "up", kj::str(id)})
      .then([this,id,requireRestartProcess]() {
    kj::String name = kj::str(id);

    int fds[2];
    KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
    kj::AutoCloseFd writeEnd(fds[1]);
    auto input = ioProvider.wrapInputFd(fds[0],
        kj::LowLevelAsyncIoProvider::Flags::TAKE_OWNERSHIP |
        kj::LowLevelAsyncIoProvider::Flags::ALREADY_CLOEXEC);

    auto addr = kj::str(logSinkAddress, '/', name);
    kj::Vector<kj::StringPtr> args;
    args.addAll(kj::ArrayPtr<const kj::StringPtr>({
        "vagrant", "ssh", name, "--", "sudo", "/vagrant/bin/blackrock",
        "slave", "--log", addr, "if4:eth1"}));
    if (requireRestartProcess) args.add("-r");
    sandstorm::Subprocess::Options options(args.asPtr());
    options.stdout = writeEnd;
    auto exitPromise = subprocessSet.waitForSuccess(kj::mv(options));

    auto outputPromise = capnp::readMessage(*input);
    return outputPromise.then([this,id,KJ_MVCAP(exitPromise),KJ_MVCAP(input)](
                               kj::Own<capnp::MessageReader> reader) mutable {
      auto path = reader->getRoot<VatPath>();
      vatPaths[id] = kj::mv(reader);
      return exitPromise.then([path]() { return path; });
    });
  });
}

kj::Promise<void> VagrantDriver::stop(MachineId id) {
  return subprocessSet.waitForSuccess({"vagrant", "destroy", "-f", kj::str(id)});
}

} // namespace blackrock
