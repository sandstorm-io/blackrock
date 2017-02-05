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
#include "backend-set.h"

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

// TODO(cleanup): Here we have a little hack so that registrationArray(...) returns an array built
//   from its parameters. Unfortunately std::initializer_list doesn't work for us because of its
//   constness. But, we should perhaps put this utility code into KJ.
typedef kj::Array<kj::Own<BackendSetFeederBase::Registration>> RegistrationArray;
RegistrationArray addEach(kj::ArrayBuilder<kj::Own<BackendSetFeederBase::Registration>>& builder) {
  return builder.finish();
}
template <typename First, typename... Rest>
RegistrationArray addEach(kj::ArrayBuilder<kj::Own<BackendSetFeederBase::Registration>>& builder,
                          First&& first, Rest&&... rest) {
  builder.add(kj::fwd<First>(first));
  return addEach(builder, kj::fwd<Rest>(rest)...);
}
template <typename... Params>
RegistrationArray registrationArray(Params&&... params) {
  auto builder = kj::heapArrayBuilder<kj::Own<BackendSetFeederBase::Registration>>(
      sizeof...(params));
  return addEach(builder, kj::fwd<Params>(params)...);
}

class MachineHarness {
  // Runs one machine, booting it and automatically restarting it as needed. A callback is provided
  // which is called each time a connection to the machine is established in order to add it to
  // the right load-balacing sets.

public:
  MachineHarness(kj::Timer& timer, capnp::RpcSystem<VatPath>& rpcSystem, VatId::Reader self,
                 ComputeDriver& driver, ComputeDriver::MachineId id,
                 bool alreadyBooted, bool requireRestartProcess,
                 kj::Function<RegistrationArray(Machine::Client)> setup)
      : timer(timer), rpcSystem(rpcSystem), self(self), driver(driver), id(id),
        setup(kj::mv(setup)), booted(alreadyBooted),
        runTask(run(requireRestartProcess ? RESTART : RECONNECT)
            .eagerlyEvaluate([](kj::Exception&& exception) {
          // Shouldn't happen! Don't let cluster end up in broken state.
          KJ_LOG(FATAL, "MachineHarness run task failed", exception);
          abort();
        })) {}

private:
  kj::Timer& timer;
  capnp::RpcSystem<VatPath>& rpcSystem;
  VatId::Reader self;
  ComputeDriver& driver;
  ComputeDriver::MachineId id;
  kj::Function<RegistrationArray(Machine::Client)> setup;
  bool booted;
  kj::Promise<void> runTask;

  enum RetryStage {
    RECONNECT,  // Reconnect to already-running process (or restart it if it died).
    RESTART,    // Kill running process and restart.
    REBOOT      // Reboot machine.
  };

  kj::Promise<void> run(RetryStage retryStage) {
    if (booted) {
      // Already booted. Should we reboot?
      if (retryStage == REBOOT) {
        return driver.stop(id).then([this]() {
          booted = false;
          // Since we're not booted, the stage we pass to run() here is irrelevant.
          return run(RECONNECT);
        });
      }
    } else {
      return driver.boot(id).then([this]() {
        booted = true;
        // Since we just booted, RECONNECT vs. RESTART are equivalent.
        return run(RECONNECT);
      });
    }

    return driver.run(id, self, retryStage == RESTART)
        .then([this,retryStage](VatPath::Reader path) {
      auto machine = rpcSystem.bootstrap(path).castAs<Machine>();

      // Try to send a ping, giving up after 60 seconds.
      auto initialPing = machine.pingRequest().send().then([](auto&&) {});
      return timer.timeoutAfter(60 * kj::SECONDS, kj::mv(initialPing))
          .then([this,KJ_MVCAP(machine)]() mutable {
        // Successfully pinged. The machine is up.

        // Call the setup function.
        auto registrations = setup(machine);

        // Arrange a hanging ping and periodic quick pings to detect machine failure.
        auto req = machine.pingRequest();
        req.setHang(true);
        return req.send().then([](auto&&) {})
            .exclusiveJoin(pingLoop(machine))
            .attach(kj::mv(registrations))
            .then([this]() {
          KJ_LOG(ERROR, "monitoring for machine returned without error? reconnecting", id);
        }, [this](kj::Exception&& exception) {
          KJ_LOG(ERROR, "lost connection to machine; reconnecting", id, exception);
        }).then([this]() {
          return run(RECONNECT);
        });
      }, [this,retryStage](kj::Exception&& exception) {
        // If we only tried to reconnect, now try to restart the process, otherwise try to reboot
        // the machine. For worker machines, we skip straight to reboot as restarting can be
        // dirty.
        if (retryStage == RECONNECT && id.type != ComputeDriver::MachineType::WORKER) {
          KJ_LOG(ERROR, "initial ping to machine failed; restarting process", id, exception);
          return run(RESTART);
        } else {
          KJ_LOG(ERROR, "initial ping to machine failed; rebooting machine", id, exception);
          return run(REBOOT);
        }
      });
    }, [this](kj::Exception&& exception) {
      // run() failed.
      KJ_LOG(ERROR, "couldn't connect to machine; rebooting it", id, exception);
      return run(REBOOT);
    });
  }

  kj::Promise<void> pingLoop(Machine::Client machine) {
    return timer.afterDelay(60 * kj::SECONDS)
        .then([this,KJ_MVCAP(machine)]() mutable {
      auto ping = machine.pingRequest().send().then([](auto&&) {});
      return timer.timeoutAfter(60 * kj::SECONDS, kj::mv(ping))
          .then([this,KJ_MVCAP(machine)]() mutable {
        return pingLoop(kj::mv(machine));
      });
    });
  }
};

}  // namespace

void runMaster(kj::AsyncIoContext& ioContext, ComputeDriver& driver, MasterConfig::Reader config,
               bool shouldRestart, kj::ArrayPtr<kj::StringPtr> machinesToRestart) {
  KJ_REQUIRE(config.getWorkerCount() > 0, "need at least one worker");

  std::set<ComputeDriver::MachineId> restartSet;
  for (auto& m: machinesToRestart) {
    restartSet.insert(m);
  }

  VatNetwork network(ioContext.provider->getNetwork(), ioContext.provider->getTimer(),
                     driver.getMasterBindAddress());
  auto rpcSystem = capnp::makeRpcClient(network);

  kj::Vector<kj::Own<MachineHarness>> harnesses;
  ErrorLogger logger;
  kj::TaskSet tasks(logger);

  uint storageCount = 1;
  uint workerCount = config.getWorkerCount();
  uint frontendCount = config.getFrontendCount();
  uint mongoCount = 1;
  uint coordinatorCount = 0;
  uint gatewayCount = 0;

  // Storage feeders.
  BackendSetFeeder<StorageSibling> storageSiblingFeeder(storageCount);
  BackendSetFeeder<Restorer<SturdyRef::Hosted>> hostedRestorerForStorageFeeder(coordinatorCount);
  BackendSetFeeder<Restorer<SturdyRef::External>> gatewayRestorerForStorageFeeder(gatewayCount);

  // Frontend feeders.
  BackendSetFeeder<Restorer<SturdyRef::Stored>> storageRestorerForFrontendFeeder(storageCount);
  BackendSetFeeder<Restorer<SturdyRef::Hosted>> hostedRestorerForFrontendFeeder(coordinatorCount);

  // Non-specific feeders.
  BackendSetFeeder<Worker> workerFeeder(workerCount);
  BackendSetFeeder<StorageRootSet> storageRootFeeder(storageCount);
  BackendSetFeeder<StorageFactory> storageFactoryFeeder(storageCount);
  BackendSetFeeder<Frontend> frontendFeeder(frontendCount);
  BackendSetFeeder<Mongo> mongoFeeder(mongoCount);

  std::map<ComputeDriver::MachineType, uint> expectedCounts;
  expectedCounts[ComputeDriver::MachineType::STORAGE] = storageCount;
  expectedCounts[ComputeDriver::MachineType::WORKER] = workerCount;
  expectedCounts[ComputeDriver::MachineType::FRONTEND] = frontendCount;
  expectedCounts[ComputeDriver::MachineType::MONGO] = mongoCount;

  VatPath::Reader storagePath;
  auto workerPaths = kj::heapArray<VatPath::Reader>(config.getWorkerCount());
  auto frontendPaths = kj::heapArray<VatPath::Reader>(config.getFrontendCount());
  VatPath::Reader mongoPath;

  KJ_LOG(INFO, "examining currently-running machines...");

  // Shut down any machines that we don't need anymore and record which others are started.
  std::set<ComputeDriver::MachineId> alreadyRunning;
  for (auto& machine: driver.listMachines().wait(ioContext.waitScope)) {
    if (machine.index >= expectedCounts[machine.type]) {
      KJ_LOG(INFO, "STOPPING", machine);
      tasks.add(driver.stop(machine));
    } else {
      alreadyRunning.insert(machine);
    }
  }

  auto start = [&](ComputeDriver::MachineId id,
                   kj::Function<RegistrationArray(Machine::Client)> setup) {
    bool shouldRestartNode = shouldRestart || restartSet.count(id) > 0;
    if (shouldRestartNode) {
      KJ_LOG(INFO, "RESTARTING", id);
    } else {
      KJ_LOG(INFO, "STARTING", id);
    }

    harnesses.add(kj::heap<MachineHarness>(
        ioContext.provider->getTimer(), rpcSystem, network.getSelf().getId(),
        driver, id, alreadyRunning.count(id) > 0, shouldRestartNode,
        kj::mv(setup)));
  };

  // Start storage.
  start({ ComputeDriver::MachineType::STORAGE, 0 }, [&](Machine::Client&& machine) {
    auto storage = machine.becomeStorageRequest().send();

    return registrationArray(
        storageSiblingFeeder.addBackend(storage.getSibling()),
        storageRootFeeder.addBackend(storage.getRootSet()),
        ({
          auto req = storage.getStorageRestorer().getForOwnerRequest();
          req.initDomain().setFrontend();
          storageRestorerForFrontendFeeder.addBackend(req.send().getAttenuated());
        }),
        storageFactoryFeeder.addBackend(storage.getStorageFactory()),
        storageSiblingFeeder.addConsumer(storage.getSiblingSet()),
        hostedRestorerForStorageFeeder.addConsumer(storage.getHostedRestorerSet()),
        gatewayRestorerForStorageFeeder.addConsumer(storage.getGatewayRestorerSet()));
  });

  // Start workers.
  for (uint i = 0; i < workerCount; i++) {
    start({ ComputeDriver::MachineType::WORKER, i }, [&](Machine::Client&& machine) {
      return registrationArray(
          workerFeeder.addBackend(machine.becomeWorkerRequest().send().getWorker()));
    });
  }

  // Start front-end.
  for (uint i = 0; i < frontendCount; i++) {
    start({ ComputeDriver::MachineType::FRONTEND, i }, [&,i](Machine::Client&& machine) {
      auto frontend = ({
        auto req = machine.becomeFrontendRequest();
        req.setConfig(config.getFrontendConfig());
        req.setReplicaNumber(i);
        req.send();
      });

      return registrationArray(
          frontendFeeder.addBackend(frontend.getFrontend()),
          storageRestorerForFrontendFeeder.addConsumer(frontend.getStorageRestorerSet()),
          storageRootFeeder.addConsumer(frontend.getStorageRootSet()),
          storageFactoryFeeder.addConsumer(frontend.getStorageFactorySet()),
          hostedRestorerForFrontendFeeder.addConsumer(frontend.getHostedRestorerSet()),
          workerFeeder.addConsumer(frontend.getWorkerSet()),
          mongoFeeder.addConsumer(frontend.getMongoSet()));
    });
  }

  // Start mongo.
  start({ ComputeDriver::MachineType::MONGO, 0 }, [&](Machine::Client&& machine) {
    return registrationArray(
        mongoFeeder.addBackend(machine.becomeMongoRequest().send().getMongo()));
  });

  // Loop forever handling messages.
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
  else HANDLE_CASE(MONGO, "mongo")
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
    case MachineType::MONGO      : typeName = "mongo"      ; break;
  }

  return kj::str(typeName, index);
}

// =======================================================================================

VagrantDriver::VagrantDriver(sandstorm::SubprocessSet& subprocessSet,
                             kj::LowLevelAsyncIoProvider& ioProvider)
    : subprocessSet(subprocessSet), ioProvider(ioProvider),
      masterBindAddress(SimpleAddress::getInterfaceAddress(AF_INET, "vboxnet0")),
      logTask(nullptr), logSinkAddress(masterBindAddress) {
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
  // output is simply empty. So... we parse. Poorly.
  auto outputPromise = readAllAsync(*input);
  return outputPromise.attach(kj::mv(input))
      .then([KJ_MVCAP(exitPromise)](kj::String allText) mutable {
    kj::Vector<MachineId> result;

    kj::StringPtr text = allText;

    text = text.slice(KJ_ASSERT_NONNULL(text.findFirst('\n')) + 1);
    KJ_ASSERT(text.startsWith("----------------"));
    text = text.slice(KJ_ASSERT_NONNULL(text.findFirst('\n')) + 1);

    // If there are no VMs running, Vagrant says "There are no active blah blah blah",
    // unfortunately *without* a leading blank line.
    if (!text.startsWith("There are no active")) {
      char* cwdp = get_current_dir_name();
      KJ_DEFER(free(cwdp));
      kj::StringPtr cwd = cwdp;

      // Parse lines until we see an empty line.
      while (!text.startsWith("\n") && !text.startsWith("\r\n")) {
        // Parse line in format: id name provider state directory

        uint eol = KJ_ASSERT_NONNULL(text.findFirst('\n'));
        auto row = text.slice(0, eol);
        text = text.slice(eol + 1);
        while (text.startsWith(" ")) text = text.slice(1);

        auto cols = sandstorm::splitSpace(row);
        KJ_ASSERT(cols.size() == 5, "couldn't parse vagrant global-status row", row);

        // Ignore VMs from other Vagrantfiles by checking the directory.
        if (kj::str(cols[4]) == cwd) {
          result.add(MachineId(kj::str(cols[1])));
        }
      }
    }

    return exitPromise.then([KJ_MVCAP(result)]() mutable { return result.releaseAsArray(); });
  });
}

kj::Promise<void> VagrantDriver::boot(MachineId id) {
  // Vagrant (or maybe VirtualBox) is incredibly bad about booting multiple machines in parallel.
  // It deadlocks frequently, or throws weird errors. So we force serialization. :(

  auto fork = bootQueue.then([this,id]() {
    return subprocessSet.waitForSuccess({"vagrant", "up", kj::str(id)});
  }).fork();

  bootQueue = fork.addBranch();
  return fork.addBranch();
}

kj::Promise<VatPath::Reader> VagrantDriver::run(
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
      "vagrant", "ssh", name, "--", "sudo", "/blackrock/bin/blackrock",
      "slave", "--log", addr, "if4:eth1"}));
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

kj::Promise<void> VagrantDriver::stop(MachineId id) {
  return subprocessSet.waitForSuccess({"vagrant", "destroy", "-f", kj::str(id)});
}

} // namespace blackrock
