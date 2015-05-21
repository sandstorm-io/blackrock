// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "worker.h"
#include <sys/socket.h>
#include "nbd-bridge.h"
#include <sodium/randombytes.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <capnp/serialize-async.h>
#include <sandstorm/version.h>
#include <sys/mount.h>
#include <grp.h>
#include <sandstorm/spk.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>

namespace blackrock {

byte PackageMountSet::dummyByte = 0;

PackageMountSet::PackageMountSet(kj::AsyncIoContext& ioContext)
    : ioContext(ioContext), tasks(*this) {}
PackageMountSet::~PackageMountSet() noexcept(false) {
  KJ_ASSERT(mounts.empty(), "PackageMountSet destroyed while packages still mounted!") { break; }
}

auto PackageMountSet::getPackage(PackageInfo::Reader package)
    -> kj::Promise<kj::Own<PackageMount>> {
  kj::Own<PackageMount> packageMount;

  auto id = package.getId();
  auto iter = mounts.find(id);
  if (iter == mounts.end()) {
    // Create the NBD socketpair.
    int nbdSocketPair[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, nbdSocketPair));
    kj::AutoCloseFd nbdKernelEnd(nbdSocketPair[0]);
    auto nbdUserEnd = ioContext.lowLevelProvider->wrapSocketFd(nbdSocketPair[1],
        kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
        kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
        kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

    // Include some randomness in the directory name to make it harder for an attacker with the
    // ability to execute a named executable to find a useful one.
    uint64_t random;
    randombytes_buf(&random, sizeof(random));

    packageMount = kj::refcounted<PackageMount>(*this, id,
        kj::str("/var/blackrock/packages/", counter++, '-', kj::hex(random)),
        package.getVolume(), kj::mv(nbdUserEnd), kj::mv(nbdKernelEnd));
    mounts[packageMount->getId()] = packageMount.get();
  } else {
    packageMount = kj::addRef(*iter->second);
  }

  auto promise = packageMount->loaded.addBranch();
  return promise.then([KJ_MVCAP(packageMount)]() mutable {
    return kj::mv(packageMount);
  });
}

void PackageMountSet::returnPackage(kj::Own<PackageMount> package) {
  if (!package->isShared()) {
    // This is the last reference. Keep the package mounted for at least 5 minutes to avoid
    // purging the kernel cache if it is needed again.
    tasks.add(ioContext.lowLevelProvider->getTimer()
        .afterDelay(5 * kj::MINUTES).attach(kj::mv(package)));
  }
}

void PackageMountSet::taskFailed(kj::Exception&& exception) {
  KJ_LOG(ERROR, exception);
}

PackageMountSet::PackageMount::PackageMount(PackageMountSet& mountSet,
    kj::ArrayPtr<const kj::byte> id, kj::String path, Volume::Client volume,
    kj::Own<kj::AsyncIoStream> nbdUserEnd, kj::AutoCloseFd nbdKernelEnd)
    : mountSet(mountSet),
      id(kj::heapArray(id)),
      path(kj::heapString(path)),
      volumeAdapter(kj::heap<NbdVolumeAdapter>(kj::mv(nbdUserEnd), kj::mv(volume))),
      volumeRunTask(volumeAdapter->run().eagerlyEvaluate([](kj::Exception&& exception) {
        KJ_LOG(FATAL, "NbdVolumeAdapter failed (grain)", exception);
      })),
      nbdThread(mountSet.ioContext.provider->newPipeThread(
          [KJ_MVCAP(path), KJ_MVCAP(nbdKernelEnd)](
            kj::AsyncIoProvider& ioProvider,
            kj::AsyncIoStream& pipe,
            kj::WaitScope& waitScope) mutable {
        // Make sure mount point exists, and try to remove it later.
        sandstorm::recursivelyCreateParent(kj::str(path, "/foo"));
        KJ_DEFER(rmdir(path.cStr()));

        // Set up NBD.
        NbdDevice device;
        NbdBinding binding(device, kj::mv(nbdKernelEnd));

        {
          Mount mount(device.getPath(), path, MS_RDONLY, nullptr);

          // Signal setup is complete.
          pipe.write(&dummyByte, 1).wait(waitScope);

          // Wait for pipe disconnect.
          while (pipe.tryRead(&dummyByte, 1, 1).wait(waitScope) > 0) {}
        }

        // We've now unmounted, but wait a couple seconds before we disconnected the nbd device
        // because it's possible that a grain supervisor started *just before* the unmount could
        // still have a reference to the mount in its private mount namespace. The meta-supervisor
        // immediately unmounts these after creating its mount namespace, but there's a race
        // condition between its unshare() and its unmount().
        //
        // TODO(race): We really need a way to ask the kernel to tell us when the device is
        //   unmounted.
        ioProvider.getTimer().afterDelay(2 * kj::SECONDS).wait(waitScope);

        // We'll close our end of the pipe on the way out, thus signaling completion.
      })),
      loaded(nbdThread.pipe->read(&dummyByte, 1).fork()) {}

PackageMountSet::PackageMount::~PackageMount() noexcept(false) {
  // We don't want to block waiting for the NBD thread to complete. So, we carefully wait for
  // the thread to report completion in a detached promise.
  //
  // TODO(cleanup): Make kj::Thread::detach() work correctly.

  mountSet.mounts.erase(id);

  auto pipe = kj::mv(nbdThread.pipe);
  pipe->shutdownWrite();
  auto runTask = kj::mv(volumeRunTask);
  loaded.addBranch().then([KJ_MVCAP(pipe)]() mutable {
    // OK, we read the first byte. Wait for the pipe to become readable again, signalling EOF
    // meaning the thread is done.
    auto promise = pipe->tryRead(&dummyByte, 1, 1).then([](size_t n) {
      KJ_ASSERT(n == 0, "expected EOF");
    });
    return promise.attach(kj::mv(pipe));
  }, [](kj::Exception&& e) {
    // Apparently the thread exited without ever signaling that NBD setup succeeded.
    return kj::READY_NOW;
  }).then([KJ_MVCAP(runTask)]() mutable {
    return kj::mv(runTask);
  }).attach(kj::mv(nbdThread.thread), kj::mv(volumeAdapter))
    .detach([](kj::Exception&& exception) {
    KJ_LOG(ERROR, "Exception while trying to unmount package.", exception);
  });
}

// =======================================================================================

class WorkerImpl::SandstormCoreImpl: public sandstorm::SandstormCore::Server {
public:
  // TODO(someday): implement SandstormCore interface
};

class WorkerImpl::RunningGrain {
  // Encapsulates executing a grain on the worker.

public:
  RunningGrain(WorkerImpl& worker,
               Worker::Client workerCap,
               kj::Own<capnp::MessageBuilder>&& grainState,
               sandstorm::Assignable<GrainState>::Setter::Client&& grainStateSetter,
               kj::Own<kj::AsyncIoStream> nbdSocket,
               Volume::Client volume,
               kj::Own<kj::AsyncIoStream> capnpSocket,
               kj::Own<PackageMountSet::PackageMount> packageMount,
               sandstorm::Subprocess::Options&& subprocessOptions)
      : worker(worker),
        workerCap(kj::mv(workerCap)),
        grainState(kj::mv(grainState)),
        grainStateSetter(kj::mv(grainStateSetter)),
        packageMount(kj::mv(packageMount)),
        nbdVolume(kj::mv(nbdSocket), kj::mv(volume)),
        volumeRunTask(nbdVolume.run().eagerlyEvaluate([](kj::Exception&& exception) {
          KJ_LOG(FATAL, "NbdVolumeAdapter failed (grain)", exception);
        })),
        volumeDisconnectTask(nbdVolume.onDisconnected().then([this]() {
          // If the volume disconnected while the grain is still running, kill the grain.
          subprocess.signal(SIGTERM);
        }).eagerlyEvaluate(nullptr)),
        subprocess(kj::mv(subprocessOptions)),
        processWaitTask(worker.subprocessSet.waitForSuccess(subprocess)),
        capnpSocket(kj::mv(capnpSocket)),
        rpcClient(*this->capnpSocket, kj::heap<SandstormCoreImpl>()),
        persistentRegistration(worker.persistentRegistry, rpcClient.bootstrap()) {
  }

  ~RunningGrain() {
    auto newState = grainState->getRoot<GrainState>();

    // TODO(cleanup): Calling setInactive() doesn't actually remove the `active` pointer;
    //   it just sets the union discriminant to "inactive". Unfortunately the storage
    //   server just sees the pointers. Is this a bug in Cap'n Proto? It seems
    //   challenging to fix, except perhaps by introducing the native-mutable-objects
    //   idea.
    newState.disownActive();

    newState.setInactive();
    auto sizeHint = newState.totalSize();
    sizeHint.wordCount += 4;
    auto req = grainStateSetter.setRequest(sizeHint);
    req.setValue(newState);
    req.send().detach([](kj::Exception&& exception) {
      KJ_LOG(ERROR, "dirty grain shutdown", exception);
    });
    worker.packageMountSet.returnPackage(kj::mv(packageMount));
  }

  kj::Promise<void> onExit() {
    return processWaitTask.catch_([this](auto) { return kj::mv(volumeRunTask); });
  }

  sandstorm::Supervisor::Client getSupervisor() {
    return persistentRegistration.getWrapped().castAs<sandstorm::Supervisor>();
  }

private:
  WorkerImpl& worker;
  Worker::Client workerCap;
  kj::Own<capnp::MessageBuilder> grainState;
  sandstorm::Assignable<GrainState>::Setter::Client grainStateSetter;
  // TODO(cleanup): `grainState` holds a GrainState value in a MessageBuilder so that we can modify
  //   it over time and call grainStateSetter.set(). We really need to extend Cap'n Proto with
  //   mutable types.

  kj::Own<PackageMountSet::PackageMount> packageMount;
  NbdVolumeAdapter nbdVolume;
  kj::Promise<void> volumeRunTask;
  kj::Promise<void> volumeDisconnectTask;

  sandstorm::Subprocess subprocess;
  kj::Promise<void> processWaitTask;  // until onExit() is called.

  kj::Own<kj::AsyncIoStream> capnpSocket;
  capnp::TwoPartyClient rpcClient;
  // Cap'n Proto RPC connection to the grain's supervisor.

  LocalPersistentRegistry::Registration persistentRegistration;
};

WorkerImpl::WorkerImpl(kj::AsyncIoContext& ioContext, sandstorm::SubprocessSet& subprocessSet,
                       LocalPersistentRegistry& persistentRegistry)
    : ioProvider(*ioContext.lowLevelProvider), subprocessSet(subprocessSet),
      persistentRegistry(persistentRegistry), packageMountSet(ioContext), tasks(*this) {
  NbdDevice::loadKernelModule();
}
WorkerImpl::~WorkerImpl() noexcept(false) {}

struct WorkerImpl::CommandInfo {
  kj::Array<kj::String> commandArgs;
  kj::Array<kj::String> envArgs;

  CommandInfo(sandstorm::spk::Manifest::Command::Reader command) {
    auto argvReader = command.getArgv();
    auto argvBuilder = kj::heapArrayBuilder<kj::String>(
        argvReader.size() + command.hasDeprecatedExecutablePath());
    if (command.hasDeprecatedExecutablePath()) {
      argvBuilder.add(kj::heapString(command.getDeprecatedExecutablePath()));
    }
    for (auto arg: argvReader) {
      argvBuilder.add(kj::heapString(arg));
    }
    commandArgs = argvBuilder.finish();

    envArgs = KJ_MAP(v, command.getEnviron()) {
      return kj::str("-e", v.getKey(), '=', v.getValue());
    };
  }
};

kj::Promise<void> WorkerImpl::newGrain(NewGrainContext context) {
  auto params = context.getParams();
  auto storageFactory = params.getStorage();

  auto grainVolume = storageFactory.newVolumeRequest().send().getVolume();
  auto exclusiveVolume = grainVolume.getExclusiveRequest().send().getExclusive();

  auto supervisorPaf = kj::newPromiseAndFulfiller<sandstorm::Supervisor::Client>();

  auto grainStateHolder = kj::heap<capnp::MallocMessageBuilder>(8);

  auto req = storageFactory.newAssignableRequest<GrainState>();
  {
    auto grainStateValue = req.initInitialValue();
    grainStateValue.setActive(kj::mv(supervisorPaf.promise));
    grainStateValue.setVolume(kj::mv(grainVolume));
    grainStateHolder->setRoot(grainStateValue.asReader());
  }
  OwnedAssignable<GrainState>::Client grainState = req.send().getAssignable();
  auto setter = grainState.getRequest().send().getSetter();

  sandstorm::Supervisor::Client supervisor = bootGrain(params.getPackage(),
      kj::mv(grainStateHolder), kj::mv(setter),
      kj::mv(exclusiveVolume), params.getCommand(), true);

  supervisorPaf.fulfiller->fulfill(kj::cp(supervisor));

  auto results = context.getResults();
  results.setGrain(kj::mv(supervisor));
  results.setGrainState(kj::mv(grainState));

  // Promises are neat.
  return kj::READY_NOW;
}

kj::Promise<void> WorkerImpl::restoreGrain(RestoreGrainContext context) {
  auto params = context.getParams();

  auto grainState = params.getGrainState();
  auto grainStateHolder = kj::heap<capnp::MallocMessageBuilder>(
      grainState.totalSize().wordCount + 8);
  grainStateHolder->setRoot(grainState);

  auto mutableGrainState = grainStateHolder->getRoot<GrainState>();
  auto setter = params.getExclusiveGrainStateSetter();

  auto supervisor = bootGrain(
      params.getPackage(), kj::mv(grainStateHolder), setter,
      params.getExclusiveVolume(), params.getCommand(), false);

  mutableGrainState.setActive(supervisor);

  auto sizeHint = mutableGrainState.totalSize();
  sizeHint.wordCount += 4;
  auto req = setter.setRequest(sizeHint);
  req.setValue(mutableGrainState);

  context.getResults(capnp::MessageSize { 4, 1 }).setGrain(kj::mv(supervisor));

  return req.send().then([](auto) {});
}

sandstorm::Supervisor::Client WorkerImpl::bootGrain(
    PackageInfo::Reader packageInfo, kj::Own<capnp::MessageBuilder> grainState,
    sandstorm::Assignable<GrainState>::Setter::Client grainStateSetter, Volume::Client grainVolume,
    sandstorm::spk::Manifest::Command::Reader commandReader, bool isNew) {
  // Copy command info from params, since params will no longer be valid when we return.
  CommandInfo command(commandReader);

  // Make sure the package is mounted, then start the grain.
  return packageMountSet.getPackage(packageInfo)
      .then([this,isNew,KJ_MVCAP(grainState),KJ_MVCAP(grainStateSetter),
             KJ_MVCAP(command),KJ_MVCAP(grainVolume)]
            (auto&& packageMount) mutable {
    // Create the NBD socketpair. The Supervisor will actually mount the NBD device (in its own
    // mount namespace) but we'll implement it in the Worker.
    int nbdSocketPair[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, nbdSocketPair));
    kj::AutoCloseFd nbdKernelEnd(nbdSocketPair[0]);
    auto nbdUserEnd = ioProvider.wrapSocketFd(nbdSocketPair[1],
        kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
        kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
        kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

    // Create the Cap'n Proto socketpair, for Worker <-> Supervisor communication.
    int capnpSocketPair[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, capnpSocketPair));
    kj::AutoCloseFd capnpSupervisorEnd(capnpSocketPair[0]);
    auto capnpWorkerEnd = ioProvider.wrapSocketFd(capnpSocketPair[1],
        kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
        kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
        kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

    kj::String packageRoot = kj::str(packageMount->getPath(), "/spk");

    // Build array of StringPtr for argv.
    KJ_STACK_ARRAY(kj::StringPtr, argv,
        command.commandArgs.size() + command.envArgs.size() + 6 + isNew, 16, 16);
    {
      int i = 0;
      argv[i++] = "blackrock";
      argv[i++] = "grain";  // meta-supervisor
      if (isNew) {
        argv[i++] = "-n";
      }
      argv[i++] = packageMount->getPath();
      argv[i++] = "--";  // begin supervisor args
      for (auto& envArg: command.envArgs) {
        argv[i++] = envArg;
      }
      argv[i++] = "--";
      argv[i++] = packageRoot;
      for (auto& commandArg: command.commandArgs) {
        argv[i++] = commandArg;
      }
      KJ_ASSERT(i == argv.size());
    }

    // Build the subprocess options.
    sandstorm::Subprocess::Options options("/proc/self/exe");
    options.argv = argv;

    // Pass the capnp socket on FD 3 and the kernel end of the NBD socketpair as FD 4.
    int moreFds[2] = { capnpSupervisorEnd, nbdKernelEnd };
    options.moreFds = moreFds;

    // Make the RunningGrain.
    auto grain = kj::heap<RunningGrain>(
        *this, thisCap(), kj::mv(grainState), kj::mv(grainStateSetter), kj::mv(nbdUserEnd),
        kj::mv(grainVolume), kj::mv(capnpWorkerEnd), kj::mv(packageMount), kj::mv(options));

    auto supervisor = grain->getSupervisor();

    // Put it in the map so that it doesn't go away.
    auto grainPtr = grain.get();
    runningGrains[grainPtr] = kj::mv(grain);
    auto remover = kj::defer([this,grainPtr]() { runningGrains.erase(grainPtr); });
    tasks.add(grainPtr->onExit().attach(kj::mv(remover)));

    return supervisor;
  });
}

void WorkerImpl::taskFailed(kj::Exception&& exception) {
  KJ_LOG(ERROR, exception);
}

// -----------------------------------------------------------------------------

class WorkerImpl::PackageUploadStreamImpl: public Worker::PackageUploadStream::Server {
public:
  PackageUploadStreamImpl(
      Worker::Client workerCap,
      OwnedVolume::Client volume,
      kj::Own<kj::AsyncIoStream> nbdUserEnd,
      kj::Own<kj::AsyncOutputStream> stdinPipe,
      kj::Own<kj::AsyncInputStream> stdoutPipe,
      kj::Promise<void> subprocess)
      : workerCap(kj::mv(workerCap)),
        nbdVolume(kj::mv(nbdUserEnd), volume),
        volume(kj::mv(volume)),
        volumeRunTask(nbdVolume.run().eagerlyEvaluate([](kj::Exception&& exception) {
          KJ_LOG(FATAL, "NbdVolumeAdapter failed (package)", exception);
        })),
        stdinPipe(kj::mv(stdinPipe)),
        stdoutPipe(kj::mv(stdoutPipe)),
        subprocess(kj::mv(subprocess)) {
  }

protected:
  kj::Promise<void> write(WriteContext context) override {
    auto promise = stdinWriteQueue.then([this,context]() mutable {
      auto data = context.getParams().getData();
      return KJ_ASSERT_NONNULL(stdinPipe, "can't call write() after done()")
          ->write(data.begin(), data.size());
    }).fork();
    stdinWriteQueue = promise.addBranch();
    return promise.addBranch();
  }

  kj::Promise<void> done(DoneContext context) override {
    auto promise = stdinWriteQueue.then([this,context]() mutable {
      stdinPipe = nullptr;
    }).fork();
    stdinWriteQueue = promise.addBranch();
    return promise.addBranch();
  }

  kj::Promise<void> expectSize(ExpectSizeContext context) override {
    // We don't care.
    return kj::READY_NOW;
  }

  kj::Promise<void> getResult(GetResultContext context) override {
    KJ_REQUIRE(!calledGetResult);
    calledGetResult = true;
    context.releaseParams();

    return capnp::readMessage(*stdoutPipe)
        .then([this,context](kj::Own<capnp::MessageReader> message) mutable {
      auto inResults = message->getRoot<GetResultResults>();
      capnp::MessageSize size = inResults.totalSize();
      ++size.capCount;
      auto results = context.getResults(size);
      results.setAppId(inResults.getAppId());
      results.setManifest(inResults.getManifest());
      results.setVolume(kj::mv(volume));
      auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
      promises.add(kj::mv(volumeRunTask));
      promises.add(kj::mv(subprocess));
      return kj::joinPromises(promises.finish());
    });
  }

private:
  Worker::Client workerCap;
  NbdVolumeAdapter nbdVolume;
  OwnedVolume::Client volume;
  kj::Promise<void> volumeRunTask;
  kj::Maybe<kj::Own<kj::AsyncOutputStream>> stdinPipe;
  kj::Promise<void> stdinWriteQueue = kj::READY_NOW;
  kj::Own<kj::AsyncInputStream> stdoutPipe;
  kj::Promise<void> subprocess;
  bool calledGetResult = false;
};

kj::Promise<void> WorkerImpl::unpackPackage(UnpackPackageContext context) {
  // Create the NBD socketpair. The unpacker will actually mount the NBD device (in its own
  // mount namespace) but we'll implement it in the Worker.
  int nbdSocketPair[2];
  KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, nbdSocketPair));
  kj::AutoCloseFd nbdKernelEnd(nbdSocketPair[0]);
  auto nbdUserEnd = ioProvider.wrapSocketFd(nbdSocketPair[1],
      kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
      kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

  // Create pipes for stdin and stdout.
  int stdinFds[2];
  KJ_SYSCALL(pipe2(stdinFds, O_CLOEXEC));
  kj::AutoCloseFd stdin(stdinFds[0]);
  auto stdinPipe = ioProvider.wrapOutputFd(stdinFds[1],
      kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC);

  int stdoutFds[2];
  KJ_SYSCALL(pipe2(stdoutFds, O_CLOEXEC));
  kj::AutoCloseFd stdout(stdoutFds[1]);
  auto stdoutPipe = ioProvider.wrapInputFd(stdoutFds[0],
      kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
      kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC);

  // Create the subprocess.
  sandstorm::Subprocess::Options options("/proc/self/exe");
  kj::StringPtr argv[2];
  argv[0] = "blackrock";
  argv[1] = "unpack";
  options.argv = argv;
  options.stdin = stdin;
  options.stdout = stdout;
  int moreFds[1] = { nbdKernelEnd };
  options.moreFds = moreFds;

  auto promise = subprocessSet.waitForSuccess(kj::mv(options));

  auto volume = context.getParams().getStorage().newVolumeRequest().send().getVolume();

  context.getResults().setStream(kj::heap<PackageUploadStreamImpl>(
      thisCap(), kj::mv(volume), kj::mv(nbdUserEnd),
      kj::mv(stdinPipe), kj::mv(stdoutPipe), kj::mv(promise)));
  return kj::READY_NOW;
}

// =======================================================================================

class SupervisorMain::SystemConnectorImpl: public sandstorm::SupervisorMain::SystemConnector {
public:
  RunResult run(kj::AsyncIoContext& ioContext,
                sandstorm::Supervisor::Client mainCapability) const override {
    auto runner = kj::heap<Runner>(ioContext, kj::mv(mainCapability));

    capnp::MallocMessageBuilder message(8);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(capnp::rpc::twoparty::Side::SERVER);
    auto core = runner->rpcSystem.bootstrap(vatId).castAs<sandstorm::SandstormCore>();

    auto promise = runner->network.onDisconnect();

    return { promise.attach(kj::mv(runner)), kj::mv(core) };
  }

  void checkIfAlreadyRunning() const override {
    // Not relevant for Blackrock.
  }

  kj::Maybe<int> getSaveFd() const override {
    return 3;
  }

private:
  struct Runner {
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;

    Runner(kj::AsyncIoContext& ioContext, sandstorm::Supervisor::Client mainCapability)
        : stream(ioContext.lowLevelProvider->wrapSocketFd(3,
              kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
              kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
              kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK)),
          network(*stream, capnp::rpc::twoparty::Side::CLIENT),
          rpcSystem(network, kj::mv(mainCapability)) {}
  };
};

SupervisorMain::SupervisorMain(kj::ProcessContext& context)
    : context(context),
      sandstormSupervisor(context) {}

kj::MainFunc SupervisorMain::getMain() {
  return kj::MainBuilder(context, "Blackrock version " SANDSTORM_VERSION,
                         "Runs a Blackrock grain supervisor for an instance of the package found "
                         "at path <package>. <command> is executed inside the sandbox to start "
                         "the grain. The caller must provide a Cap'n Proto towparty socket on "
                         "FD 3 which is used to talk to the grain supervisor.\n"
                         "\n"
                         "NOT FOR HUMAN CONSUMPTION: This should only be executed by "
                         "`blackrock grain`.")
      .addOption({'n', "new"}, [this]() { sandstormSupervisor.setIsNew(true); return true; },
                 "Initializes a new grain. (Otherwise, runs an existing one.)")
      .addOptionWithArg({'e', "env"}, KJ_BIND_METHOD(sandstormSupervisor, addEnv), "<name>=<val>",
                        "Set the environment variable <name> to <val> inside the sandbox.  Note "
                        "that *no* environment variables are set by default.")
      .expectArg("<package>", KJ_BIND_METHOD(sandstormSupervisor, setPkg))
      .expectOneOrMoreArgs("<command>", KJ_BIND_METHOD(sandstormSupervisor, addCommandArg))
      .callAfterParsing(KJ_BIND_METHOD(*this, run))
      .build();
}

kj::MainBuilder::Validity SupervisorMain::run() {
  // Rename the task to allow for orderly teardown when killing all blackrock processes.
  KJ_SYSCALL(prctl(PR_SET_NAME, "blackrock-sup", 0, 0, 0));

  // Set CLOEXEC on the Cap'n Proto socket so that the app doesn't see it.
  KJ_SYSCALL(fcntl(3, F_SETFD, FD_CLOEXEC));

  sandstormSupervisor.setAppName("appname-not-applicable");
  sandstormSupervisor.setGrainId("grainid-not-applicable");

  SystemConnectorImpl connector;
  sandstormSupervisor.setSystemConnector(connector);

  sandstormSupervisor.setVar("/mnt/grain");

  return sandstormSupervisor.run();
}

// -----------------------------------------------------------------------------

MetaSupervisorMain::MetaSupervisorMain(kj::ProcessContext& context)
    : context(context) {}

kj::MainFunc MetaSupervisorMain::getMain() {
  return kj::MainBuilder(context, "Blackrock version " SANDSTORM_VERSION,
                         "Runs 'blackrock supervise' passing it <args>. Forwards the -n option "
                         "if given. The caller must provide a Cap'n Proto towparty socket on "
                         "FD 3 which is used to talk to the grain supervisor, and FD 4 must be "
                         "a socket implementing the NBD protocol exporting the grain's mutable "
                         "storage.\n"
                         "\n"
                         "NOT FOR HUMAN CONSUMPTION: Given the FD requirements, you obviously "
                         "can't run this directly from the command-line. It is intended to be "
                         "invoked by the Blackrock worker.")
      .addOption({'n', "new"}, [this]() { isNew = true; args.add("-n"); return true; },
                 "Initializes a new grain. (Otherwise, runs an existing one.)")
      .expectArg("<pkg>", [this](kj::StringPtr s) { packageMount = s; return true; })
      .expectZeroOrMoreArgs("<args>", [this](kj::StringPtr s) { args.add(s); return true; })
      .callAfterParsing(KJ_BIND_METHOD(*this, run))
      .build();
}

kj::MainBuilder::Validity MetaSupervisorMain::run() {
  // Rename the task to allow for orderly teardown when killing all blackrock processes.
  KJ_SYSCALL(prctl(PR_SET_NAME, "blackrock-msup", 0, 0, 0));

  // Make sure any descendents that die are re-parented to us. This way we can make sure all
  // children are reaped before we attempt to unmount, which is important to ensure that all
  // clones of the mount are gone before we disconnect nbd.
  KJ_SYSCALL(prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0));

  // Set CLOEXEC on the the nbd fd so that the supervisor doesn't see it.
  KJ_SYSCALL(fcntl(4, F_SETFD, FD_CLOEXEC));

  // Enter mount namespace, to mount the grain.
  KJ_SYSCALL(unshare(CLONE_NEWNS));

  // Unmount all packages other than our own, so that we don't hold them open in our mount
  // namespace.
  //
  // TODO(race): One of these packages could expire and be disconnected by the worker between
  //   our unshare() and our unmount(), in which case the kernel could freak out because we're
  //   unmounting something not connected. To help avoid this, when the worker cleans up a package,
  //   it sleeps for a couple seconds after unmount. It may also not be a big deal because the
  //   package is mounted read-only and the only observed kernel issues with disconnecting before
  //   unmount had to do with flushing write buffers.
  bool sawSelf = false;
  for (auto& file: sandstorm::listDirectory("/var/blackrock/packages")) {
    auto path = kj::str("/var/blackrock/packages/", file);
    if (path == packageMount) {
      sawSelf = true;
    } else {
      KJ_SYSCALL(umount(path.cStr()));
    }
  }
  KJ_REQUIRE(sawSelf, "package mount not seen in packages dir", packageMount);

  // We'll mount our grain data on /mnt because it's our own mount namespace so why not?
  NbdDevice device;
  NbdBinding binding(device, kj::AutoCloseFd(4));

  if (isNew) {
    // Experimentally, it seems 256M is a sweet spot that generates a minimal number of non-zero
    // blocks. We can extend the filesystem later if desired.
    device.format(256);
  }

  Mount mount(device.getPath(), "/mnt", 0, nullptr);
  KJ_SYSCALL(chown("/mnt", 1000, 1000));

  // Mask SIGCHLD and SIGTERM so that we can handle them later.
  sigset_t sigmask;
  sigemptyset(&sigmask);
  sigaddset(&sigmask, SIGCHLD);
  sigaddset(&sigmask, SIGTERM);
  KJ_SYSCALL(sigprocmask(SIG_SETMASK, &sigmask, nullptr));

  // In order to ensure we get a chance to clean up after the supervisor exits, we run it in a
  // fork.
  sandstorm::Subprocess child([this]() -> int {
    // Change signal mask back to empty.
    sigset_t emptymask;
    sigemptyset(&emptymask);
    KJ_SYSCALL(sigprocmask(SIG_SETMASK, &emptymask, nullptr));

    // Unfortunately, we are still root here. Drop privs.
    KJ_SYSCALL(setgroups(0, nullptr));
    KJ_SYSCALL(setresgid(1000, 1000, 1000));
    KJ_SYSCALL(setresuid(1000, 1000, 1000));

    // More unfortunately, the ownership of /proc/self/uid_map and similar special files are set
    // on exec() and do not change until the next exec() (fork()s inherit the current permissions
    // of the parent's file). Therefore, now that we've changed our uid, we *must* exec() before
    // we can set up a sandbox. Otherwise, we'd just call sandstorm::SupervisorMain directly here.

    const char* subArgs[args.size() + 3];
    uint i = 0;
    subArgs[i++] = "blackrock";
    subArgs[i++] = "supervise";
    for (auto& arg: args) {
      subArgs[i++] = arg.cStr();
    }
    subArgs[i++] = nullptr;

    auto subArgv = const_cast<char* const*>(subArgs);  // execv() is not const-correct. :(
    KJ_SYSCALL(execv("/proc/self/exe", subArgv));
    KJ_UNREACHABLE;
  });

  // FD 3 is used by the child. Close our handle.
  KJ_SYSCALL(close(3));

  // Wait for signal indicating child exit.
  for (;;) {
    siginfo_t siginfo;
    KJ_SYSCALL(sigwaitinfo(&sigmask, &siginfo));
    if (siginfo.si_signo == SIGTERM) {
      // We might get a SIGTERM if the worker wants this grain to shut down. Forward it to the
      // supervisor process.
      child.signal(SIGTERM);
    } else {
      KJ_ASSERT(siginfo.si_signo == SIGCHLD);
      break;
    }
  }

  child.waitForSuccess();

  // Wait for any children that were reparented to us when the supervisor died. The supervisor
  // should have already sent them SIGKILL but we need to make sure they've actually died before
  // unmounting because they may hold a reference to the mount in their mount namespace, and we
  // need to make sure the mount really is unmounted before we disconnect nbd.
  for (;;) {
    int status;
    if (wait(&status) < 0) {
      int error = errno;
      if (error == ECHILD) {
        // No more children.
        break;
      } else if (error != EINTR) {
        KJ_FAIL_SYSCALL("wait()", error);
      }
    }
  }

  return true;
}

// =======================================================================================

kj::MainFunc UnpackMain::getMain() {
  return kj::MainBuilder(context, "Blackrock version " SANDSTORM_VERSION,
                         "Runs `spk unpack`, reading the SPK file from stdin. Additionally, "
                         "FD 3 is expected to be an NBD socket, which will be mounted and "
                         "the package contents unpacked into it.\n"
                         "\n"
                         "NOT FOR HUMAN CONSUMPTION: Given the FD requirements, you obviously "
                         "can't run this directly from the command-line. It is intended to be "
                         "invoked by the Blackrock worker.")
      .callAfterParsing(KJ_BIND_METHOD(*this, run))
      .build();
}

static void seteugidNoHang(uid_t uid, gid_t gid) {
  // Apparently, glibc's setuid() and setgid() are complicated wrappers which attempt to interrupt
  // every thread in the process in order to set their IDs. But guess what? Our NBD thread is
  // NOT INTERRUPTIBLE. So... hang. But we don't actually want to change that thread's IDs anyway.

  KJ_SYSCALL(syscall(SYS_setresgid, -1, gid, -1)) { break; }
  KJ_SYSCALL(syscall(SYS_setresuid, -1, uid, -1)) { break; }
}

kj::MainBuilder::Validity UnpackMain::run() {
  // Enter mount namespace!
  KJ_SYSCALL(unshare(CLONE_NEWNS));

  // We'll mount our package on /mnt because it's our own mount namespace so why not?
  NbdDevice device;
  NbdBinding binding(device, kj::AutoCloseFd(3));
  device.format(2048);  // TODO(soon): Set max filesystem size based on uncompressed package size.
  Mount mount(device.getPath(), "/mnt", 0, nullptr);
  KJ_SYSCALL(mkdir("/mnt/spk", 0755));

  // We unpack packages with uid 1/gid 1: IDs that are not zero, but are also not used by apps.
  KJ_SYSCALL(chown("/mnt/spk", 1, 1));
  seteugidNoHang(1, 1);
  KJ_DEFER(seteugidNoHang(0, 0));

  kj::String appId = sandstorm::unpackSpk(STDIN_FILENO, "/mnt/spk", "/tmp");

  // Read manifest.
  capnp::StreamFdMessageReader reader(sandstorm::raiiOpen(
      "/mnt/spk/sandstorm-manifest", O_RDONLY | O_CLOEXEC));

  // Write result to stdout.
  capnp::MallocMessageBuilder message;
  auto root = message.getRoot<Worker::PackageUploadStream::GetResultResults>();
  root.setAppId(appId);
  root.setManifest(reader.getRoot<sandstorm::spk::Manifest>());
  capnp::writeMessageToFd(STDOUT_FILENO, message);

  return true;
}

}  // namespace blackrock
