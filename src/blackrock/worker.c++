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
#include <grp.h>
#include <sandstorm/spk.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <sandstorm/backup.h>
#include "bundle.h"

#include <sys/mount.h>
#undef BLOCK_SIZE // grr, mount.h

namespace blackrock {

namespace {

void unshareMountNamespace() {
  KJ_SYSCALL(unshare(CLONE_NEWNS));

  // To really unshare the mount namespace, we also have to make sure all mounts are private.
  // The parameters here were derived by strace'ing `mount --make-rprivate /`.  AFAICT the flags
  // are undocumented.  :(
  KJ_SYSCALL(mount("none", "/", nullptr, MS_REC | MS_PRIVATE, nullptr));
}

class BlobDownloadStreamImpl: public sandstorm::ByteStream::Server {
public:
  BlobDownloadStreamImpl(kj::AutoCloseFd fd): target(kj::mv(fd)) {}

  void requireDone() {
    KJ_REQUIRE(isDone, "done() not called on ByteStream");
  }

  kj::Promise<void> write(WriteContext context) override {
    KJ_REQUIRE(!isDone);

    auto data = context.getParams().getData();
    target.write(data.begin(), data.size());

    return kj::READY_NOW;
  }

  kj::Promise<void> done(DoneContext context) override {
    isDone = true;
    return kj::READY_NOW;
  }

private:
  kj::FdOutputStream target;
  bool isDone = false;
};

kj::Promise<void> uploadBlob(kj::AutoCloseFd fd, sandstorm::ByteStream::Client stream) {
  auto req = stream.writeRequest(capnp::MessageSize {2052, 0});
  auto orphanage = capnp::Orphanage::getForMessageContaining(
      kj::implicitCast<sandstorm::ByteStream::WriteParams::Builder>(req));

  auto orphan = orphanage.newOrphan<capnp::Data>(8192);
  auto buffer = orphan.get();
  ssize_t n;
  KJ_SYSCALL(n = read(fd, buffer.begin(), buffer.size()));
  if (n < buffer.size()) {
    if (n == 0) {
      return stream.doneRequest().send().then([](auto&&) {});
    }
    orphan.truncate(n);
  }

  req.adoptData(kj::mv(orphan));

  return req.send().then([KJ_MVCAP(fd),KJ_MVCAP(stream)](auto&&) mutable {
    return uploadBlob(kj::mv(fd), kj::mv(stream));
  });
}

class TemporaryFile {
  // Creates a temporary file with an on-disk path, then deletes it in the destructor.

public:
  TemporaryFile() {
    char name[] = "/var/tmp/blackrock-temp.XXXXXX";
    int fd_;
    KJ_SYSCALL(fd_ = mkostemp(name, O_CLOEXEC));
    fd = kj::AutoCloseFd(fd_);
    filename = kj::heapString(name);
  }

  ~TemporaryFile() noexcept(false) {
    KJ_SYSCALL(unlink(filename.cStr())) { break; }
  }

  kj::AutoCloseFd releaseFd() {
    return kj::mv(fd);
  }

  kj::StringPtr getFilename() {
    return filename;
  }

private:
  kj::String filename;
  kj::AutoCloseFd fd;
};

struct AsyncOutSyncInPipe {
  kj::AutoCloseFd readEnd;
  kj::Own<kj::AsyncOutputStream> writeEnd;

  static AsyncOutSyncInPipe make(kj::LowLevelAsyncIoProvider& ioProvider) {
    int fds[2];
    KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
    return {
      kj::AutoCloseFd(fds[0]),
      ioProvider.wrapOutputFd(fds[1],
          kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
          kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC)
    };
  }
};

struct AsyncInSyncOutPipe {
  kj::Own<kj::AsyncInputStream> readEnd;
  kj::AutoCloseFd writeEnd;

  static AsyncInSyncOutPipe make(kj::LowLevelAsyncIoProvider& ioProvider) {
    int fds[2];
    KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
    return {
      ioProvider.wrapInputFd(fds[0],
          kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
          kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC),
      kj::AutoCloseFd(fds[1])
    };
  }
};

struct NbdSocketPair {
  kj::Own<kj::AsyncIoStream> userEnd;
  kj::AutoCloseFd kernelEnd;

  static NbdSocketPair make(kj::LowLevelAsyncIoProvider& ioProvider) {
    int fds[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, fds));
    return {
      ioProvider.wrapSocketFd(fds[0],
          kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
          kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
          kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK),
      kj::AutoCloseFd(fds[1])
    };
  }
};

class CowVolume: public Volume::Server {
  // Wraps a read-only Volume adding an in-memory copy-on-write overlay so that the volume can
  // be written.
  //
  // The main use case for this is to allow a dirty read-only ext4 FS to be mounted; the ext4
  // driver will play back the journal resulting in writes to the overlay.

public:
  CowVolume(Volume::Client inner): inner(kj::mv(inner)) {}

protected:
  kj::Promise<void> read(ReadContext context) override {
    auto params = context.getParams();
    uint32_t start = params.getBlockNum();
    uint32_t count = params.getCount();
    context.releaseParams();

    // TODO(perf): Currently we issue the whole read to the backend and then replace the blocks
    //   that have been overwritten locally. In theory we should not request blocks from the
    //   backend that we're going to overwrite anyway, but in practice we probably won't get any
    //   such requests since the kernel should already be caching any data it recently wrote.

    auto req = inner.readRequest();
    req.setBlockNum(start);
    req.setCount(count);
    return req.send().then([this,start,count,context](auto&& results) mutable {
      context.setResults(results);
      auto data = context.getResults().getData();
      KJ_ASSERT(data.size() == count * Volume::BLOCK_SIZE);
      for (uint32_t i = 0; i < count; i++) {
        auto iter = overlay.find(start + i);
        if (iter != overlay.end()) {
          if (iter->second == nullptr) {
            memset(data.begin() + i * Volume::BLOCK_SIZE, 0, Volume::BLOCK_SIZE);
          } else {
            memcpy(data.begin() + i * Volume::BLOCK_SIZE, iter->second.begin(), Volume::BLOCK_SIZE);
          }
        }
      }
    });
  }

  kj::Promise<void> write(WriteContext context) override {
    auto params = context.getParams();
    uint32_t start = params.getBlockNum();
    auto data = params.getData();

    uint32_t count = data.size() / Volume::BLOCK_SIZE;
    KJ_ASSERT(data.size() % Volume::BLOCK_SIZE == 0);

    for (uint32_t i = 0; i < count; i++) {
      auto& slot = overlay[start + i];
      if (slot == nullptr) {
        slot = kj::heapArray<byte>(Volume::BLOCK_SIZE);
      }
      memcpy(slot.begin(), data.begin() + i * Volume::BLOCK_SIZE, Volume::BLOCK_SIZE);
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> zero(ZeroContext context) override {
    auto params = context.getParams();
    uint32_t start = params.getBlockNum();
    uint32_t count = params.getCount();
    context.releaseParams();

    for (uint32_t i = 0; i < count; i++) {
      overlay[start + i] = nullptr;
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> sync(SyncContext context) override {
    return kj::READY_NOW;
  }

private:
  Volume::Client inner;

  std::unordered_map<uint32_t, kj::Array<byte>> overlay;
  // Maps block index -> block content. All byte arrays are exactly one block in size, unless they
  // are null, in which case the block is all-zero.
};

}  // namespace

// =======================================================================================

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
    KJ_LOG(INFO, "registering package", id.asChars(), packageMount->getPath());
    mounts[packageMount->getId()] = packageMount.get();
  } else {
    packageMount = kj::addRef(*iter->second);

    // Let's update to the latest Volume capability. This way, if our Volume has been disconnected
    // in the background but we haven't noticed yet because the kernel hasn't tried to read any
    // blocks, we can recover. (This is common when a full storage crash occurs: the grains will
    // all have been killed because their grain storage was disconnected, but often it won't be
    // noticed that the packages have disconnected because the kernel hasn't read any blocks from
    // them lately. The kernel definitely won't read any blocks while all the grains are dead, but
    // it might purge some from the cache. Luckily the next time the package is used we'll get a
    // new capability and the kernel will never know anything went wrong!)
    //
    // TODO(security): Do a capability join to verify that they are the same capability, and panic
    //   if they aren't. (Or better yet, turn the whole package mount table into an identity map
    //   and ditch the IDs. Either way we need capnp level 4 to do this.)
    packageMount->updateVolume(package.getVolume());
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
      volumeAdapter(kj::heap<NbdVolumeAdapter>(kj::mv(nbdUserEnd), kj::mv(volume),
                                               NbdAccessType::READ_ONLY)),
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
        NbdBinding binding(device, kj::mv(nbdKernelEnd), NbdAccessType::READ_ONLY);
        Mount mount(device.getPath(), path, MS_RDONLY | MS_NOATIME, nullptr);

        // Signal setup is complete.
        pipe.write(&dummyByte, 1).wait(waitScope);

        // Wait for pipe disconnect.
        while (pipe.tryRead(&dummyByte, 1, 1).wait(waitScope) > 0) {}

        // Note that a just-created grain process could in theory have inherited our package mount
        // into its mount namespace. The meta-supervisor unmounts all packages other than its own
        // immediately after unsharing the mount namespace, but there is a brief time in between
        // when it holds a reference to our mount, and thus our attempt to umount() won't actually
        // unmount the filesystem. Luckily, NbdBinding checks if the device is still mounted and
        // delays disconnect in the meantime, so we're actually safe.

        // We'll close our end of the pipe on the way out, thus signaling completion.
      })),
      loaded(nbdThread.pipe->read(&dummyByte, 1).fork()),
      disconnected(volumeAdapter->onDisconnected().then([this]() {
        // Volume disconnected. Unregister to prevent new grains from reusing this mount.
        KJ_LOG(ERROR, "package volume connection lost", this->id.asChars(), this->path);
        unregister();
      }).fork()) {}

PackageMountSet::PackageMount::~PackageMount() noexcept(false) {
  unregister();

  // We don't want to block waiting for the NBD thread to complete. So, we carefully wait for
  // the thread to report completion in a detached promise.
  //
  // TODO(cleanup): Make kj::Thread::detach() work correctly.

  auto pipe = kj::mv(nbdThread.pipe);

  // Send EOF to the NBD thread, which tells it to disconnect.
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

void PackageMountSet::PackageMount::updateVolume(Volume::Client newVolume) {
  volumeAdapter->updateVolume(kj::mv(newVolume));
}

void PackageMountSet::PackageMount::unregister() {
  if (!unregistered) {
    KJ_LOG(INFO, "unregistering package", id.asChars(), path);
    mountSet.mounts.erase(id);
    unregistered = true;
  }
}

// =======================================================================================

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
               kj::Own<PackageMountSet::PackageMount> packageMountParam,
               sandstorm::Subprocess::Options&& subprocessOptions,
               kj::String grainIdParam,
               sandstorm::SandstormCore::Client core,
               kj::Own<LocalPersistentRegistry::Registration> persistentRegistration)
      : worker(worker),
        workerCap(kj::mv(workerCap)),
        grainState(kj::mv(grainState)),
        grainStateSetter(kj::mv(grainStateSetter)),
        packageMount(kj::mv(packageMountParam)),
        nbdVolume(kj::mv(nbdSocket), kj::mv(volume), NbdAccessType::READ_WRITE),
        volumeRunTask(nbdVolume.run().eagerlyEvaluate([](kj::Exception&& exception) {
          KJ_LOG(FATAL, "NbdVolumeAdapter failed (grain)", exception);
        })),
        volumeDisconnectTask(nbdVolume.onDisconnected()
            .exclusiveJoin(packageMount->onDisconnected())
            .then([this]() {
          // If the package or grain volume disconnected while the grain is still running, kill
          // the grain.
          KJ_LOG(ERROR, "emergency grain shutdown due to storage loss", grainId);
          subprocess.signal(SIGTERM);
        }).eagerlyEvaluate(nullptr)),
        subprocess(kj::mv(subprocessOptions)),
        processWaitTask(worker.subprocessSet.waitForSuccess(subprocess)),
        capnpSocket(kj::mv(capnpSocket)),
        rpcClient(*this->capnpSocket, kj::mv(core)),
        persistentRegistration(kj::mv(persistentRegistration)),
        grainId(kj::mv(grainIdParam)) {
    KJ_LOG(INFO, "starting grain", grainId);
  }

  ~RunningGrain() {
    KJ_LOG(INFO, "stopping grain", grainId);

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
    return rpcClient.bootstrap().castAs<sandstorm::Supervisor>();
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

  kj::Own<LocalPersistentRegistry::Registration> persistentRegistration;
  // We hold on to this until the grain shuts down, so that the grain can be restored from storage.

  kj::String grainId;
};

WorkerImpl::WorkerImpl(kj::AsyncIoContext& ioContext, sandstorm::SubprocessSet& subprocessSet,
                       LocalPersistentRegistry& persistentRegistry)
    : ioProvider(*ioContext.lowLevelProvider), subprocessSet(subprocessSet),
      persistentRegistry(persistentRegistry), packageMountSet(ioContext), tasks(*this) {
  NbdDevice::loadKernelModule();
  KJ_IF_MAYBE(fd, sandstorm::raiiOpenIfExists(
      "/proc/sys/kernel/unprivileged_userns_clone", O_WRONLY | O_TRUNC | O_CLOEXEC)) {
    // This system may need to be configured to enable user namespaces.
    kj::FdOutputStream(fd->get()).write("1\n", 2);
  }
  KJ_IF_MAYBE(fd, sandstorm::raiiOpenIfExists(
      "/proc/sys/fs/inotify/max_user_instances", O_WRONLY | O_TRUNC | O_CLOEXEC)) {
    // Default = 128, but we need one per grain and theoretically we could have 4k grains.
    kj::FdOutputStream(fd->get()).write("8192\n", strlen("8192\n"));
  }
  KJ_IF_MAYBE(fd, sandstorm::raiiOpenIfExists(
      "/proc/sys/fs/inotify/max_user_watches", O_WRONLY | O_TRUNC | O_CLOEXEC)) {
    // Default = 8192, which is too low.
    kj::FdOutputStream(fd->get()).write("524288\n", strlen("524288\n"));
  }
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

  // Create a promise for the Supervisor, and then make that promise persistent. Although in theory
  // we don't need this weirdness in the newGrain() path (only restoreGrain()), we can reuse mode
  // code by doing it in both paths.
  auto paf = kj::newPromiseAndFulfiller<sandstorm::Supervisor::Client>();
  auto persistentRegistration = persistentRegistry.makePersistent(kj::mv(paf.promise));
  sandstorm::Supervisor::Client supervisor =
      persistentRegistration->getWrapped().castAs<sandstorm::Supervisor>();

  // Construct objects and create the GrainState Assignable.
  auto storageFactory = params.getStorage();
  auto grainVolume = storageFactory.newVolumeRequest().send().getVolume();
  auto grainStateHolder = kj::heap<capnp::MallocMessageBuilder>(8);
  auto req = storageFactory.newAssignableRequest<GrainState>();
  {
    auto grainStateValue = req.initInitialValue();
    grainStateValue.setActive(supervisor);
    grainStateValue.setVolume(kj::mv(grainVolume));
    grainStateHolder->setRoot(grainStateValue.asReader());
  }
  OwnedAssignable<GrainState>::Client grainState = req.send().getAssignable();
  auto setter = grainState.getRequest().send().getSetter();

  // Boot the grain.
  paf.fulfiller->fulfill(bootGrain(params.getPackage(),
      kj::mv(grainStateHolder), kj::mv(setter), params.getCommand(), true,
      kj::heapString(params.getGrainId()), params.getCore(),
      kj::mv(persistentRegistration)));

  auto results = context.getResults(capnp::MessageSize { 4, 1 });
  results.setGrain(kj::mv(supervisor));
  results.setGrainState(kj::mv(grainState));

  // Promises are neat.
  return kj::READY_NOW;
}

kj::Promise<void> WorkerImpl::restoreGrain(RestoreGrainContext context) {
  auto params = context.getParams();

  // Create a promise for the Supervisor, and then make that promise persistent. We need to save
  // the promise into the persistent GrainState *before* we actually attempt to start the grain,
  // in order to detect if the GrainState has been modified concurrently.
  auto paf = kj::newPromiseAndFulfiller<sandstorm::Supervisor::Client>();
  auto persistentRegistration = persistentRegistry.makePersistent(kj::mv(paf.promise));
  sandstorm::Supervisor::Client supervisor =
      persistentRegistration->getWrapped().castAs<sandstorm::Supervisor>();

  // Make a copy of the GrainState and modify it to the `active` state.
  auto grainState = params.getGrainState();
  auto grainStateHolder = kj::heap<capnp::MallocMessageBuilder>(
      grainState.totalSize().wordCount + 8);
  grainStateHolder->setRoot(grainState);
  auto mutableGrainState = grainStateHolder->getRoot<GrainState>();
  mutableGrainState.setActive(supervisor);

  // Might as well fill in the results now.
  context.getResults(capnp::MessageSize { 4, 1 }).setGrain(kj::mv(supervisor));

  // Call set() on our GrainState Assignable setter to save the new supervisor cap to storage.
  // TODO(perf): If we were clever, we could start the grain now but prevent any writes or outgoing
  //   messages until the set() succeeds. This would be complicated, though, especially since we
  //   can't call getExclusive() on the volume until we know we actually have exclusivity.
  auto setter = params.getExclusiveGrainStateSetter();
  auto sizeHint = mutableGrainState.totalSize();
  sizeHint.wordCount += 4;
  auto req = setter.setRequest(sizeHint);
  req.setValue(mutableGrainState);
  auto fulfiller = kj::mv(paf.fulfiller);
  return req.send()
      .then([this,context,params,KJ_MVCAP(setter),KJ_MVCAP(fulfiller),
             KJ_MVCAP(persistentRegistration),KJ_MVCAP(grainStateHolder)](auto&&) mutable {
    // OK, the set succeeded, so the grain is officially ours.

    fulfiller->fulfill(bootGrain(params.getPackage(),
        kj::mv(grainStateHolder), kj::mv(setter), params.getCommand(), false,
        kj::heapString(params.getGrainId()), params.getCore(),
        kj::mv(persistentRegistration)));
  });
}

sandstorm::Supervisor::Client WorkerImpl::bootGrain(
    PackageInfo::Reader packageInfo, kj::Own<capnp::MessageBuilder> grainState,
    sandstorm::Assignable<GrainState>::Setter::Client grainStateSetter,
    sandstorm::spk::Manifest::Command::Reader commandReader, bool isNew,
    kj::String grainId, sandstorm::SandstormCore::Client core,
    kj::Own<LocalPersistentRegistry::Registration> persistentRegistration) {
  // Obtain exclusive control of volume.
  auto grainVolume = grainState->getRoot<GrainState>().getVolume()
      .getExclusiveRequest().send().getExclusive();

  // Copy command info from params, since params will no longer be valid when we return.
  CommandInfo command(commandReader);

  // Make sure the package is mounted, then start the grain.
  return packageMountSet.getPackage(packageInfo)
      .then([this,isNew,KJ_MVCAP(grainState),KJ_MVCAP(grainStateSetter),
             KJ_MVCAP(command),KJ_MVCAP(grainVolume),KJ_MVCAP(grainId),
             KJ_MVCAP(core),KJ_MVCAP(persistentRegistration)]
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
        command.commandArgs.size() + command.envArgs.size() + 7 + isNew, 16, 16);
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
      argv[i++] = grainId;
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
        kj::mv(grainVolume), kj::mv(capnpWorkerEnd), kj::mv(packageMount), kj::mv(options),
        kj::mv(grainId), kj::mv(core), kj::mv(persistentRegistration));

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
        nbdVolume(kj::mv(nbdUserEnd), volume, NbdAccessType::READ_WRITE),
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
      results.setVolume(volume);
      if (inResults.hasAuthorPgpKeyFingerprint()) {
        results.setAuthorPgpKeyFingerprint(inResults.getAuthorPgpKeyFingerprint());
      }
      auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
      promises.add(kj::mv(volumeRunTask));
      promises.add(kj::mv(subprocess));
      return kj::joinPromises(promises.finish());
    }).then([this]() {
      // Freeze the volume so it can never be written again.
      return volume.freezeRequest().send().ignoreResult();
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

// -----------------------------------------------------------------------------

kj::Promise<void> WorkerImpl::unpackBackup(UnpackBackupContext context) {
  auto params = context.getParams();
  auto blob = params.getData();
  auto storage = params.getStorage();
  context.releaseParams();

  // Create temporary file.
  auto tmpfile = kj::heap<TemporaryFile>();
  auto& tmpfileRef = *tmpfile;

  // Create the new volume.
  auto volume = storage.newVolumeRequest().send().getVolume();

  // We have two lambdas below that both want to capture `volume`. Due to clang bug #22354, if we
  // try to use the copy constructor to capture `volume` "by value" rather than by move, we run
  // into problems, so manually make a copy here. https://llvm.org/bugs/show_bug.cgi?id=22354
  // TODO(cleanup): Remove this hack when Clang is fixed.
  auto volume2 = volume;

  // Arrange to download the blob.
  auto stream = kj::heap<BlobDownloadStreamImpl>(tmpfile->releaseFd());
  auto& streamRef = *stream;
  sandstorm::ByteStream::Client streamCap = kj::mv(stream);
  auto req = blob.writeToRequest();
  req.setStream(streamCap);
  return req.send()
      .then([this,KJ_MVCAP(volume),&tmpfileRef,KJ_MVCAP(streamCap),&streamRef](auto&&) mutable {
    streamRef.requireDone();

    // Setup NBD.
    auto nbdSocketPair = NbdSocketPair::make(ioProvider);
    auto nbdVolume = kj::heap<NbdVolumeAdapter>(
        kj::mv(nbdSocketPair.userEnd), volume, NbdAccessType::READ_WRITE);
    auto volumeRunTask = nbdVolume->run().attach(kj::mv(nbdVolume));

    // Run backup process.
    sandstorm::Subprocess::Options options({
        "blackrock", "meta-backup", "-r", tmpfileRef.getFilename()});
    options.executable = "/proc/self/exe";
    int moreFds[1] = { nbdSocketPair.kernelEnd };
    auto stdoutPipe = AsyncInSyncOutPipe::make(ioProvider);
    options.stdout = stdoutPipe.writeEnd;
    options.moreFds = moreFds;
    auto process = subprocessSet.waitForSuccess(kj::mv(options));

    // Read the grain info from the process's stdout.
    auto readTask = capnp::readMessage(*stdoutPipe.readEnd).attach(kj::mv(stdoutPipe.readEnd));

    // It's most important to use that volumeRunTask has a chance to complete successfully. It's
    // also important to us that we don't kill the subprocess since it needs to unmount stuff.
    // Comparatively, there's not much harm in cancelling readTask if these fail. Thus, instead
    // of joining the promises, we chain them.
    return volumeRunTask.then([KJ_MVCAP(process)]() mutable {
      return kj::mv(process);
    }).then([KJ_MVCAP(readTask)]() mutable {
      return kj::mv(readTask);
    });
  }).attach(kj::mv(tmpfile))
    .then([context,KJ_MVCAP(volume2)](kj::Own<capnp::MessageReader>&& grainInfoReader) mutable {
    auto grainInfo = grainInfoReader->getRoot<sandstorm::GrainInfo>();
    auto sizeHint = grainInfo.totalSize();
    sizeHint.wordCount += 4;
    sizeHint.capCount += 1;
    auto results = context.getResults(sizeHint);
    results.setVolume(volume2);
    results.setMetadata(grainInfo);
  });
}

kj::Promise<void> WorkerImpl::packBackup(PackBackupContext context) {
  auto params = context.getParams();
  auto volume = params.getVolume();
  auto metadata = params.getMetadata();
  auto storage = params.getStorage();

  auto tmpfile = kj::heap<TemporaryFile>();

  // Setup NBD. We don't want packing a backup to modify the underlying disk, but we do need to
  // mount it read-write because the disk may be in an unclean state which will cause ext4 to want
  // to perform journal playback. So we apply a copy-on-write overlay (CowVolume).
  auto nbdSocketPair = NbdSocketPair::make(ioProvider);
  auto nbdVolume = kj::heap<NbdVolumeAdapter>(
      kj::mv(nbdSocketPair.userEnd),
      kj::heap<CowVolume>(kj::mv(volume)),
      NbdAccessType::READ_WRITE);
  auto volumeRunTask = nbdVolume->run().attach(kj::mv(nbdVolume));

  // Run backup process.
  sandstorm::Subprocess::Options options({
      "blackrock", "meta-backup", tmpfile->getFilename()});
  options.executable = "/proc/self/exe";
  int moreFds[1] = { nbdSocketPair.kernelEnd };
  auto stdinPipe = AsyncOutSyncInPipe::make(ioProvider);
  options.stdin = stdinPipe.readEnd;
  options.moreFds = moreFds;
  auto process = subprocessSet.waitForSuccess(kj::mv(options));

  // Write the grain info to the process's stdin.
  auto grainInfoMessage = kj::heap<capnp::MallocMessageBuilder>(metadata.totalSize().wordCount + 4);
  grainInfoMessage->setRoot(metadata);
  auto writeTask = capnp::writeMessage(*stdinPipe.writeEnd, *grainInfoMessage)
      .attach(kj::mv(grainInfoMessage), kj::mv(stdinPipe.writeEnd))
      .eagerlyEvaluate(nullptr);  // ensure pipe write end gets closed

  // It's most important to use that volumeRunTask has a chance to complete successfully. It's
  // also important to us that we don't kill the subprocess since it needs to unmount stuff.
  // Comparatively, there's not much harm in cancelling readTask if these fail. Thus, instead
  // of joining the promises, we chain them.
  return volumeRunTask.then([KJ_MVCAP(process)]() mutable {
    return kj::mv(process);
  }).then([KJ_MVCAP(writeTask)]() mutable {
    return kj::mv(writeTask);
  }).then([context,KJ_MVCAP(tmpfile),KJ_MVCAP(storage)]() mutable {
    context.releaseParams();
    auto upload = storage.uploadBlobRequest().send();
    context.getResults(capnp::MessageSize {4, 1}).setData(upload.getBlob());
    return uploadBlob(tmpfile->releaseFd(), upload.getStream());
  });
}

// =======================================================================================

class SupervisorMain::SystemConnectorImpl: public sandstorm::SupervisorMain::SystemConnector {
public:
  kj::Promise<void> run(kj::AsyncIoContext& ioContext,
                        sandstorm::Supervisor::Client mainCapability,
                        kj::Own<sandstorm::CapRedirector> coreRedirector) const override {
    auto runner = kj::heap<Runner>(ioContext, kj::mv(mainCapability));

    capnp::MallocMessageBuilder message(8);
    auto vatId = message.getRoot<capnp::rpc::twoparty::VatId>();
    vatId.setSide(capnp::rpc::twoparty::Side::SERVER);
    coreRedirector->setTarget(
        runner->rpcSystem.bootstrap(vatId).castAs<sandstorm::SandstormCore>());

    auto promise = runner->network.onDisconnect();
    return promise.attach(kj::mv(runner));
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
      .expectArg("<grain-id>", KJ_BIND_METHOD(sandstormSupervisor, setGrainId))
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
  unshareMountNamespace();

  // Unmount all packages other than our own, so that we don't hold them open in our mount
  // namespace.
  bool sawSelf = false;
  for (auto& file: sandstorm::listDirectory("/var/blackrock/packages")) {
    auto path = kj::str("/var/blackrock/packages/", file);
    if (path == packageMount) {
      sawSelf = true;
    } else {
      while (umount(path.cStr()) < 0) {
        int error = errno;
        if (error == EINVAL) {
          // Not a mount point?
          KJ_LOG(WARNING, "stale package directory", path);
          break;
        } else if (error != EINTR) {
          KJ_FAIL_SYSCALL("unmount", error, path);
        }
      }
    }
  }
  KJ_REQUIRE(sawSelf, "package mount not seen in packages dir", packageMount);

  // We'll mount our grain data on /mnt because it's our own mount namespace so why not?
  NbdDevice device;
  NbdBinding binding(device, kj::AutoCloseFd(4), NbdAccessType::READ_WRITE);

  if (isNew) {
    device.format();
  } else {
    device.fixSurpriseFeatures();
  }

  KJ_ON_SCOPE_SUCCESS(device.trimJournalIfClean());
  Mount mount(device.getPath(), "/mnt", MS_NOATIME, "discard");
  KJ_SYSCALL(chown("/mnt", 1000, 1000));

  // Mask SIGCHLD and SIGTERM so that we can handle them later.
  sigset_t sigmask;
  sigemptyset(&sigmask);
  sigaddset(&sigmask, SIGCHLD);
  sigaddset(&sigmask, SIGTERM);
  KJ_SYSCALL(sigprocmask(SIG_SETMASK, &sigmask, nullptr));

  KJ_DEFER({
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
          KJ_FAIL_SYSCALL("wait()", error) { return; }
        }
      }
    }
  });

  // In order to ensure we get a chance to clean up after the supervisor exits, we run it in a
  // fork.
  sandstorm::Subprocess child([this]() -> int {
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
  unshareMountNamespace();

  // We'll mount our package on /mnt because it's our own mount namespace so why not?
  NbdDevice device;
  NbdBinding binding(device, kj::AutoCloseFd(3), NbdAccessType::READ_WRITE);
  device.format();
  KJ_ON_SCOPE_SUCCESS(device.trimJournalIfClean());
  Mount mount(device.getPath(), "/mnt", MS_NOATIME, "discard");
  KJ_SYSCALL(mkdir("/mnt/spk", 0755));

  kj::String appId = ({
    // We unpack packages with uid 1/gid 1: IDs that are not zero, but are also not used by apps.
    KJ_SYSCALL(chown("/mnt/spk", 1, 1));
    seteugidNoHang(1, 1);
    KJ_DEFER(seteugidNoHang(0, 0));

    sandstorm::unpackSpk(STDIN_FILENO, "/mnt/spk", "/tmp");
  });

  // Read manifest.
  capnp::ReaderOptions manifestLimits;
  manifestLimits.traversalLimitInWords = sandstorm::spk::Manifest::SIZE_LIMIT_IN_WORDS;
  capnp::StreamFdMessageReader reader(sandstorm::raiiOpen(
      "/mnt/spk/sandstorm-manifest", O_RDONLY | O_CLOEXEC), manifestLimits);

  // Write result to stdout.
  capnp::MallocMessageBuilder message;
  auto root = message.getRoot<Worker::PackageUploadStream::GetResultResults>();
  auto manifest = reader.getRoot<sandstorm::spk::Manifest>();
  root.setAppId(appId);
  root.setManifest(manifest);
  KJ_IF_MAYBE(fp, checkPgpSignatureInBundle(appId, manifest.getMetadata())) {
    root.setAuthorPgpKeyFingerprint(*fp);
  }

  capnp::writeMessageToFd(STDOUT_FILENO, message);

  return true;
}

// =======================================================================================

kj::MainFunc BackupMain::getMain() {
  return kj::MainBuilder(context, "Blackrock version " SANDSTORM_VERSION,
                         "Backs up or restores a grain. FD 3 is expected to be an NBD socket, "
                         "which will be mounted as the grain data directory.\n"
                         "\n"
                         "NOT FOR HUMAN CONSUMPTION: Given the FD requirements, you obviously "
                         "can't run this directly from the command-line. It is intended to be "
                         "invoked by the Blackrock worker.")
      .addOption({'r', "restore"}, [this]() { restore = true; return true; },
                 "Restore a backup, rather than create a backup.")
      .expectArg("<file>", KJ_BIND_METHOD(*this, run))
      .build();
}

kj::MainBuilder::Validity BackupMain::run(kj::StringPtr filename) {
  // Enter mount namespace!
  unshareMountNamespace();

  // We'll mount our grain on /mnt because it's our own mount namespace so why not?
  NbdDevice device;
  NbdBinding binding(device, kj::AutoCloseFd(3), NbdAccessType::READ_WRITE);
  if (restore) {
    device.format();
  } else {
    device.fixSurpriseFeatures();
  }
  KJ_ON_SCOPE_SUCCESS(if (restore) device.trimJournalIfClean());
  Mount mount(device.getPath(), "/mnt", MS_NOATIME, "discard");

  KJ_SYSCALL(chown(filename.cStr(), 1000, 1000));
  if (restore) {
    KJ_SYSCALL(chown("/mnt", 1000, 1000));
    KJ_SYSCALL(mkdir("/mnt/grain", 0755));
    KJ_SYSCALL(chown("/mnt/grain", 1000, 1000));
  }

  sandstorm::Subprocess([&]() -> int {
    // Unfortunately, we are still root here. Drop privs.
    KJ_SYSCALL(setgroups(0, nullptr));
    KJ_SYSCALL(setresgid(1000, 1000, 1000));
    KJ_SYSCALL(setresuid(1000, 1000, 1000));

    // More unfortunately, the ownership of /proc/self/uid_map and similar special files are set
    // on exec() and do not change until the next exec() (fork()s inherit the current permissions
    // of the parent's file). Therefore, now that we've changed our uid, we *must* exec() before
    // we can set up a sandbox. Otherwise, we'd just call sandstorm::BackupMain directly here.

    const char* subArgs[8];
    int pos = 0;
    subArgs[pos++] = "blackrock";
    subArgs[pos++] = "backup";
    if (restore) {
      subArgs[pos++] = "-r";
    }
    subArgs[pos++] = "--root";
    subArgs[pos++] = "/blackrock/bundle";
    subArgs[pos++] = filename.cStr();
    subArgs[pos++] = "/mnt/grain";
    subArgs[pos++] = nullptr;
    KJ_ASSERT(pos <= kj::size(subArgs));

    auto subArgv = const_cast<char* const*>(subArgs);  // execv() is not const-correct. :(
    KJ_SYSCALL(execv("/proc/self/exe", subArgv));
    KJ_UNREACHABLE;
  }).waitForSuccess();

  return true;
}

}  // namespace blackrock
