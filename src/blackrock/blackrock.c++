// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
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

#include <kj/main.h>
#include <kj/async-io.h>
#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/serialize.h>
#include <sandstorm/version.h>
#include <sandstorm/util.h>
#include <blackrock/machine.capnp.h>
#include "cluster-rpc.h"
#include "worker.h"
#include "fs-storage.h"
#include "master.h"
#include "logs.h"
#include <netdb.h>
#include <sys/file.h>
#include <sys/sendfile.h>
#include <sys/prctl.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/eventfd.h>
#include <sys/mount.h>
#include "backend-set.h"
#include "frontend.h"
#include "local-persistent-registry.h"
#include <stdio.h>
#include "nbd-bridge.h"
#include "gce.h"
#include "bundle.h"
#include <sandstorm/backup.h>
#include <sys/time.h>
#include <sys/resource.h>

namespace blackrock {

class RemoteRestorer: public Restorer<SturdyRef>::Server {
  // Implements a Restorer intended to be called by *this* process to restore *remote* SturdyRefs.
  // This mainly delegates to the various other restorer capabilities.

public:
  explicit RemoteRestorer(capnp::RpcSystem<VatPath>& rpcSystem): rpcSystem(rpcSystem) {}

protected:
  kj::Promise<void> restore(RestoreContext context) override {
    auto ref = context.getParams().getSturdyRef();

    if (ref.isTransient()) {
      auto transient = ref.getTransient();
      auto req = rpcSystem.bootstrap(transient.getVat()).castAs<Restorer<>>().restoreRequest();
      req.setSturdyRef(transient.getLocalRef());
      context.releaseParams();
      // We can't quite tail-call here because it's a different generic type. Darn.
      context.getResults(capnp::MessageSize {4, 1}).setCap(req.send().getCap());
      return kj::READY_NOW;
    } else {
      return KJ_EXCEPTION(UNIMPLEMENTED, "Restoring non-transient ref not implemented.");
    }
  }

  kj::Promise<void> drop(DropContext context) override {
    auto ref = context.getParams().getSturdyRef();

    if (ref.isTransient()) {
      auto transient = ref.getTransient();
      auto req = rpcSystem.bootstrap(transient.getVat()).castAs<Restorer<>>().dropRequest();
      req.setSturdyRef(transient.getLocalRef());
      context.releaseParams();
      // We can't quite tail-call here because it's a different generic type. Darn.
      return req.send().ignoreResult();
    } else {
      return KJ_EXCEPTION(UNIMPLEMENTED, "Restoring non-transient ref not implemented.");
    }
  }

private:
  capnp::RpcSystem<VatPath>& rpcSystem;
};

class MachineImpl: public Machine::Server {
  // TODO(security): For most become*() methods, we should probably actually spawn a child process.
  //   (But before we do that we probably need to implement Cap'n Proto Level 3.)

public:
  MachineImpl(kj::AsyncIoContext& ioContext, capnp::RpcSystem<VatPath>& rpcSystem,
              LocalPersistentRegistry& persistentRegistry, SimpleAddress selfAddress)
      : ioContext(ioContext),
        persistentRegistry(persistentRegistry),
        rpcSystem(rpcSystem),
        subprocessSet(ioContext.unixEventPort),
        selfAddress(selfAddress) {}

  kj::Promise<void> becomeStorage(BecomeStorageContext context) override {
    StorageInfo* info;
    KJ_IF_MAYBE(i, storageInfo) {
      KJ_LOG(INFO, "rebecome storage...");
      info = *i;
    } else {
      KJ_LOG(INFO, "become storage...");
      mkdir("/var", 0755);
      mkdir("/var/blackrock", 0755);
      mkdir("/var/blackrock/storage", 0755);
      auto ptr = kj::heap<StorageInfo>(ioContext, rpcSystem);
      info = ptr;
      storageInfo = kj::mv(ptr);
    }

    auto results = context.getResults();
    results.setSibling(info->selfAsSibling);
    results.setRootSet(info->rootSet);
    results.setStorageRestorer(info->restorer);
    results.setStorageFactory(info->factory);

    results.setSiblingSet(kj::addRef(*info->siblingSet));
    results.setHostedRestorerSet(kj::addRef(*info->hostedRestorerSet));
    results.setGatewayRestorerSet(kj::addRef(*info->gatewayRestorerSet));

    return kj::READY_NOW;
  }

  kj::Promise<void> becomeWorker(BecomeWorkerContext context) override {
    mkdir("/var", 0755);
    mkdir("/var/blackrock", 0755);

    // Mark this machine as "dirty", which means we will refuse to start back up after a crash
    // except with -r (which the master never uses on workers except when running a dev build
    // locally). We do this because if we crash then NBD devices could be left in a bad state.
    sandstorm::raiiOpen("/var/blackrock/dirty", O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC);

    Worker::Client client = nullptr;
    KJ_IF_MAYBE(w, worker) {
      KJ_LOG(INFO, "rebecome worker...");
      client = *w;
    } else {
      KJ_LOG(INFO, "become worker...");
      client = kj::heap<WorkerImpl>(ioContext, subprocessSet, persistentRegistry);
      worker = client;
    }

    context.getResults().setWorker(kj::mv(client));
    return kj::READY_NOW;
  }

  kj::Promise<void> becomeFrontend(BecomeFrontendContext context) override {
    FrontendInfo* info = nullptr;
    KJ_IF_MAYBE(i, frontendInfo) {
      KJ_LOG(INFO, "rebecome frontend...");
      i->get()->impl->setConfig(context.getParams().getConfig());
      info = *i;
    } else {
      KJ_LOG(INFO, "become frontend...");
      auto params = context.getParams();
      auto ptr = kj::heap<FrontendInfo>(kj::heap<FrontendImpl>(
          *ioContext.lowLevelProvider,
          subprocessSet, params.getConfig(), params.getReplicaNumber()));
      info = ptr;
      frontendInfo = kj::mv(ptr);
    }

    auto results = context.getResults();
    results.setFrontend(info->client);
    results.setStorageRootSet(info->impl->getStorageRootBackendSet());
    results.setStorageFactorySet(info->impl->getStorageFactoryBackendSet());
    results.setWorkerSet(info->impl->getWorkerBackendSet());
    results.setMongoSet(info->impl->getMongoBackendSet());

    // TODO(soon): These are placeholders.
    results.setStorageRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::Stored>>>());
    results.setHostedRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::Hosted>>>());

    return kj::READY_NOW;
  }

  kj::Promise<void> becomeMongo(BecomeMongoContext context) override {
    Mongo::Client client = nullptr;
    KJ_IF_MAYBE(w, mongo) {
      KJ_LOG(INFO, "rebecome mongo...");
      client = *w;
    } else {
      KJ_LOG(INFO, "become mongo...");
      SimpleAddress mongoAddr = selfAddress;
      mongoAddr.setPort(27017);
      client = kj::heap<MongoImpl>(ioContext.provider->getTimer(), subprocessSet, mongoAddr);
      mongo = client;
    }

    context.getResults().setMongo(kj::mv(client));
    return kj::READY_NOW;
  }

  kj::Promise<void> ping(PingContext context) override {
    if (context.getParams().getHang()) {
      return kj::NEVER_DONE;
    } else {
      return kj::READY_NOW;
    }
  }

private:
  kj::AsyncIoContext& ioContext;
  LocalPersistentRegistry& persistentRegistry;
  capnp::RpcSystem<VatPath>& rpcSystem;
  sandstorm::SubprocessSet subprocessSet;
  SimpleAddress selfAddress;

  struct StorageInfo {
    StorageSibling::Client selfAsSibling;
    StorageRootSet::Client rootSet;
    MasterRestorer<SturdyRef::Stored>::Client restorer;
    StorageFactory::Client factory;

    kj::Own<BackendSetImpl<StorageSibling>> siblingSet;
    kj::Own<BackendSetImpl<Restorer<SturdyRef::Hosted>>> hostedRestorerSet;
    kj::Own<BackendSetImpl<Restorer<SturdyRef::External>>> gatewayRestorerSet;

    StorageInfo(kj::AsyncIoContext& ioContext, capnp::RpcSystem<VatPath>& rpcSystem)
        : selfAsSibling(nullptr),  // TODO(someday)
          rootSet(kj::heap<FilesystemStorage>(
              sandstorm::raiiOpen("/var/blackrock/storage", O_RDONLY | O_DIRECTORY | O_CLOEXEC),
              ioContext.unixEventPort, ioContext.lowLevelProvider->getTimer(),
              kj::heap<RemoteRestorer>(rpcSystem))),
          restorer(nullptr),       // TODO(someday)
          factory(rootSet.getFactoryRequest().send().getFactory()),
          siblingSet(kj::refcounted<BackendSetImpl<StorageSibling>>()),
          hostedRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::Hosted>>>()),
          gatewayRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::External>>>()) {}
  };
  kj::Maybe<kj::Own<StorageInfo>> storageInfo;

  kj::Maybe<Worker::Client> worker;

  struct FrontendInfo {
    FrontendImpl* impl;
    Frontend::Client client;

    FrontendInfo(kj::Own<FrontendImpl> impl)
        : impl(impl), client(kj::mv(impl)) {}
  };
  kj::Maybe<kj::Own<FrontendInfo>> frontendInfo;

  kj::Maybe<Mongo::Client> mongo;
};

class BootstrapFactoryImpl: public capnp::BootstrapFactory<VatPath> {
public:
  BootstrapFactoryImpl(LocalPersistentRegistry& persistentRegistry,
                       Machine::Client machine)
      : persistentRegistry(persistentRegistry), machine(machine) {}

  capnp::Capability::Client createFor(VatPath::Reader clientId) override {
    // Read the master ID from disk.
    capnp::StreamFdMessageReader masterIdReader(
        sandstorm::raiiOpen("/var/run/master-vatid", O_RDONLY | O_CLOEXEC));
    auto masterVatId = masterIdReader.getRoot<VatId>();

    auto key = clientId.getId();
    if (masterVatId.getPublicKey0() == key.getPublicKey0() &&
        masterVatId.getPublicKey1() == key.getPublicKey1() &&
        masterVatId.getPublicKey2() == key.getPublicKey2() &&
        masterVatId.getPublicKey3() == key.getPublicKey3()) {
      // Master calling. Return the machine.
      return machine;
    } else {
      // Some other slave node calling. Return our restorer.
      return persistentRegistry.createRestorerFor(clientId);
    }
  }

private:
  LocalPersistentRegistry& persistentRegistry;
  Machine::Client machine;
};

static constexpr const char LOG_ADDRESS_FILE[] = "/var/run/blackrock-logsink-address";

class Main {
public:
  Main(kj::ProcessContext& context): context(context) {
    kj::_::Debug::setLogLevel(kj::LogSeverity::INFO);
  }

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock.")
        .addSubCommand("master", KJ_BIND_METHOD(*this, getMasterMain), "run as master node")
        .addSubCommand("start", KJ_BIND_METHOD(*this, getStartMain), "start master as daemon")
        .addSubCommand("slave", KJ_BIND_METHOD(*this, getSlaveMain), "run as slave node")
        .addSubCommand("grain", KJ_BIND_METHOD(*this, getMetaSupervisorMain),
            "(internal) run a grain meta-supervisor")
        .addSubCommand("supervise", KJ_BIND_METHOD(*this, getSupervisorMain),
            "(internal) run a grain supervisor")
        .addSubCommand("unpack", KJ_BIND_METHOD(*this, getUnpackMain),
            "(internal) unpack an spk into a network volume")
        .addSubCommand("meta-backup", KJ_BIND_METHOD(*this, getMetaBackupMain),
            "(internal) backup/restore grain data from/to volume")
        .addSubCommand("backup", KJ_BIND_METHOD(*this, getBackupMain),
            "(internal) backup/restore grain data from/to directory")
        .addSubCommand("log", KJ_BIND_METHOD(*this, getLogMain), "run log client")
        .build();
  }

  kj::MainFunc getMasterMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Runs Blackrock master in foreground, logging to stdout.")
        .addOption({'r', "restart"}, KJ_BIND_METHOD(*this, setRestart),
            "Restarts Blackrock processes on all slave machines if already running.")
        .addOptionWithArg({'R', "restart-one"}, KJ_BIND_METHOD(*this, addRestart), "<machine>",
            "Restarts Blackrock process on the given machine if already running.")
        .expectArg("<master-config>", KJ_BIND_METHOD(*this, runMaster))
        .build();
  }

  kj::MainFunc getStartMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock master as daemon, logging to log directory.")
        .expectArg("<master-config>", KJ_BIND_METHOD(*this, runMasterDaemon))
        .build();
  }

  kj::MainFunc getSlaveMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock slave. The master vat's VatId must be written to "
                           "the slaves stdin. It will then write its own VatPath to stdout.")
        .addOptionWithArg({'l', "log"}, KJ_BIND_METHOD(*this, setLogSink), "<addr>/<name>",
            "Redirect console logs to the log sink server at <addr>, self-identifying as <name>.")
        .addOption({'r', "restart"}, KJ_BIND_METHOD(*this, killExisting),
            "Kill any existing slave running on this machine.")
        .expectArg("<bind-ip>", KJ_BIND_METHOD(*this, setBindIp))
        .callAfterParsing(KJ_BIND_METHOD(*this, runSlave))
        .build();
  }

  kj::MainFunc getLogMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock logger client, which reads from standard input "
                           "and sends the data to the blackrock log server.")
        .expectArg("<name>", KJ_BIND_METHOD(*this, runLog))
        .build();
  }

  kj::MainFunc getSupervisorMain() {
    alternateMain = kj::heap<SupervisorMain>(context);
    return alternateMain->getMain();
  }

  kj::MainFunc getMetaSupervisorMain() {
    alternateMain = kj::heap<MetaSupervisorMain>(context);
    return alternateMain->getMain();
  }

  kj::MainFunc getUnpackMain() {
    alternateMain = kj::heap<UnpackMain>(context);
    return alternateMain->getMain();
  }

  kj::MainFunc getBackupMain() {
    alternateMain = kj::heap<sandstorm::BackupMain>(context);
    return alternateMain->getMain();
  }

  kj::MainFunc getMetaBackupMain() {
    alternateMain = kj::heap<BackupMain>(context);
    return alternateMain->getMain();
  }

private:
  kj::ProcessContext& context;
  kj::Own<sandstorm::AbstractMain> alternateMain;
  SimpleAddress bindAddress = nullptr;
  bool killedExisting = false;
  bool shouldRestart = false;
  kj::Vector<kj::StringPtr> machinesToRestart;

  kj::Maybe<kj::StringPtr> loggingName;

  kj::MainBuilder::Validity setLogSink(kj::StringPtr arg) {
    kj::StringPtr addrStr, name;
    kj::String scratch;

    KJ_IF_MAYBE(slashPos, arg.findFirst('/')) {
      scratch = kj::str(arg.slice(0, *slashPos));
      name = arg.slice(*slashPos + 1);
      addrStr = scratch;
    } else {
      addrStr = arg;
      name = nullptr;
    }

    auto address = SimpleAddress::lookup(addrStr);

    // Verify that we can connect at all.
    {
      int sock_;
      KJ_SYSCALL(sock_ = socket(address.family(), SOCK_STREAM, 0));
      kj::AutoCloseFd sock(sock_);
      KJ_SYSCALL(connect(sock, address.asSockaddr(), address.getSockaddrSize()));
    }

    kj::String tempname = kj::str(LOG_ADDRESS_FILE, '~');
    kj::FdOutputStream(sandstorm::raiiOpen(tempname, O_WRONLY | O_CREAT | O_EXCL, 0600))
        .write(&address, sizeof(address));
    KJ_SYSCALL(rename(tempname.cStr(), LOG_ADDRESS_FILE));

    loggingName = name;
    return true;
  }

  kj::MainBuilder::Validity setBindIp(kj::StringPtr bindIp) {
    if (bindIp.startsWith("if4:")) {
      bindAddress = SimpleAddress::getInterfaceAddress(AF_INET, bindIp.slice(strlen("if4:")));
      return true;
    } else if (bindIp.startsWith("if6:")) {
      bindAddress = SimpleAddress::getInterfaceAddress(AF_INET6, bindIp.slice(strlen("if6:")));
      return true;
    } else {
      bindAddress = SimpleAddress::lookup(bindIp);
      return true;
    }
  }

  kj::MainBuilder::Validity killExisting() {
    pid_t me = getpid();

    // Carefully find and terminate all meta-supervisors, which should in turn terminate
    // supervisors.
    auto supers = findProcesses("blackrock-msup");
    if (supers.size() > 0) {
      for (auto pid: supers) {
        kill(pid, SIGTERM);
      }
      KJ_LOG(INFO, "waiting for supervisors to terminate", supers.size());

      for (;;) {
        sleep(1);

        supers = findProcesses("blackrock-msup");
        if (supers.size() == 0) break;

        KJ_LOG(INFO, "still waiting...", supers.size());
      }
    }

    // All supervisors should be dead, but let's check.
    supers = findProcesses("blackrock-sup");
    if (supers.size() > 0) {
      KJ_LOG(ERROR, "orphan supervisors detected!");
      for (auto pid: supers) {
        kill(pid, SIGKILL);
      }
    }

    // If this is a worker machine, there may be leftover package mounts. Unmount them.
    if (access("/var/blackrock/packages", F_OK) >= 0) {
      for (auto& pkg: sandstorm::listDirectory("/var/blackrock/packages")) {
        KJ_LOG(INFO, "unmounting package", pkg);
        auto path = kj::str("/var/blackrock/packages/", pkg);
        umount2(path.cStr(), MNT_FORCE);
        rmdir(path.cStr());
      }
    }

    // Disconnect all NBD devices if present.
    if (access("/dev/nbd0", F_OK) == 0) {
      NbdDevice::disconnectAll();
      KJ_LOG(INFO, "NBD disconnect successful");
    }

    // Kill the blackrock processes. (We didn't do this earlier because killing the server end
    // of an NBD while the client is still running can lead to deadlock and other badness.)
    for (auto& file: sandstorm::listDirectory("/proc")) {
      KJ_IF_MAYBE(pid, sandstorm::parseUInt(file, 10)) {
        if (*pid != me) {
          auto maybeFd = sandstorm::raiiOpenIfExists(kj::str("/proc/", file, "/comm"), O_RDONLY);
          // No big deal if it doesn't exist. Probably the pid disappeared while we were listing
          // the directory.

          KJ_IF_MAYBE(fd, maybeFd) {
            if (sandstorm::trim(sandstorm::readAll(*fd)) == "blackrock") {
              kill(*pid, SIGKILL);
            }
          }
        }
      }
    }

    killedExisting = true;

    return true;
  }

  kj::Array<pid_t> findProcesses(kj::StringPtr comm) {
    pid_t me = getpid();
    kj::Vector<pid_t> result;

    for (auto& file: sandstorm::listDirectory("/proc")) {
      KJ_IF_MAYBE(pid, sandstorm::parseUInt(file, 10)) {
        if (*pid != me) {
          auto maybeFd = sandstorm::raiiOpenIfExists(kj::str("/proc/", file, "/comm"), O_RDONLY);
          // No big deal if it doesn't exist. Probably the pid disappeared while we were listing
          // the directory.

          KJ_IF_MAYBE(fd, maybeFd) {
            if (sandstorm::trim(sandstorm::readAll(*fd)) == comm) {
              result.add(*pid);
            }
          }
        }
      }
    }

    return result.releaseAsArray();
  }

  kj::MainBuilder::Validity setRestart() {
    shouldRestart = true;
    return true;
  }

  kj::MainBuilder::Validity addRestart(kj::StringPtr arg) {
    machinesToRestart.add(arg);
    return true;
  }

  bool runMaster(kj::StringPtr configFile) {
    KJ_LOG(INFO, "*** Starting Blackrock Master ***");

    capnp::StreamFdMessageReader configReader(
        sandstorm::raiiOpen(configFile, O_RDONLY | O_CLOEXEC));
    auto config = configReader.getRoot<MasterConfig>();

    auto ioContext = kj::setupAsyncIo();
    sandstorm::SubprocessSet subprocessSet(ioContext.unixEventPort);

    kj::Own<ComputeDriver> driver;
    switch (config.which()) {
      case MasterConfig::VAGRANT:
        driver = kj::heap<VagrantDriver>(subprocessSet, *ioContext.lowLevelProvider);
        break;
      case MasterConfig::GCE:
        driver = kj::heap<GceDriver>(subprocessSet, *ioContext.lowLevelProvider, config.getGce());
        break;
    }
    blackrock::runMaster(ioContext, *driver, config, shouldRestart, machinesToRestart);
    KJ_UNREACHABLE;
  }

  bool runMasterDaemon(kj::StringPtr configFile) {
    sandstorm::recursivelyCreateParent("/var/blackrock/shutdown");

    // Kill the old master.
    auto existing = findProcesses("blackrock-mstr");
    if (existing.size() > 0) {
      sandstorm::raiiOpen("/var/blackrock/shutdown", O_CREAT | O_TRUNC | O_CLOEXEC);
      for (uint i = 0; existing.size() > 0; i++) {
        KJ_ASSERT(existing.size() == 1, "multiple blackrock master processes?");
        if (i == 0) {
          context.warning("sending SIGTERM to old blackrock master...");
          KJ_SYSCALL(kill(existing[0], SIGTERM));
        } else if (i == 15) {
          context.warning("sending SIGKILL to old blackrock master...");
          KJ_SYSCALL(kill(existing[0], SIGTERM));
        } else {
          context.warning("(waiting for shutdown)");
        }
        sleep(1);
        existing = findProcesses("blackrock-mstr");
      }
      context.warning("old master shutdown complete...");
      sleep(1);
      KJ_ASSERT(findProcesses("blackrock-mstr").size() == 0,
                "blackrock master returned from the dead!");
      KJ_SYSCALL(unlink("/var/blackrock/shutdown"));
    }

    sandstorm::Subprocess([&]() -> int {
      // Detach from controlling terminal and make ourselves session leader.
      KJ_SYSCALL(setsid());

      // Repeatedly re-run until shutdown file appears.
      while (access("/var/blackrock/shutdown", F_OK) < 0) {
        kj::runCatchingExceptions([&]() {
          auto logPipe = sandstorm::Pipe::make();

          // Fork again so that we are no longer session leader (which prevents us from picking up
          // a controlling terminal by merely opening one).
          sandstorm::Subprocess subprocess2([&]() -> int {
            KJ_SYSCALL(dup2(logPipe.writeEnd, STDOUT_FILENO));
            KJ_SYSCALL(dup2(logPipe.writeEnd, STDERR_FILENO));
            logPipe.readEnd = nullptr;
            logPipe.writeEnd = nullptr;
            KJ_SYSCALL(prctl(PR_SET_NAME, "blackrock-mstr", 0, 0, 0));
            return runMaster(configFile) ? 0 : 1;
          });

          // Parent records logs.
          logPipe.writeEnd = nullptr;
          sandstorm::recursivelyCreateParent("/var/blackrock/log/dummy");
          auto logDirFd = sandstorm::raiiOpen(
              "/var/blackrock/log", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
          rotateLogs(logPipe.readEnd, logDirFd);
          subprocess2.waitForSuccess();
        });
      }

      return 0;
    }).detach();

    context.exitInfo("Blackrock started");
  }

  bool runSlave() {
    if (!killedExisting) {
      if (access("/var/blackrock/dirty", F_OK) == 0) {
        context.exitError("worker dirty; needs reboot");
      }
    }

    increaseFdLimit();
    createSandstormDirectories();

    auto pidfile = sandstorm::raiiOpen("/var/run/blackrock-slave",
        O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    int l;
    KJ_NONBLOCKING_SYSCALL(l = flock(pidfile, LOCK_EX | (killedExisting ? 0 : LOCK_NB)));

    // Copy master's VatId from stdin to disk.
    dumpToFile(STDIN_FILENO, sandstorm::raiiOpen("/var/run/master-vatid.next",
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600));
    KJ_SYSCALL(rename("/var/run/master-vatid.next", "/var/run/master-vatid"));

    if (l < 0) {
      // pidfile locked; slave already running

      // Copy pidfile (slave VatPath) from disk to stdout.
      dumpFile(pidfile, STDOUT_FILENO);

      context.exit();
    }

    // We're the only slave running. Go!
    KJ_SYSCALL(ftruncate(pidfile, 0));

    int readyPipe[2];
    KJ_SYSCALL(pipe2(readyPipe, O_CLOEXEC));
    kj::AutoCloseFd readyPipeIn(readyPipe[0]);
    kj::AutoCloseFd readyPipeOut(readyPipe[1]);

    sandstorm::Subprocess daemon([&]() -> int {
      readyPipeIn = nullptr;

      // Detach from controlling terminal and make ourselves session leader.
      KJ_SYSCALL(setsid());

      // Write logs to log process.
      KJ_IF_MAYBE(n, loggingName) {
        int fds[2];
        KJ_SYSCALL(pipe2(fds, O_CLOEXEC));
        kj::AutoCloseFd readEnd(fds[0]);
        kj::AutoCloseFd writeEnd(fds[1]);

        sandstorm::Subprocess::Options options({"blackrock", "log", *n});
        options.executable = "/proc/self/exe";
        options.stdin = readEnd;
        sandstorm::Subprocess(kj::mv(options)).detach();
        KJ_SYSCALL(dup2(writeEnd, STDERR_FILENO));
      }

      // Create a new pid namespace so that when the blackrock daemon is killed or dies, everything
      // below it dies.
      KJ_SYSCALL(unshare(CLONE_NEWPID));

      // Fork again so that we are no longer session leader (which prevents us from picking up a
      // controlling terminal by merely opening one) and to enter the new PID namespace.
      pid_t pid;
      KJ_SYSCALL(pid = fork());
      if (pid != 0) {
        // Parent exits.
        return 0;
      }

      KJ_ASSERT(getpid() == 1);

      // Redirect stdout to stderr (i.e. the log sink).
      KJ_SYSCALL(dup2(STDERR_FILENO, STDOUT_FILENO));

      // Make standard input /dev/null.
      KJ_SYSCALL(dup2(sandstorm::raiiOpen("/dev/null", O_RDONLY | O_CLOEXEC), STDIN_FILENO));

      // Set up the VatNetwork.
      auto ioContext = kj::setupAsyncIo();
      VatNetwork network(ioContext.provider->getNetwork(), ioContext.provider->getTimer(),
                         bindAddress);

      // Write VatPath to pidfile.
      {
        capnp::MallocMessageBuilder vatPath(16);
        vatPath.setRoot(network.getSelf());
        capnp::writeMessageToFd(pidfile, vatPath);
      }

      // Signal readiness to parent.
      byte b = 0;
      KJ_SYSCALL(write(readyPipeOut, &b, 1));
      readyPipeOut = nullptr;

      KJ_LOG(INFO, "starting slave...");

      // Awkwardly, MachineImpl requires a reference to the RPC system, but we need to pass the
      // bootstrap capability into the RPC system. We solve this with a promise.
      auto paf = kj::newPromiseAndFulfiller<Machine::Client>();

      LocalPersistentRegistry persistentRegistry(network.getSelf());
      BootstrapFactoryImpl bootstrapFactory(persistentRegistry, kj::mv(paf.promise));

      // Set up RPC.
      auto rpcSystem = capnp::makeRpcServer(network, bootstrapFactory);

      // OK, now we can construct the MachineImpl.
      paf.fulfiller->fulfill(kj::heap<MachineImpl>(
          ioContext, rpcSystem, persistentRegistry,
          SimpleAddress(network.getSelf().getAddress())));

      // Loop forever handling messages.
      kj::NEVER_DONE.wait(ioContext.waitScope);
      KJ_UNREACHABLE;
    });

    readyPipeOut = nullptr;

    // Wait for child ready.
    byte b;
    ssize_t n;
    KJ_SYSCALL(n = read(readyPipeIn, &b, 1));
    if (n < 1) {
      // Pipe closed before ready. Child probably logged.
      context.exitError("child process failed to become ready");
    } else {
      // The pidfile contains the VatPath. Write it to stdout, then exit;
      dumpFile(pidfile, STDOUT_FILENO);
      context.exit();
    }
  }

  bool runLog(kj::StringPtr name) {
    // Rename the task so that when we kill all blackrock processes we can avoid killing the
    // logger, which will die naturally as soon as it finishes up.
    KJ_SYSCALL(prctl(PR_SET_NAME, "blackrock-log", 0, 0, 0));

    runLogClient(name, LOG_ADDRESS_FILE, "/var/log");
    KJ_UNREACHABLE;
  }

  void dumpFile(int inFd, int outFd) {
    ssize_t n;
    off_t offset = 0;
    do {
      KJ_SYSCALL(n = sendfile(outFd, inFd, &offset, 4096));
    } while (n > 0);
  }

  void dumpToFile(int inFd, int outFd) {
    ssize_t n;
    do {
      KJ_SYSCALL(n = splice(inFd, nullptr, outFd, nullptr, 4096, 0));
    } while (n > 0);
  }

  void increaseFdLimit() {
    struct rlimit limit;
    memset(&limit, 0, sizeof(limit));
    limit.rlim_cur = 65536;
    limit.rlim_max = 65536;
    KJ_SYSCALL(setrlimit(RLIMIT_NOFILE, &limit));
  }
};

}  // namespace blackrock

KJ_MAIN(blackrock::Main)
