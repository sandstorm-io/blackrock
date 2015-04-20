// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
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
#include "backend-set.h"
#include "frontend.h"

namespace blackrock {

class MachineImpl: public Machine::Server {
  // TODO(security): For most become*() methods, we should probably actually spawn a child process.
  //   (But before we do that we probably need to implement Cap'n Proto Level 3.)

public:
  MachineImpl(kj::AsyncIoContext& ioContext)
      : ioContext(ioContext), subprocessSet(ioContext.unixEventPort) {}

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
      auto ptr = kj::heap<StorageInfo>(ioContext);
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
    Worker::Client client = nullptr;
    KJ_IF_MAYBE(w, worker) {
      KJ_LOG(INFO, "rebecome worker...");
      client = *w;
    } else {
      KJ_LOG(INFO, "become worker...");
      client = kj::heap<WorkerImpl>(ioContext, subprocessSet);
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
      auto ptr = kj::heap<FrontendInfo>(kj::heap<FrontendImpl>(
          *ioContext.provider, subprocessSet, context.getParams().getConfig()));
      info = ptr;
      frontendInfo = kj::mv(ptr);
    }

    auto results = context.getResults();
    results.setFrontend(info->client);
    results.setStorageRootSet(info->impl->getStorageRootBackendSet());
    results.setStorageFactorySet(info->impl->getStorageFactoryBackendSet());
    results.setWorkerSet(info->impl->getWorkerBackendSet());

    // TODO(soon): These are placeholders.
    results.setStorageRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::Stored>>>());
    results.setHostedRestorerSet(kj::refcounted<BackendSetImpl<Restorer<SturdyRef::Hosted>>>());

    return kj::READY_NOW;
  }

private:
  kj::AsyncIoContext& ioContext;
  sandstorm::SubprocessSet subprocessSet;

  struct StorageInfo {
    StorageSibling::Client selfAsSibling;
    StorageRootSet::Client rootSet;
    MasterRestorer<SturdyRef::Stored>::Client restorer;
    StorageFactory::Client factory;

    kj::Own<BackendSetImpl<StorageSibling>> siblingSet;
    kj::Own<BackendSetImpl<Restorer<SturdyRef::Hosted>>> hostedRestorerSet;
    kj::Own<BackendSetImpl<Restorer<SturdyRef::External>>> gatewayRestorerSet;

    StorageInfo(kj::AsyncIoContext& ioContext)
        : selfAsSibling(nullptr),  // TODO(someday)
          rootSet(kj::heap<FilesystemStorage>(
              sandstorm::raiiOpen("/var/blackrock/storage", O_RDONLY | O_DIRECTORY | O_CLOEXEC),
              ioContext.unixEventPort, ioContext.lowLevelProvider->getTimer(), nullptr)),
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
        .addSubCommand("slave", KJ_BIND_METHOD(*this, getSlaveMain), "run as slave node")
        .addSubCommand("grain", KJ_BIND_METHOD(*this, getSupervisorMain),
            "(internal) run a supervised grain")
        .addSubCommand("unpack", KJ_BIND_METHOD(*this, getUnpackMain),
            "(internal) unpack an spk into a network volume")
        .addSubCommand("log", KJ_BIND_METHOD(*this, getLogMain), "run log client")
        .build();
  }

  kj::MainFunc getMasterMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock master.")
        .addOption({'r', "restart"}, KJ_BIND_METHOD(*this, setRestart),
            "Restart all slave machines immediately.")
        .expectArg("<master-config>", KJ_BIND_METHOD(*this, runMaster))
        .build();
  }

  kj::MainFunc getSlaveMain() {
    return kj::MainBuilder(context, "Sandstorm Blackrock version " SANDSTORM_VERSION,
                           "Starts Blackrock slave.")
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

  kj::MainFunc getUnpackMain() {
    alternateMain = kj::heap<UnpackMain>(context);
    return alternateMain->getMain();
  }

private:
  kj::ProcessContext& context;
  kj::Own<sandstorm::AbstractMain> alternateMain;
  SimpleAddress bindAddress = nullptr;
  bool killedExisting = false;
  bool shouldRestart = false;

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

    for (auto& file: sandstorm::listDirectory("/proc")) {
      KJ_IF_MAYBE(pid, sandstorm::parseUInt(file, 10)) {
        if (*pid != me) {
          auto maybeFd = sandstorm::raiiOpenIfExists(kj::str("/proc/", file, "/comm"), O_RDONLY);
          // No big deal if it doesn't exist. Probably the pid disappeared while we were listing
          // the directory.

          KJ_IF_MAYBE(fd, maybeFd) {
            if (sandstorm::trim(sandstorm::readAll(*fd)) == "blackrock") {
              KJ_SYSCALL(kill(*pid, SIGTERM));
            }
          }
        }
      }
    }

    killedExisting = true;

    return true;
  }

  kj::MainBuilder::Validity setRestart() {
    shouldRestart = true;
    return true;
  }

  bool runMaster(kj::StringPtr configFile) {
    capnp::StreamFdMessageReader configReader(
        sandstorm::raiiOpen(configFile, O_RDONLY | O_CLOEXEC));

    auto ioContext = kj::setupAsyncIo();
    sandstorm::SubprocessSet subprocessSet(ioContext.unixEventPort);
    VagrantDriver driver(subprocessSet, *ioContext.lowLevelProvider);
    blackrock::runMaster(ioContext, driver, configReader.getRoot<MasterConfig>(), shouldRestart);
    KJ_UNREACHABLE;
  }

  bool runSlave() {
    auto pidfile = sandstorm::raiiOpen("/var/run/blackrock-slave",
        O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    int l;
    KJ_NONBLOCKING_SYSCALL(l = flock(pidfile, LOCK_EX | (killedExisting ? 0 : LOCK_NB)));

    if (l < 0) {
      // pidfile locked; slave already running
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

      // Fork again so that we are no longer session leader, which prevents us from picking up a
      // controlling terminal by merely opening one.
      pid_t pid;
      KJ_SYSCALL(pid = fork());
      if (pid == 0) {
        // Parent exits.
        return 0;
      }

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

      // Set up RPC.
      // TODO(security): Only let the master bootstrap the MachineImpl.
      auto rpcSystem = capnp::makeRpcServer(network, kj::heap<MachineImpl>(ioContext));

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
};

}  // namespace blackrock

KJ_MAIN(blackrock::Main)
