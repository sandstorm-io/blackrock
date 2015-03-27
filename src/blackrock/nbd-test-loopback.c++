// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "nbd-bridge.h"
#include "fs-storage.h"
#include <kj/main.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <errno.h>
#include <signal.h>
#include <sandstorm/util.h>
#include <unistd.h>
#include <sched.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>

namespace blackrock {
namespace {

class NbdLoopbackMain {
  // A test program that mounts a FUSE filesystem that just mirrors some other directory.

public:
  NbdLoopbackMain(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "Fuse test, unknown version",
          "Creates a Sandstore at <source-dir> containing a single Volume, then mounts that "
          "volume at <mount-point>.")
        .addOptionWithArg({'o', "options"}, KJ_BIND_METHOD(*this, setOptions), "<options>",
                          "Set mount options.")
        .addOption({'r', "reset"}, KJ_BIND_METHOD(*this, reset),
                   "Reset all nbd devices, hopefully killing any processes blocked on them.")
        .expectArg("<mount-point>", KJ_BIND_METHOD(*this, setMountPoint))
        .expectArg("<soure-dir>", KJ_BIND_METHOD(*this, setStorageDir))
        .expectOneOrMoreArgs("<command>", KJ_BIND_METHOD(*this, addCommandArg))
        .callAfterParsing(KJ_BIND_METHOD(*this, run))
        .build();
  }

private:
  kj::ProcessContext& context;
  kj::StringPtr options;
  kj::StringPtr mountPoint;
  kj::AutoCloseFd storage;
  kj::Vector<kj::StringPtr> command;

  kj::MainBuilder::Validity setOptions(kj::StringPtr arg) {
    options = arg;
    return true;
  }

  kj::MainBuilder::Validity setMountPoint(kj::StringPtr arg) {
    mountPoint = arg;
    return true;
  }

  kj::MainBuilder::Validity setStorageDir(kj::StringPtr arg) {
    storage = sandstorm::raiiOpen(arg.cStr(), O_RDONLY | O_DIRECTORY);
    return true;
  }

  kj::MainBuilder::Validity addCommandArg(kj::StringPtr arg) {
    command.add(arg);
    return true;
  }

  kj::MainBuilder::Validity reset() {
    NbdDevice::resetAll();
    context.exit();
  }

  kj::MainBuilder::Validity run() {
    KJ_SYSCALL(unshare(CLONE_NEWNS), "are you root?");

    mkdirat(storage, "staging", 0777);
    mkdirat(storage, "main", 0777);
    mkdirat(storage, "death-row", 0777);

    auto stagingFd = sandstorm::raiiOpenAt(storage, "staging", O_RDONLY | O_DIRECTORY);
    auto mainFd = sandstorm::raiiOpenAt(storage, "main", O_RDONLY | O_DIRECTORY);
    auto deathRowFd = sandstorm::raiiOpenAt(storage, "death-row", O_RDONLY | O_DIRECTORY);
    auto journalFd = sandstorm::raiiOpenAt(storage, "journal", O_RDWR | O_CREAT);

    // Read the root key, or generate it.
    FilesystemStorage::ObjectKey key;
    auto rootKeyFd = sandstorm::raiiOpenAt(storage, "root-key", O_RDWR | O_CREAT);
    bool newKey = false;
    {
      ssize_t n;
      KJ_SYSCALL(n = read(rootKeyFd, &key, sizeof(key)));
      if (n == 0) {
        newKey = true;
      }
    }

    int pair[2];
    socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, pair);
    kj::AutoCloseFd kernelEnd(pair[0]);
    kj::AutoCloseFd userEnd(pair[1]);

    kj::AutoCloseFd abortEvent = newEventFd(0, EFD_CLOEXEC | EFD_NONBLOCK);

    kj::Thread serverThread([&]() {
      auto io = kj::setupAsyncIo();
      FilesystemStorage storage(mainFd, stagingFd, deathRowFd, journalFd, io.unixEventPort,
                                io.provider->getTimer(), nullptr);

      auto factory = storage.getFactory();
      OwnedVolume::Client volume = nullptr;

      if (newKey) {
        volume = factory.newVolumeRequest().send().getVolume();
        auto req2 = factory.newAssignableRequest<OwnedVolume>();
        req2.setInitialValue(volume);
        key = storage.setRoot(req2.send().getAssignable()).wait(io.waitScope);
        KJ_SYSCALL(write(rootKeyFd, &key, sizeof(key)));
      } else {
        volume = storage.getRoot<OwnedVolume>(key).getRequest().send().getValue();
      }

      kj::UnixEventPort::FdObserver cancelObserver(io.unixEventPort, abortEvent,
          kj::UnixEventPort::FdObserver::OBSERVE_READ);

      NbdVolumeAdapter volumeAdapter(
          io.lowLevelProvider->wrapSocketFd(userEnd,
              kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
              kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK),
          kj::mv(volume));
      volumeAdapter.run().exclusiveJoin(cancelObserver.whenBecomesReadable())
          .wait(io.waitScope);
    });

    // Ensure thread gets canceled before its destructor is called.
    KJ_ON_SCOPE_FAILURE(writeEvent(abortEvent, 1));

    NbdDevice device;
    NbdBinding binding(device, kj::mv(kernelEnd));

    KJ_DBG(device.getPath());

    // TODO(soon): We potentially have to format the device before we mount it.
//    Mount mount(device.getPath(), mountPoint, options);

    pid_t child;
    KJ_SYSCALL(child = fork());
    if (child == 0) {
      KJ_DEFER(_exit(1));
      KJ_SYSCALL(prctl(PR_SET_PDEATHSIG, SIGHUP));
      auto args = kj::heapArray<char*>(command.size() + 1);
      for (auto i: kj::indices(command)) {
        args[i] = const_cast<char*>(command[i].cStr());  // evecvp() is not const-correct. :(
      }
      args[command.size()] = nullptr;
      KJ_SYSCALL(execvp(command[0].cStr(), args.begin()));
    }

    int status;
    KJ_SYSCALL(waitpid(child, &status, 0));
    if (WIFEXITED(status)) {
      int code = WEXITSTATUS(status);
      if (code == 0) {
        context.exit();
      } else {
        context.exitError(kj::str("child exited with error code: ", code));
      }
    } else if (WIFSIGNALED(status)) {
      context.exitError(kj::str("child killed by signal: ", strsignal(WTERMSIG(status))));
    } else {
      context.exitError(kj::str("invalid child exit status? status = ", status));
      return 1;
    }
  }
};

}  // namespace
}  // namespace blackrock

KJ_MAIN(blackrock::NbdLoopbackMain);
