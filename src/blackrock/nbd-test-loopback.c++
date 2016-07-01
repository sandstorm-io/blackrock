// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
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
#include <sys/mount.h>

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
  kj::AutoCloseFd storageDir;
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
    storageDir = sandstorm::raiiOpen(arg.cStr(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
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
    KJ_SYSCALL(mount("none", "/", nullptr, MS_REC | MS_PRIVATE, nullptr));

    NbdDevice::loadKernelModule();
    bool isNew = faccessat(storageDir, "roots/root", F_OK, 0) < 0;

    int pair[2];
    socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, pair);
    kj::AutoCloseFd kernelEnd(pair[0]);
    kj::AutoCloseFd userEnd(pair[1]);

    kj::AutoCloseFd abortEvent = newEventFd(0, EFD_CLOEXEC | EFD_NONBLOCK);

    kj::Thread serverThread([&]() {
      KJ_IF_MAYBE(exception, kj::runCatchingExceptions([&]() {
        auto io = kj::setupAsyncIo();
        StorageRootSet::Client storage = kj::heap<FilesystemStorage>(
            storageDir, io.unixEventPort,
            io.provider->getTimer(), nullptr);

        auto factory = storage.getFactoryRequest().send().getFactory();
        OwnedVolume::Client volume = nullptr;

        if (isNew) {
          volume = factory.newVolumeRequest().send().getVolume();
          auto req2 = factory.newAssignableRequest<OwnedVolume>();
          req2.setInitialValue(volume);

          auto req3 = storage.setRequest<Assignable<OwnedVolume>>();
          req3.setName("root");
          req3.setObject(req2.send().getAssignable());

          req3.send().wait(io.waitScope);
        } else {
          auto req = storage.getRequest<Assignable<OwnedVolume>>();
          req.setName("root");
          volume = req.send().getObject().castAs<OwnedAssignable<OwnedVolume>>()
              .getRequest().send().getValue();
        }

        kj::UnixEventPort::FdObserver cancelObserver(io.unixEventPort, abortEvent,
            kj::UnixEventPort::FdObserver::OBSERVE_READ);

        NbdVolumeAdapter volumeAdapter(
            io.lowLevelProvider->wrapSocketFd(userEnd,
                kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC |
                kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK),
            kj::mv(volume), NbdAccessType::READ_WRITE);
        volumeAdapter.run().exclusiveJoin(cancelObserver.whenBecomesReadable())
            .wait(io.waitScope);
      })) {
        KJ_LOG(FATAL, "nbd server threw exception", *exception);
      }
    });

    // Ensure thread gets canceled before its destructor is called.
    KJ_ON_SCOPE_FAILURE(writeEvent(abortEvent, 1));

    NbdDevice device;
    context.warning(kj::str("using: ", device.getPath()));

    NbdBinding binding(device, kj::mv(kernelEnd), NbdAccessType::READ_WRITE);
    KJ_DEFER(context.warning("unbinding..."));

    if (isNew) {
      context.warning("formatting...");
      device.format();
    }

    context.warning("mounting...");
    KJ_DEFER(device.trimJournalIfClean());
    Mount mount(device.getPath(), mountPoint, 0, options);
    KJ_DEFER(context.warning("unmounting..."));

    KJ_SYSCALL(unshare(CLONE_NEWPID));

    sandstorm::Subprocess(sandstorm::Subprocess::Options(command)).waitForSuccess();

    return true;
  }
};

}  // namespace
}  // namespace blackrock

KJ_MAIN(blackrock::NbdLoopbackMain);
