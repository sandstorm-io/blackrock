// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "frontend.h"
#include <grp.h>
#include <signal.h>
#include <sandstorm/version.h>
#include <sys/mount.h>
#include <sys/sendfile.h>

namespace blackrock {

#define BUNDLE_PATH "/blackrock/frontend"

class FrontendImpl::BackendImpl: public sandstorm::Backend::Server {
public:
  explicit BackendImpl(FrontendImpl& frontend): frontend(frontend) {}

protected:
  kj::Promise<void> startGrain(StartGrainContext context) override {
    auto params = context.getParams();

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    // Load the package volume.
    auto packageId = params.getPackageId();
    auto packageVolume = ({
      auto req = storage.getRequest<Volume>();
      req.setName(kj::str("package-", packageId));
      req.send().getObject().castAs<OwnedVolume>();
    });

    // TODO(security): make `packageVolume` read-only somehow

    // Get the owner user data.
    auto owner = ({
      auto userObjectName = kj::str("user-", params.getOwnerId());

      auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
      req.setName(userObjectName);
      req.initDefaultValue();
      req.send().getObject();
    });

    auto ownerGet = owner.getRequest().send();

    if (params.getIsNew()) {
      Worker::Client worker = frontend.workers->chooseOne();

      auto promise = ({
        auto req = worker.newGrainRequest();
        auto packageInfo = req.initPackage();
        packageInfo.setId(packageId.asBytes());  // TODO(perf): parse ID hex to bytes?
        packageInfo.setVolume(kj::mv(packageVolume));
        req.setCommand(params.getCommand());
        req.setStorage(kj::mv(storageFactory));
        req.send();
      });

      context.getResults(capnp::MessageSize { 4, 1 }).setSupervisor(promise.getGrain());
      auto grainState = promise.getGrainState();
      auto grainId = params.getGrainId();

      // Update owner.
      return ownerGet.then([KJ_MVCAP(grainState),grainId](auto&& getResults) mutable {
        auto userInfo = getResults.getValue();

        // Ugh, extending a list in Cap'n Proto is kind of a pain...
        // We copy the user info to a temporary, and then we use Orphan::truncate() to extend the
        // list by one, and then we fill in the new slot.
        capnp::MallocMessageBuilder temp;
        temp.setRoot(userInfo);
        auto userInfoCopy = temp.getRoot<AccountStorage>();
        auto grainsOrphan = userInfoCopy.disownGrains();
        grainsOrphan.truncate(grainsOrphan.get().size() + 1);
        auto grains = grainsOrphan.get();
        auto newGrainInfo = grains[grains.size() - 1];
        newGrainInfo.setId(grainId);
        newGrainInfo.setState(kj::mv(grainState));
        userInfoCopy.adoptGrains(kj::mv(grainsOrphan));

        // Now we can copy the whole thing back into a set request. (We didn't build it there in
        // the first place because extending a list usually leaves an allocation hole, requiring
        // a copy to GC.)
        auto req = getResults.getSetter().setRequest();
        req.setValue(userInfoCopy);
        return req.send().then([](auto) {});
      });
    } else {
      return ownerGet.then([params](auto&& getResults) {
        auto grainId = params.getGrainId();
        for (auto grainInfo: getResults.getValue().getGrains()) {
          if (grainInfo.getId() == grainId) {
            // This is the grain we're looking for.
            return grainInfo.getState();
          }
        }
        KJ_FAIL_REQUIRE("no such grain", grainId);
      }).then([this,context,packageId,params,KJ_MVCAP(storageFactory),KJ_MVCAP(packageVolume)]
              (auto grainState) mutable {
        auto volume = grainState.getRequest().send().getValue().getVolume();
        Worker::Client worker = frontend.workers->chooseOne();

        auto req = worker.restoreGrainRequest();
        auto packageInfo = req.initPackage();
        packageInfo.setId(packageId.asBytes());  // TODO(perf): parse ID hex to bytes?
        packageInfo.setVolume(kj::mv(packageVolume));
        req.setCommand(params.getCommand());
        req.setStorage(kj::mv(storageFactory));
        req.setGrainState(kj::mv(grainState));
        // TODO(now): Disconnect the volume!
        req.setVolume(kj::mv(volume));

        context.getResults().setSupervisor(req.send().getGrain());
      });
    }
  }

  kj::Promise<void> getGrain(GetGrainContext context) override {
    // TODO(now): implement -- probably requires restorable sturdyrefs. For now, thorwing
    //   disconnected will cause the caller to try startGrain().
    return KJ_EXCEPTION(DISCONNECTED, "getGrain() not implemented");
  }

  kj::Promise<void> deleteGrain(DeleteGrainContext context) override {
    auto params = context.getParams();
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();

    auto owner = ({
      auto userObjectName = kj::str("user-", params.getOwnerId());

      auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
      req.setName(userObjectName);
      req.initDefaultValue();
      req.send().getObject();
    });

    return owner.getRequest().send()
        .then([params](auto&& getResults) -> kj::Promise<void> {
      auto grainId = params.getGrainId();
      auto userInfo = getResults.getValue();

      capnp::MallocMessageBuilder temp;
      temp.setRoot(userInfo);
      auto userInfoCopy = temp.getRoot<AccountStorage>();
      auto grainsOrphan = userInfoCopy.disownGrains();

      auto listBuilder = grainsOrphan.get();
      bool found = false;
      for (auto i: kj::indices(listBuilder)) {
        if (listBuilder[i].getId().asReader() == grainId) {
          if (i == listBuilder.size() - 1) {
            // Copy the last element in the list over this one.
            listBuilder.setWithCaveats(i, listBuilder[listBuilder.size() - 1]);
          }
          found = true;
          break;
        }
      }
      if (found) {
        grainsOrphan.truncate(listBuilder.size() - 1);
        userInfoCopy.adoptGrains(kj::mv(grainsOrphan));

        auto req = getResults.getSetter().setRequest();
        req.setValue(userInfoCopy);
        return req.send().then([](auto) {});
      } else {
        return kj::READY_NOW;
      }
    });
  }

  kj::Promise<void> installPackage(InstallPackageContext context) override {
    Worker::Client worker = frontend.workers->chooseOne();
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    auto stream = ({
      auto req = worker.unpackPackageRequest();
      req.setStorage(kj::mv(storageFactory));
      req.send().getStream();
    });

    context.getResults().setStream(
        kj::heap<PackageUploadStreamImpl>(kj::mv(storage), kj::mv(stream)));
    return kj::READY_NOW;
  }

  kj::Promise<void> getPackage(GetPackageContext context) override {
    // TODO(now): We need to actually mount up the volume and read the manifest. Need a new worker
    //   method for this.
    KJ_UNIMPLEMENTED("getPackage()");
  }

  kj::Promise<void> deletePackage(DeletePackageContext context) override {
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    auto req = storage.removeRequest();
    req.setName(kj::str("package-", context.getParams().getPackageId()));
    context.releaseParams();
    return req.send().then([](auto) {});
  }

private:
  FrontendImpl& frontend;

  class PackageUploadStreamImpl: public sandstorm::Backend::PackageUploadStream::Server {
  public:
    PackageUploadStreamImpl(StorageRootSet::Client storage,
                            Worker::PackageUploadStream::Client inner)
        : storage(kj::mv(storage)), inner(kj::mv(inner)) {}

  protected:
    kj::Promise<void> write(WriteContext context) override {
      auto params = context.getParams();
      auto req = inner.writeRequest(params.totalSize());
      req.setData(params.getData());
      return context.tailCall(kj::mv(req));
    }

    kj::Promise<void> done(DoneContext context) override {
      auto params = context.getParams();
      auto req = inner.doneRequest(params.totalSize());
      return context.tailCall(kj::mv(req));
    }

    kj::Promise<void> expectSize(ExpectSizeContext context) override {
      auto params = context.getParams();
      auto req = inner.expectSizeRequest(params.totalSize());
      req.setSize(params.getSize());
      return context.tailCall(kj::mv(req));
    }

    kj::Promise<void> saveAs(SaveAsContext context) override {
      auto req = inner.getResultRequest(capnp::MessageSize { 8, 0 });
      return req.send().then([this,context](auto&& results) mutable {
        auto packageId = context.getParams().getPackageId();

        auto promise = ({
          auto req = storage.setRequest<Volume>();
          req.setName(kj::str("package-", packageId));
          req.setObject(results.getVolume());
          req.send();
        });

        context.releaseParams();
        auto outerResults = context.getResults();
        outerResults.setAppId(results.getAppId());
        outerResults.setManifest(results.getManifest());

        return promise.then([](auto&&) {});
      });
    }

  private:
    StorageRootSet::Client storage;
    Worker::PackageUploadStream::Client inner;
  };
};

FrontendImpl::FrontendImpl(kj::AsyncIoProvider& ioProvider,
                           sandstorm::SubprocessSet& subprocessSet,
                           FrontendConfig::Reader config)
    : subprocessSet(subprocessSet),
      capnpServer(kj::heap<BackendImpl>(*this)),
      storageRoots(kj::refcounted<BackendSetImpl<StorageRootSet>>()),
      storageFactories(kj::refcounted<BackendSetImpl<StorageFactory>>()),
      workers(kj::refcounted<BackendSetImpl<Worker>>()),
      tasks(*this) {
  setConfig(config);

  sandstorm::recursivelyCreateParent(sandstorm::Backend::SOCKET_PATH);
  unlink(sandstorm::Backend::SOCKET_PATH->cStr());

  tasks.add(ioProvider.getNetwork().parseAddress(kj::str("unix:", sandstorm::Backend::SOCKET_PATH))
      .then([this](kj::Own<kj::NetworkAddress>&& addr) {
    auto listener = addr->listen();

    // Now that we're listening on the socket, it's safe to start the front-end.
    tasks.add(execLoop());

    auto promise = capnpServer.listen(*listener);
    return promise.attach(kj::mv(listener));
  }));
}

void FrontendImpl::taskFailed(kj::Exception&& exception) {
  KJ_LOG(ERROR, exception);
}

void FrontendImpl::setConfig(FrontendConfig::Reader config) {
  configMessage = kj::heap<capnp::MallocMessageBuilder>();
  configMessage->setRoot(config);
  this->config = configMessage->getRoot<FrontendConfig>();
  if (frontendPid != 0) {
    // Restart frontend.
    KJ_LOG(INFO, "restarting front-end due to config change");
    KJ_SYSCALL(kill(frontendPid, SIGTERM));
  }
}

BackendSet<StorageRootSet>::Client FrontendImpl::getStorageRootBackendSet() {
  return kj::addRef(*storageRoots);
}
BackendSet<StorageFactory>::Client FrontendImpl::getStorageFactoryBackendSet() {
  return kj::addRef(*storageFactories);
}
BackendSet<Worker>::Client FrontendImpl::getWorkerBackendSet() {
  return kj::addRef(*workers);
}

kj::Promise<void> FrontendImpl::execLoop() {
  sandstorm::Subprocess subprocess([&]() -> int {
    // We're going to set up a small sandbox in which the front-end will run.

    KJ_SYSCALL(chdir(BUNDLE_PATH));

    // Clear the temp directory.
    if (access("tmp", F_OK) >= 0) {
      sandstorm::recursivelyDelete("tmp");
    }
    KJ_SYSCALL(mkdir("tmp", S_ISVTX | 0777));

    // Enter mount namespace so that we can bind stuff in.
    KJ_SYSCALL(unshare(CLONE_NEWNS));

    // Bind desired devices from /dev into our chroot environment.
    KJ_SYSCALL(mount("/dev/null", "dev/null", nullptr, MS_BIND, nullptr));
    KJ_SYSCALL(mount("/dev/zero", "dev/zero", nullptr, MS_BIND, nullptr));
    KJ_SYSCALL(mount("/dev/random", "dev/random", nullptr, MS_BIND, nullptr));
    KJ_SYSCALL(mount("/dev/urandom", "dev/urandom", nullptr, MS_BIND, nullptr));

    // Mount a tmpfs at /etc and copy over necessary config files from the host.
    KJ_SYSCALL(mount("tmpfs", "etc", "tmpfs", MS_NOSUID | MS_NOEXEC,
                     kj::str("size=2m,nr_inodes=128,mode=755,uid=0,gid=0").cStr()));
    {
      auto files = sandstorm::splitLines(sandstorm::readAll("etc.list"));

      // Now copy over each file.
      for (auto& file: files) {
        if (access(file.cStr(), R_OK) == 0) {
          auto in = sandstorm::raiiOpen(file, O_RDONLY);
          auto out = sandstorm::raiiOpen(kj::str(".", file), O_WRONLY | O_CREAT | O_EXCL);
          ssize_t n;
          do {
            KJ_SYSCALL(n = sendfile(out, in, nullptr, 1 << 20));
          } while (n > 0);
        }
      }
    }

    // chroot into the frontend dir.
    KJ_SYSCALL(chroot(BUNDLE_PATH));
    KJ_SYSCALL(chdir("/"));

    // Drop privileges.
    KJ_SYSCALL(setresgid(1000, 1000, 1000));
    KJ_SYSCALL(setgroups(0, nullptr));
    KJ_SYSCALL(setresuid(1000, 1000, 1000));

    // Clear signal mask.
    sigset_t sigset;
    KJ_SYSCALL(sigemptyset(&sigset));
    KJ_SYSCALL(sigprocmask(SIG_SETMASK, &sigset, nullptr));

    // Set up environment.
    KJ_SYSCALL(setenv("ROOT_URL", config.getBaseUrl().cStr(), true));
    KJ_SYSCALL(setenv("PORT", "6080", true));
    KJ_SYSCALL(setenv("MONGO_URL", config.getMongoUrl().cStr(), true));
    KJ_SYSCALL(setenv("MONGO_OPLOG_URL", config.getMongoOplogUrl().cStr(), true));
    KJ_SYSCALL(setenv("BIND_IP", "0.0.0.0", true));
    if (config.hasMailUrl()) {
      KJ_SYSCALL(setenv("MAIL_URL", config.getMailUrl().cStr(), true));
    }
    if (config.hasDdpUrl()) {
      KJ_SYSCALL(setenv("DDP_DEFAULT_CONNECTION_URL", config.getDdpUrl().cStr(), true));
    }
    kj::String buildstamp;
    if (SANDSTORM_BUILD == 0) {
      buildstamp = kj::str("\"[", sandstorm::trim(sandstorm::readAll("buildstamp")), "]\"");
    } else {
      buildstamp = kj::str(SANDSTORM_BUILD);
    }
    KJ_SYSCALL(setenv("METEOR_SETTINGS", kj::str(
        "{\"public\":{\"build\":", buildstamp,
        ", \"kernelTooOld\": false"
        ", \"allowDemoAccounts\":", config.getAllowDemoAccounts() ? "true" : "false",
        ", \"allowDevAccounts\": false"
        ", \"isTesting\":", config.getIsTesting() ? "true" : "false",
        ", \"wildcardHost\":\"", config.getWildcardHost(), "\"",
        "}}").cStr(), true));

    // Execute!
    KJ_SYSCALL(execl("bin/node", "bin/node", "main.js", (char*)nullptr));
    KJ_UNREACHABLE;
  });

  frontendPid = subprocess.getPid();

  return subprocessSet.waitForSuccess(kj::mv(subprocess)).then([]() {
    KJ_LOG(ERROR, "frontend exited 'successfully' (shouldn't happen); restarting");
  }, [](kj::Exception&& exception) {
    KJ_LOG(ERROR, "frontend died; restarting", exception);
  }).then([this]() {
    return execLoop();
  });
}

} // namespace blackrock

