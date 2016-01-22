// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "frontend.h"
#include <grp.h>
#include <signal.h>
#include <sandstorm/version.h>
#include <sandstorm/spk.h>
#include <sys/mount.h>
#include <sys/sendfile.h>
#include <sys/syscall.h>
#include <sys/prctl.h>
#include <sodium/randombytes.h>
#include <unistd.h>
#include <limits.h>
#include "bundle.h"

namespace blackrock {

class FrontendImpl::BackendImpl: public sandstorm::Backend::Server {
public:
  BackendImpl(FrontendImpl& frontend, kj::Timer& timer,
              sandstorm::SandstormCoreFactory::Client&& sandstormCoreFactory)
      : frontend(frontend), timer(timer), coreFactory(kj::mv(sandstormCoreFactory)) {}

protected:
  kj::Promise<void> ping(PingContext context) override {
    return kj::READY_NOW;
  }

  kj::Promise<void> startGrain(StartGrainContext context) override {
    auto params = context.getParams();
    auto packageId = params.getPackageId();
    auto grainId = params.getGrainId();
    KJ_LOG(INFO, "Backend: startGrain", grainId, packageId);

    sandstorm::SandstormCore::Client core = ({
      auto req = coreFactory.getSandstormCoreRequest();
      req.setGrainId(grainId);
      req.send().getCore();
    });

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    // Load the package volume.
    auto packageVolume = ({
      auto req = storage.getRequest<Assignable<PackageStorage>>();
      req.setName(kj::str("package-", packageId));
      req.send().getObject().castAs<OwnedAssignable<PackageStorage>>()
          .getRequest().send().getValue().getVolume();
    });

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
        req.setGrainIdForLogging(grainId);
        req.setCore(core);
        req.send();
      });

      context.getResults(capnp::MessageSize { 4, 1 }).setSupervisor(promise.getGrain());
      auto grainState = promise.getGrainState();

      // Update owner.
      return addGrainToUser(kj::mv(ownerGet), grainId, kj::mv(grainState));
    } else {
      return ownerGet.then(
          [this,context,params,packageId,grainId,
           KJ_MVCAP(storageFactory),KJ_MVCAP(packageVolume),KJ_MVCAP(core)]
          (auto&& getResults) mutable {
        for (auto grainInfo: getResults.getValue().getGrains()) {
          if (grainInfo.getId() == grainId) {
            // This is the grain we're looking for.

            // TODO(perf): It would be cool to return a promise for the supervisor without waiting
            //   for continueGrain() to finish, but we need `packageId` and `command` to stay
            //   live in the continueGrain() loop. Make a copy?
            return continueGrain({grainInfo.getState(), kj::mv(storageFactory),
                    kj::mv(packageVolume), packageId, grainId, params.getCommand(),
                    kj::mv(core)})
                .then([context](sandstorm::Supervisor::Client supervisor) mutable {
              context.getResults(capnp::MessageSize { 4, 1 })
                  .setSupervisor(kj::mv(supervisor));
            });
          }
        }
        KJ_FAIL_REQUIRE("no such grain", grainId);
      });
    }
  }

  kj::Promise<void> getGrain(GetGrainContext context) override {
    auto params = context.getParams();
    auto grainId = params.getGrainId();
    KJ_LOG(INFO, "Backend: getGrain", grainId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();

    auto owner = ({
      auto userObjectName = kj::str("user-", params.getOwnerId());

      auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
      req.setName(userObjectName);
      req.initDefaultValue();
      req.send().getObject();
    });

    return owner.getRequest().send()
        .then([params,grainId,context](auto&& getResults) mutable -> kj::Promise<void> {
      auto userInfo = getResults.getValue();

      for (auto grain: userInfo.getGrains()) {
        if (grain.getId() == grainId) {
          return grain.getState().getRequest().send()
              .then([context](auto&& response) mutable -> kj::Promise<void> {
            auto grainState = response.getValue();
            if (grainState.isActive()) {
              auto supervisor = grainState.getActive();
              context.releaseParams();

              return supervisor.keepAliveRequest().send()
                  .then([KJ_MVCAP(supervisor),context](auto) mutable -> kj::Promise<void> {
                context.getResults(capnp::MessageSize {4, 1}).setSupervisor(kj::mv(supervisor));
                return kj::READY_NOW;
              }, [](kj::Exception&& e) -> kj::Promise<void> {
                // Threw exception. Assume dead.
                return KJ_EXCEPTION(DISCONNECTED, "grain supervisor is dead");
              });
            } else {
              // Not currently active.
              return KJ_EXCEPTION(DISCONNECTED, "grain is inactive");
            }
          });
        }
      }

      return KJ_EXCEPTION(FAILED, "no such grain");
    });
  }

  kj::Promise<void> deleteGrain(DeleteGrainContext context) override {
    auto params = context.getParams();
    auto grainId = params.getGrainId();
    KJ_LOG(INFO, "Backend: deleteGrain", grainId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();

    auto owner = ({
      auto userObjectName = kj::str("user-", params.getOwnerId());

      auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
      req.setName(userObjectName);
      req.initDefaultValue();
      req.send().getObject();
    });

    return owner.getRequest().send()
        .then([grainId](auto&& getResults) -> kj::Promise<void> {
      auto userInfo = getResults.getValue();

      capnp::MallocMessageBuilder temp;
      temp.setRoot(userInfo);
      auto userInfoCopy = temp.getRoot<AccountStorage>();
      auto grainsOrphan = userInfoCopy.disownGrains();

      auto listBuilder = grainsOrphan.get();
      bool found = false;
      for (auto i: kj::indices(listBuilder)) {
        if (listBuilder[i].getId().asReader() == grainId) {
          if (i < listBuilder.size() - 1) {
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

  kj::Promise<void> transferGrain(TransferGrainContext context) override {
    // Ownership transfer at the storage level is not currently supported, so instead we do a
    // backup followed by a restore.
    //
    // TODO(cleanup): TODO(perf): This is pretty awful. Support ownership transfer at the storage
    //   level, please.

    auto params = context.getParams();
    auto grainId = params.getGrainId();
    auto oldOwnerId = params.getOwnerId();
    auto newOwnerId = params.getNewOwnerId();
    KJ_LOG(INFO, "Backend: transferGrain", grainId, oldOwnerId, newOwnerId);

    byte random[16];
    randombytes(random, sizeof(random));
    auto backupId = sandstorm::hexEncode(random);

    auto backup = thisCap().backupGrainRequest();
    backup.setBackupId(backupId);
    backup.setOwnerId(oldOwnerId);
    backup.setGrainId(grainId);
    // Don't care about GrainInfo.

    return backup.send().then([this,KJ_MVCAP(backupId),grainId,newOwnerId](auto&&) mutable {
      auto restore = thisCap().restoreGrainRequest();
      restore.setBackupId(backupId);
      restore.setOwnerId(newOwnerId);
      restore.setGrainId(grainId);

      auto cleanup = kj::heap([this,KJ_MVCAP(backupId),grainId]() {
        auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
        auto req1 = thisCap().deleteBackupRequest();
        req1.setBackupId(backupId);
        promises.add(req1.send().then([](auto&&) {}));
        auto req2 = thisCap().deleteGrainRequest();
        req2.setGrainId(grainId);
        promises.add(req2.send().then([](auto&&) {}));
        return kj::joinPromises(promises.finish());
      });

      auto& cleanupRef = *cleanup;

      return restore.send().then([this,&cleanupRef](auto&&) mutable {
        return cleanupRef();
      }, [this,&cleanupRef](kj::Exception&& exception) mutable {
        // Exception during restore. Still need to clean up.
        // TODO(cleanup): kj::Promise should have a "finally()" method, I guess. (We can't use the
        //   usual approach of attach()ing a kj::Defer here because the cleanup is asynchronous.)
        kj::Exception& exceptionRef = exception;
        return cleanupRef().then([&exceptionRef]() mutable {
          return kj::Promise<void>(kj::mv(exceptionRef));
        }, [&exceptionRef](auto&&) mutable {
          return kj::Promise<void>(kj::mv(exceptionRef));
        }).attach(kj::mv(exception));
      }).attach(kj::mv(cleanup));
    });
  }

  kj::Promise<void> deleteUser(DeleteUserContext context) override {
    auto params = context.getParams();
    auto userId = params.getUserId();
    KJ_LOG(INFO, "Backend: deleteUser", userId);

    auto userObjectName = kj::str("user-", userId);
    context.releaseParams();

    auto req = frontend.storageRoots->chooseOne().removeRequest();
    req.setName(userObjectName);
    return req.send().then([](auto&&){});
  }

  // ---------------------------------------------------------------------------

  kj::Promise<void> installPackage(InstallPackageContext context) override {
    KJ_LOG(INFO, "Backend: installPackage");

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

  kj::Promise<void> tryGetPackage(TryGetPackageContext context) override {
    auto packageId = context.getParams().getPackageId();
    KJ_LOG(INFO, "Backend: tryGetPackage", packageId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    auto req = storage.tryGetRequest<Assignable<PackageStorage>>();
    req.setName(kj::str("package-", packageId));
    context.releaseParams();

    return req.send().then([this,context](auto&& outerResult) mutable -> kj::Promise<void> {
      if (outerResult.hasObject()) {
        // Yay, the package exists. Extract the metadata.
        return outerResult.getObject().template castAs<OwnedAssignable<PackageStorage>>()
            .getRequest().send().then([this,context](auto&& innerResult) mutable {
          auto value = innerResult.getValue();
          auto resultBuilder = context.getResults(value.totalSize());
          resultBuilder.setAppId(value.getAppId());
          resultBuilder.setManifest(value.getManifest());
          if (value.hasAuthorPgpKeyFingerprint()) {
            resultBuilder.setAuthorPgpKeyFingerprint(value.getAuthorPgpKeyFingerprint());
          } else {
            // For backwards-compatibility reasons, check if a PGP signature is present and
            // validate it now. (Apps that were installed before PGP key checking was implemented,
            // but already had PGP keys, will need this. Eventually this will become dead code,
            // though. Note that this is dependent on gpg being present on frontend machines,
            // which we don't plan to be the case long-term.)
            auto manifest = value.getManifest();
            auto metadata = manifest.getMetadata();
            auto author = metadata.getAuthor();
            if (author.hasPgpSignature()) {
              KJ_IF_MAYBE(fp, checkPgpSignatureInBundle(value.getAppId(), metadata)) {
                resultBuilder.setAuthorPgpKeyFingerprint(*fp);
              }
            }
          }
        });
      } else {
        // Package doesn't exist. Leave response null.
        return kj::READY_NOW;
      }
    });
  }

  kj::Promise<void> deletePackage(DeletePackageContext context) override {
    auto packageId = context.getParams().getPackageId();
    KJ_LOG(INFO, "Backend: deletePackage", packageId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    auto req = storage.removeRequest();
    req.setName(kj::str("package-", packageId));
    context.releaseParams();
    return req.send().then([](auto) {});
  }

  // ---------------------------------------------------------------------------

  kj::Promise<void> backupGrain(BackupGrainContext context) override {
    auto params = context.getParams();
    auto grainId = params.getGrainId();
    auto backupId = params.getBackupId();
    KJ_LOG(INFO, "Backend: backupGrain", grainId, backupId);

    Worker::Client worker = frontend.workers->chooseOne();
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
    req.setName(kj::str("user-", params.getOwnerId()));
    req.initDefaultValue();
    return req.send().getObject().getRequest().send().then(
        [this,context,params,grainId,backupId,
         KJ_MVCAP(worker),KJ_MVCAP(storage),KJ_MVCAP(storageFactory)]
        (auto&& getResults) mutable {
      for (auto grainInfo: getResults.getValue().getGrains()) {
        if (grainInfo.getId() == grainId) {
          // This is the grain we're looking for!

          // Get a snapshot of the volume for use during the backup process. If the grain is
          // running then we'll make sure to tell it to sync first.
          Volume::Client volume = grainInfo.getState().getRequest().send()
              .then([](auto&& results) -> kj::Promise<Volume::Client> {
            auto state = results.getValue();
            auto getVolume = [KJ_MVCAP(results),state]() {
              return state.getVolume().pauseRequest().send().getSnapshot();
            };

            if (state.isActive()) {
              // Grain is running. Sync its storage to improve the backup reliability.
              return state.getActive().syncStorageRequest().send()
                  .then([](auto&&) {
                // Success, continue on.
              }, [](kj::Exception&& exception) {
                if (exception.getType() == kj::Exception::Type::DISCONNECTED) {
                  // Must have shut down. No problem, carry on.
                } else {
                  KJ_LOG(ERROR, "syncStorage failed", exception);
                }
              }).then(kj::mv(getVolume));
            } else {
              return getVolume();
            }
          });

          // Make request to the Worker to pack this backup.
          auto metadata = params.getInfo();
          auto sizeHint = metadata.totalSize();
          sizeHint.wordCount += 8;
          sizeHint.capCount += 2;
          auto req = worker.packBackupRequest(sizeHint);
          req.setVolume(kj::mv(volume));
          req.setMetadata(metadata);
          req.setStorage(kj::mv(storageFactory));
          return req.send().then([this,backupId,KJ_MVCAP(storage)](auto&& response) mutable {
            auto req2 = storage.setRequest<Blob>(capnp::MessageSize {4, 1});
            req2.setName(kj::str("backup-", backupId));
            req2.setObject(response.getData());
            return req2.send().then([](auto&&) {});
          });
        }
      }
      KJ_FAIL_REQUIRE("no such grain", grainId);
    });
  }

  kj::Promise<void> restoreGrain(RestoreGrainContext context) override {
    auto params = context.getParams();
    auto grainId = params.getGrainId();
    auto backupId = params.getBackupId();
    KJ_LOG(INFO, "Backend: restoreGrain", grainId, backupId);

    Worker::Client worker = frontend.workers->chooseOne();
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    auto blob = ({
      auto req = storage.getRequest<Blob>();
      req.setName(kj::str("backup-", backupId));
      req.send().getObject().castAs<Blob>();
    });

    auto req = worker.unpackBackupRequest();
    req.setData(kj::mv(blob));
    req.setStorage(storageFactory);

    return req.send().then([this,context,params,grainId,KJ_MVCAP(storage),KJ_MVCAP(storageFactory)]
                           (auto&& response) mutable {
      auto grainState = ({
        auto req = storageFactory.newAssignableRequest<GrainState>();
        auto state = req.initInitialValue();
        state.setInactive();
        state.setVolume(response.getVolume());
        req.send().getAssignable();
      });

      auto ownerGet = ({
        auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
        req.setName(kj::str("user-", params.getOwnerId()));
        req.initDefaultValue();
        req.send().getObject().getRequest().send();
      });

      auto metadata = response.getMetadata();
      auto sizeHint = metadata.totalSize();
      sizeHint.wordCount += 4;
      context.getResults(sizeHint).setInfo(metadata);

      return addGrainToUser(kj::mv(ownerGet), grainId, kj::mv(grainState));
    });
  }

  kj::Promise<void> uploadBackup(UploadBackupContext context) override {
    auto backupId = context.getParams().getBackupId();
    KJ_LOG(INFO, "Backend: uploadBackup", backupId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();
    StorageFactory::Client storageFactory = storage.getFactoryRequest().send().getFactory();

    auto upload = storageFactory.uploadBlobRequest().send();

    auto req = storage.setRequest<Blob>();
    req.setName(kj::str("backup-", backupId));
    req.setObject(upload.getBlob());
    context.releaseParams();

    // TODO(perf): Pipelining could be improved here by wrapping the returned stream such that
    //   it only requires the StorageRoot.set() request to complete when done() is called. Probably
    //   not worth the effort, though.
    context.getResults(capnp::MessageSize {4,1}).setStream(upload.getStream());
    return req.send().then([](auto&&) {});
  }

  kj::Promise<void> downloadBackup(DownloadBackupContext context) override {
    auto params = context.getParams();
    auto stream = params.getStream();
    auto backupId = params.getBackupId();
    KJ_LOG(INFO, "Backend: downloadBackup", backupId);

    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();

    auto req = storage.getRequest<Blob>();
    req.setName(kj::str("backup-", backupId));
    context.releaseParams();

    auto req2 = req.send().getObject().castAs<Blob>().writeToRequest();
    req2.setStream(kj::mv(stream));
    return req2.send().then([](auto&&) {});
  }

  kj::Promise<void> deleteBackup(DeleteBackupContext context) override {
    auto backupId = context.getParams().getBackupId();
    KJ_LOG(INFO, "Backend: deleteBackup", backupId);

    auto req = frontend.storageRoots->chooseOne().removeRequest();
    req.setName(kj::str("backup-", backupId));
    context.releaseParams();
    return req.send().then([](auto&&) {});
  }

  // ---------------------------------------------------------------------------

  kj::Promise<void> getUserStorageUsage(GetUserStorageUsageContext context) override {
    StorageRootSet::Client storage = frontend.storageRoots->chooseOne();

    auto owner = ({
      auto userObjectName = kj::str("user-", context.getParams().getUserId());
      context.releaseParams();

      auto req = storage.getOrCreateAssignableRequest<AccountStorage>();
      req.setName(userObjectName);
      req.initDefaultValue();
      req.send().getObject();
    });

    return owner.getStorageUsageRequest().send().then([context](auto&& result) mutable {
      context.getResults(capnp::MessageSize { 4, 0 }).setSize(result.getTotalBytes());
    });
  }

private:
  FrontendImpl& frontend;
  kj::Timer& timer;
  sandstorm::SandstormCoreFactory::Client coreFactory;

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
        auto appId = results.getAppId();
        auto manifest = results.getManifest();

        auto packageStorage = ({
          auto req = storage.getFactoryRequest(capnp::MessageSize {4,0}).send().getFactory()
              .newAssignableRequest<PackageStorage>();
          auto value = req.initInitialValue();
          value.setVolume(results.getVolume());
          value.setAppId(appId);
          value.setManifest(manifest);
          if (results.hasAuthorPgpKeyFingerprint()) {
            value.setAuthorPgpKeyFingerprint(results.getAuthorPgpKeyFingerprint());
          }
          req.send().getAssignable();
        });

        auto promise = ({
          auto req = storage.setRequest<Assignable<PackageStorage>>();
          req.setName(kj::str("package-", packageId));
          req.setObject(kj::mv(packageStorage));
          req.send();
        });

        context.releaseParams();
        auto outerResults = context.getResults();
        outerResults.setAppId(appId);
        outerResults.setManifest(manifest);
        if (results.hasAuthorPgpKeyFingerprint()) {
          outerResults.setAuthorPgpKeyFingerprint(results.getAuthorPgpKeyFingerprint());
        }

        return promise.then([](auto&&) {});
      });
    }

  private:
    StorageRootSet::Client storage;
    Worker::PackageUploadStream::Client inner;
  };

  kj::Promise<void> addGrainToUser(
      capnp::RemotePromise<sandstorm::Assignable<AccountStorage>::GetResults> ownerGet,
      capnp::Text::Reader grainId, OwnedAssignable<GrainState>::Client grainState) {
    // Add a new grain to a user account's grain list.

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
  }

  struct ContinueParams {
    sandstorm::Assignable<GrainState>::Client grainAssignable;
    StorageFactory::Client storageFactory;
    Volume::Client packageVolume;
    capnp::Text::Reader packageId;
    capnp::Text::Reader grainIdForLogging;
    sandstorm::spk::Manifest::Command::Reader command;
    sandstorm::SandstormCore::Client core;
  };

  kj::Promise<sandstorm::Supervisor::Client> continueGrain(
      ContinueParams params, uint retryCount = 0) {
    // Try to continue a grain that already exists. Called as part of startGrain() but may recurse.

    if (retryCount >= 4) {
      KJ_LOG(ERROR, "couldn't start grain due to repeated optimistic concurrency failure");
      return KJ_EXCEPTION(DISCONNECTED, "couldn't start grain");
    }

    auto promise = params.grainAssignable.getRequest().send();
    return promise.then([this,KJ_MVCAP(params),retryCount](auto grainGetResult) mutable
                        -> kj::Promise<sandstorm::Supervisor::Client> {
      auto grainState = grainGetResult.getValue();

      switch (grainState.which()) {
        case GrainState::INACTIVE: {
          // Grain is not running. Start it.

          // TODO(integrity): We ought to somehow "claim" the grain here before we call
          //   volume.getExclusive() so that concurrent claims don't interfere. This probably
          //   requires a "starting" state in addition to active/inactive. Unfortunately we can't
          //   just store a promise for the supervisor into the "active" field now because the
          //   set() will not complete until the promise resolves, and we don't know that there
          //   is no conflict until the set() completes.

          auto volume = grainState.getVolume();
          Worker::Client worker = frontend.workers->chooseOne();

          auto req = worker.restoreGrainRequest();
          auto packageInfo = req.initPackage();
          // TODO(perf): parse ID hex to bytes? Be sure to update worker.c++ which logs
          //   id.asChars() in some places.
          packageInfo.setId(params.packageId.asBytes());
          packageInfo.setVolume(kj::mv(params.packageVolume));
          req.setCommand(params.command);
          req.setStorage(kj::mv(params.storageFactory));
          req.setGrainState(grainState);
          req.setExclusiveGrainStateSetter(grainGetResult.getSetter());
          req.setExclusiveVolume(volume.getExclusiveRequest().send().getExclusive());
          req.setGrainIdForLogging(params.grainIdForLogging);
          req.setCore(params.core);

          return req.send().getGrain();
        }

        case GrainState::ACTIVE: {
          // Attempt to keep-alive the old supervisor.
          auto supervisor = grainState.getActive();
          auto promise = timer.timeoutAfter(4 * kj::SECONDS, supervisor.keepAliveRequest().send());
          return promise.then([KJ_MVCAP(supervisor)](auto&&) mutable
              -> kj::Promise<sandstorm::Supervisor::Client> {
            // Keep-alive succeeded. Use existing supervisor.
            return kj::mv(supervisor);
          }, [this,KJ_MVCAP(params),KJ_MVCAP(grainGetResult),grainState,retryCount]
             (kj::Exception&& e) mutable
              -> kj::Promise<sandstorm::Supervisor::Client> {
            // Keep-alive failed. Possibilities:
            // 1. The grain is in the midst of shutting down. The supervisor has already exited
            //    but we're still waiting for clean unmount. We should pause for a short time to
            //    give it a chance to clean up.
            // 2. The worker died while the grain was running and as a result never managed to
            //    update the grain state to reflect that it is no longer live. We can simply take
            //    ownership.
            // 3. The worker is unhealthy but still executing. This state is dangerous, since we
            //    cannot support concurrent access to the same underlying volume. Luckily each
            //    grain takes out an "exclusive" Volume capability which will disconnect itself
            //    the moment we attempt to take over. We may lose some data that was not yet
            //    written, but given that the worker appears unhealthy that data was probably
            //    in bad shape already.
            //
            // To cover all cases, waiting a few seconds should be sufficient.
            //
            // TODO(integrity): We could handle case (1) more precisely by observing the GrainState,
            //   waiting for the previous worker to set it to `inactive`. Then we could pause much
            //   longer, so that we don't interrupt an unmount that has a lot of data to flush.
            //   This should be exceedingly rare, though. In fact, this whole branch is rare.
            KJ_LOG(INFO, "RARE: GrainState is active, but supervisor appears dead.", e);

            return timer.afterDelay(4 * kj::SECONDS)
                .then([KJ_MVCAP(grainGetResult),grainState]() {
              // OK, a few seconds have gone by. Let's attempt to switch this grain's state to
              // "inactive".
              auto setter = grainGetResult.getSetter();
              auto sizeHint = grainState.totalSize();
              sizeHint.wordCount += 16;
              auto req = setter.setRequest(sizeHint);
              req.setValue(grainState);

              // TODO(cleanup): Calling setInactive() doesn't actually remove the `active` pointer;
              //   it just sets the union discriminant to "inactive". Unfortunately the storage
              //   server just sees the pointers. Is this a bug in Cap'n Proto? It seems
              //   challenging to fix, except perhaps by introducing the native-mutable-objects
              //   idea.
              req.getValue().disownActive();

              req.getValue().setInactive();
              return req.send().then([](auto) -> kj::Promise<void> {
                // successfully updated
                return kj::READY_NOW;
              }, [](kj::Exception&& e) -> kj::Promise<void> {
                if (e.getType() == kj::Exception::Type::DISCONNECTED) {
                  // Concurrent modification blocked our update. That's fine, we were about to
                  // start over anyway.
                  return kj::READY_NOW;
                } else {
                  // Other exception.
                  return kj::mv(e);
                }
              });
            }).then([this,KJ_MVCAP(params),retryCount]() mutable {
              // OK, try again now.
              return continueGrain(kj::mv(params), retryCount + 1);
            });
          });
        }
      }
      KJ_UNREACHABLE;
    });
  }
};

// =======================================================================================

struct FrontendImpl::MongoInfo {
  SimpleAddress address;
  kj::String username;
  kj::String password;

  explicit MongoInfo(Mongo::GetConnectionInfoResults::Reader reader)
      : address(reader.getAddress()),
        username(kj::str(reader.getUsername())),
        password(kj::str(reader.getPassword())) {}

  kj::String toString() {
    return kj::str(username, ':', password, '@', address);
  }
};

FrontendImpl::FrontendImpl(kj::Network& network, kj::Timer& timer,
                           sandstorm::SubprocessSet& subprocessSet,
                           FrontendConfig::Reader config, uint replicaNumber,
                           kj::PromiseFulfillerPair<sandstorm::Backend::Client> paf)
    : timer(timer),
      subprocessSet(subprocessSet),
      capnpServer(kj::mv(paf.promise)),
      storageRoots(kj::refcounted<BackendSetImpl<StorageRootSet>>()),
      storageFactories(kj::refcounted<BackendSetImpl<StorageFactory>>()),
      workers(kj::refcounted<BackendSetImpl<Worker>>()),
      mongos(kj::refcounted<BackendSetImpl<Mongo>>()),
      tasks(*this) {
  paf.fulfiller->fulfill(kj::heap<BackendImpl>(*this, timer,
      capnpServer.getBootstrap().castAs<sandstorm::SandstormCoreFactory>()));

  setConfig(config);

  kj::StringPtr socketPath = sandstorm::Backend::SOCKET_PATH;
  KJ_ASSERT(socketPath.startsWith("/var/"));
  kj::String outsideSandboxSocketPath =
      kj::str("/var/blackrock/bundle", socketPath.slice(strlen("/var")));
  sandstorm::recursivelyCreateParent(outsideSandboxSocketPath);
  unlink(outsideSandboxSocketPath.cStr());

  auto mongoInfoPromise = mongos->chooseOne().getConnectionInfoRequest().send();

  auto promise = network.parseAddress(kj::str("unix:", outsideSandboxSocketPath));
  tasks.add(promise.then([this,KJ_MVCAP(outsideSandboxSocketPath),KJ_MVCAP(mongoInfoPromise),
                          replicaNumber](kj::Own<kj::NetworkAddress>&& addr) mutable {
    return mongoInfoPromise
        .then([this,KJ_MVCAP(addr),KJ_MVCAP(outsideSandboxSocketPath),replicaNumber]
              (auto&& mongoInfo) mutable {
      auto listener = addr->listen();
      KJ_SYSCALL(chown(outsideSandboxSocketPath.cStr(), 0, 1000));
      KJ_SYSCALL(chmod(outsideSandboxSocketPath.cStr(), 0770));

      // Now that we're listening on the socket, and we have Mongo's address, it's safe to start
      // the front-end.
      uint n = this->config.getReplicasPerMachine();
      for (uint i = 0; i < n; i++) {
        tasks.add(execLoop(MongoInfo(mongoInfo), replicaNumber * n + i, 6080 + i));
      }

      return capnpServer.listen(kj::mv(listener));
    });
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
BackendSet<Mongo>::Client FrontendImpl::getMongoBackendSet() {
  return kj::addRef(*mongos);
}

kj::Promise<void> FrontendImpl::execLoop(MongoInfo&& mongoInfo, uint replicaNumber, uint port) {
  // If node fails, we will wait until at least 10 seconds from now before restarting it.
  auto rateLimit = timer.afterDelay(10 * kj::SECONDS);

  auto promise = kj::evalNow([&]() {
    KJ_LOG(INFO, "starting node...");

    createSandstormDirectories();

    sandstorm::Subprocess subprocess([&]() -> int {
      enterSandstormBundle();

      // Set up environment.
      KJ_SYSCALL(setenv("ROOT_URL", config.getBaseUrl().cStr(), true));
      KJ_SYSCALL(setenv("PORT", kj::str(port).cStr(), true));
      KJ_SYSCALL(setenv("MONGO_URL",
          kj::str("mongodb://", mongoInfo, "/meteor?authSource=admin").cStr(), true));
      KJ_SYSCALL(setenv("MONGO_OPLOG_URL",
          kj::str("mongodb://", mongoInfo, "/local?authSource=admin").cStr(), true));
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
          ", \"hideTroubleshooting\": true",
          ", \"wildcardHost\":\"", config.getWildcardHost(), "\""
          ", \"quotaEnabled\": true"
          ", \"stripePublicKey\":\"", config.getStripePublicKey(), "\""
          ", \"allowUninvited\":", config.getAllowUninvited() ? "true" : "false",
          "}"
          ", \"stripeKey\":\"", config.getStripeKey(), "\""
          ", \"mailchimpKey\":\"", config.getMailchimpKey(), "\""
          ", \"mailchimpListId\":\"", config.getMailchimpListId(), "\""
          ", \"replicaNumber\":", replicaNumber,
          "}").cStr(), true));

      // Execute!
      KJ_SYSCALL(execl("bin/node", "bin/node", "main.js", (char*)nullptr));
      KJ_UNREACHABLE;
    });

    frontendPid = subprocess.getPid();

    return subprocessSet.waitForSuccess(kj::mv(subprocess));
  });
  return promise.then([]() {
    KJ_FAIL_ASSERT("frontend exited 'successfully' (shouldn't happen); restarting");
  }).catch_([KJ_MVCAP(rateLimit)](kj::Exception&& exception) mutable {
    KJ_LOG(ERROR, "frontend died; restarting", exception);
    return kj::mv(rateLimit);
  }).then([this,KJ_MVCAP(mongoInfo),replicaNumber, port]() mutable {
    return execLoop(kj::mv(mongoInfo), replicaNumber, port);
  });
}

// =======================================================================================
// Mongo harness.
//
// This is in frontend.c++ mostly because it shares a bunch of code related to using the
// Sandstorm bundle, and because the Frontend is the only thing that uses Mongo.

MongoImpl::MongoImpl(
    kj::Timer& timer, sandstorm::SubprocessSet& subprocessSet, SimpleAddress bindAddress,
    kj::PromiseFulfillerPair<void> passwordPaf)
    : timer(timer), subprocessSet(subprocessSet), bindAddress(bindAddress),
      passwordPromise(passwordPaf.promise.fork()),
      execTask(startExecLoop(kj::mv(passwordPaf.fulfiller))) {}

kj::Promise<void> MongoImpl::getConnectionInfo(GetConnectionInfoContext context) {
  return passwordPromise.addBranch().then([this,context]() mutable {
    auto results = context.getResults();
    bindAddress.copyTo(results.initAddress());
    results.setUsername("sandstorm");
    results.setPassword(KJ_ASSERT_NONNULL(password));
  });
}

kj::Promise<void> MongoImpl::startExecLoop(kj::Own<kj::PromiseFulfiller<void>> passwordFulfiller) {
  auto promise = execLoop(*passwordFulfiller);
  return promise.attach(kj::mv(passwordFulfiller));
}

kj::Promise<void> MongoImpl::execLoop(kj::PromiseFulfiller<void>& passwordFulfiller) {
  // If mongo fails, we will wait until at least 10 seconds from now before restarting it.
  auto rateLimit = timer.afterDelay(10 * kj::SECONDS);

  return kj::evalNow([&]() {
    createSandstormDirectories();

    sandstorm::Subprocess subprocess([&]() -> int {
      enterSandstormBundle();

      KJ_SYSCALL(execl("bin/mongod", "bin/mongod", "--fork",
          "--bind_ip", kj::str("127.0.0.1,", bindAddress.toStringWithoutPort()).cStr(),
          "--port", kj::str(bindAddress.getPort()).cStr(),
          "--dbpath", "/var/mongo", "--logpath", "/var/log/mongo.log",
          "--pidfilepath", "/var/pid/mongo.pid",
          "--auth", "--nohttpinterface", "--replSet", "ssrs", "--oplogSize", "128",
          (char*)nullptr));
      KJ_UNREACHABLE;
    });

    // Wait for mongod to return, meaning the database is up.  Then get its real pid via the
    // pidfile.
    return subprocessSet.waitForSuccess(kj::mv(subprocess));
  }).then([this,&passwordFulfiller]() {
    pid_t pid = KJ_ASSERT_NONNULL(sandstorm::parseUInt(sandstorm::trim(
        sandstorm::readAll("/var/blackrock/bundle/pid/mongo.pid")), 10));

    // Check if mongo pid still exists. If not, it's possible that we already received its
    // termination signal and so it's too late to adopt it formally.
    KJ_SYSCALL(kill(pid, 0), "mongod dead on arrival");

    // OK, the pid is live, so let's adopt it. Note that the reason this works is because we are
    // pid 1 of the pid namespace, making us "init" for everything inside, therefore when the mongo
    // starter process exits, the daemon process reparents to us!
    KJ_ASSERT(getpid() == 1);
    sandstorm::Subprocess mongoProc(pid);

    return initializeMongo()
        .then([this,KJ_MVCAP(mongoProc),&passwordFulfiller](kj::String&& pw) mutable {
      password = kj::mv(pw);
      passwordFulfiller.fulfill();
      return subprocessSet.waitForSuccess(kj::mv(mongoProc));
    });
  }).then([this]() {
    KJ_FAIL_ASSERT("mongo exited successfully?");
  }).catch_([KJ_MVCAP(rateLimit)](kj::Exception&& exception) mutable {
    KJ_LOG(ERROR, "mongo died; restarting", exception);
    return kj::mv(rateLimit);
  }).then([this,&passwordFulfiller]() {
    return execLoop(passwordFulfiller);
  });
}

kj::Promise<kj::String> MongoImpl::initializeMongo() {
  bool isNew = access("/var/blackrock/bundle/mongo/passwd", F_OK) < 0;

  return kj::evalNow([&]() {
    if (isNew) {
      // We need to initialize the repl set to get oplog tailing. Our set isn't actually much of a
      // set since it only contains one instance, but you need that for oplog.
      return mongoCommand(kj::str(
          "rs.initiate({_id: 'ssrs', members: [{_id: 0, host: '", bindAddress, "'}]})"));
    } else {
      // It's possible that the bind address has changed, so reconfig the repl set. We have to set
      // {force: true} because if the address changed then Mongo will think it doesn't have a
      // majority (because it can't reach the old address) and will refuse to update the config.
      return mongoCommand(kj::str(
          "rs.reconfig({_id: 'ssrs', members: [{_id: 0, host: '", bindAddress, "'}]}, "
                      "{force: true})"));
    }
  }).then([this]() {
    // We have to wait a few seconds for Mongo to elect itself master of the repl set. Mongo does
    // some sort of heartbeat every second and it takes three of these for Mongo to elect itself,
    // meaning the whole process always takes 2-3 seconds. We'll sleep for 4.
    // TODO(cleanup): This is ugly.
    return timer.afterDelay(4 * kj::SECONDS);
  }).then([this,isNew]() -> kj::Promise<kj::String> {
    if (isNew) {
      // Get 30 random chars for password.
      char passwd[30];
      randombytes_buf(passwd, sizeof(passwd));

      const char* digits =
          "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";

      for (auto& c: passwd) {
        c = digits[byte(c) % 64];
      }

      auto passwdStr = kj::heapString(passwd, sizeof(passwd));

      // Create the mongo user.
      auto command = kj::str(
        "db.createUser({user: \"sandstorm\", pwd: \"", passwdStr, "\", "
        "roles: [\"readWriteAnyDatabase\",\"userAdminAnyDatabase\",\"dbAdminAnyDatabase\","
                "\"clusterAdmin\"]})");
      return mongoCommand(kj::mv(command), "admin").then([KJ_MVCAP(passwdStr)]() mutable {
        // Store the password.
        auto outFd = sandstorm::raiiOpen(
            "/var/blackrock/bundle/mongo/passwd", O_WRONLY | O_CREAT | O_EXCL, 0600);
        KJ_SYSCALL(fchown(outFd, 1000, 1000));
        kj::FdOutputStream((int)outFd).write(passwdStr.begin(), passwdStr.size());
        return kj::mv(passwdStr);
      });
    } else {
      // Read existing password.
      return sandstorm::trim(sandstorm::readAll(
          sandstorm::raiiOpen("/var/blackrock/bundle/mongo/passwd", O_RDONLY)));
    }
  });
}

kj::Promise<void> MongoImpl::mongoCommand(kj::String command, kj::StringPtr dbName) {
  sandstorm::Subprocess subprocess([&]() -> int {
    enterSandstormBundle();

    auto db = kj::str("localhost:", kj::str(bindAddress.getPort()).cStr(), '/', dbName);

    kj::Vector<const char*> args;
    args.add("/bin/mongo");

    // If /var/blackrock/bundle/mongo/passwd exists, we interpret it as containing the password
    // for a Mongo user "sandstorm", and assume we are expected to log in as this user. (If it
    // doesn't exist yet, then probably we haven't actually initialized the database yet.)
    kj::String passwordArg;
    if (access("/var/mongo/passwd", F_OK) == 0) {
      passwordArg = kj::str("--password=", sandstorm::trim(sandstorm::readAll(
          sandstorm::raiiOpen("/var/mongo/passwd", O_RDONLY))));

      args.add("-u");
      args.add("sandstorm");
      args.add(passwordArg.cStr());
      args.add("--authenticationDatabase");
      args.add("admin");
    }

    args.add("--quiet");
    args.add("--eval");
    args.add(command.cStr());

    args.add(db.cStr());
    args.add(nullptr);

    // OK, run the Mongo client!
    KJ_SYSCALL(execv(args[0], const_cast<char**>(args.begin())));

    KJ_UNREACHABLE;
  });

  return subprocessSet.waitForSuccess(kj::mv(subprocess))
      .catch_([KJ_MVCAP(command)](kj::Exception&& exception) -> kj::Promise<void> {
    KJ_LOG(FATAL, "Mongo client command failed! State is inconsistent! Hanging forever!",
        command, exception.getDescription());
    return kj::NEVER_DONE;
  });
}

} // namespace blackrock

