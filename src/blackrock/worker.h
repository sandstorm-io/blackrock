// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_WORKER_H_
#define BLACKROCK_WORKER_H_

#include "common.h"
#include <blackrock/worker.capnp.h>
#include <kj/main.h>
#include <unordered_map>
#include <sandstorm/util.h>
#include <sandstorm/supervisor.h>
#include <kj/async-io.h>
#include "local-persistent-registry.h"

namespace kj {
  class Thread;
}

namespace blackrock {

class NbdVolumeAdapter;

struct ByteStringHash {
  inline size_t operator()(const kj::ArrayPtr<const byte>& token) const {
    size_t result = 0;
    memcpy(&result, token.begin(), kj::min(sizeof(result), token.size()));
    return result;
  }
  inline size_t operator()(const kj::ArrayPtr<const byte>& a,
                           const kj::ArrayPtr<const byte>& b) const {
    return a.size() == b.size() && memcmp(a.begin(), b.begin(), a.size()) == 0;
  }
};

class PackageMountSet: private kj::TaskSet::ErrorHandler {
public:
  explicit PackageMountSet(kj::AsyncIoContext& ioContext);
  ~PackageMountSet() noexcept(false);
  KJ_DISALLOW_COPY(PackageMountSet);

  class PackageMount: public kj::Refcounted {
  public:
    PackageMount(PackageMountSet& mountSet, kj::ArrayPtr<const byte> id,
                 kj::String path, Volume::Client volume,
                 kj::Own<kj::AsyncIoStream> nbdUserEnd,
                 kj::AutoCloseFd nbdKernelEnd);
    ~PackageMount() noexcept(false);

    kj::ArrayPtr<const byte> getId() { return id; }

    kj::StringPtr getPath() { return path; }

    kj::Promise<void> whenReady() { return loaded.addBranch(); }

    kj::Promise<void> onDisconnected() { return disconnected.addBranch(); }

    void updateVolume(Volume::Client newVolume);

  private:
    friend class PackageMountSet;

    PackageMountSet& mountSet;

    kj::Array<byte> id;
    // ID string assigned to this package.

    kj::String path;

    kj::Own<NbdVolumeAdapter> volumeAdapter;
    kj::Promise<void> volumeRunTask;

    kj::AsyncIoProvider::PipeThread nbdThread;
    // Thread which mounts the NBD device. Protocol as follows:
    // 1) thread -> main: 1 byte: The mount point is ready.
    // 2) main -> thread: EOF: Please shut down.
    // 3) thread -> main: EOF: I've shut down now; it's safe to destroy the NbdVolumeAdapter and
    //                         join the thread.

    kj::ForkedPromise<void> loaded;
    // Resolves when the thread reports that the mount point is active.

    bool unregistered = false;

    kj::ForkedPromise<void> disconnected;
    // Resolves when this mount has been disconnecnted from storage and therefore will report I/O
    // errors. Grains using this package should attempt to shut down.

    void unregister();
  };

  kj::Promise<kj::Own<PackageMount>> getPackage(PackageInfo::Reader package);

  void returnPackage(kj::Own<PackageMount> package);
  // Grains "return" packages to the mount set where the package may remain mounted for some time
  // in case it is used again.

private:
  kj::AsyncIoContext& ioContext;
  std::unordered_map<kj::ArrayPtr<const byte>, PackageMount*,
                     ByteStringHash, ByteStringHash> mounts;
  uint64_t counter = 0;

  static byte dummyByte;
  // Target of pipe reads and writes where we don't care about the content.

  kj::TaskSet tasks;

  void taskFailed(kj::Exception&& exception) override;
};

class WorkerImpl: public Worker::Server, private kj::TaskSet::ErrorHandler {
public:
  WorkerImpl(kj::AsyncIoContext& ioContext, sandstorm::SubprocessSet& subprocessSet,
             LocalPersistentRegistry& persistentRegistry);
  ~WorkerImpl() noexcept(false);

protected:
  kj::Promise<void> newGrain(NewGrainContext context) override;
  kj::Promise<void> restoreGrain(RestoreGrainContext context) override;
  kj::Promise<void> unpackPackage(UnpackPackageContext context) override;
  kj::Promise<void> unpackBackup(UnpackBackupContext context) override;
  kj::Promise<void> packBackup(PackBackupContext context) override;

private:
  class RunningGrain;
  class PackageUploadStreamImpl;
  struct CommandInfo;

  kj::LowLevelAsyncIoProvider& ioProvider;
  sandstorm::SubprocessSet& subprocessSet;
  LocalPersistentRegistry& persistentRegistry;
  PackageMountSet packageMountSet;
  std::unordered_map<RunningGrain*, kj::Own<RunningGrain>> runningGrains;
  kj::TaskSet tasks;

  sandstorm::Supervisor::Client bootGrain(
      PackageInfo::Reader packageInfo, kj::Own<capnp::MessageBuilder> grainState,
      sandstorm::Assignable<GrainState>::Setter::Client grainStateSetter,
      sandstorm::spk::Manifest::Command::Reader command, bool isNew,
      kj::String grainId, sandstorm::SandstormCore::Client core,
      kj::Own<LocalPersistentRegistry::Registration> persistentRegistration);

  void taskFailed(kj::Exception&& exception) override;
};

class SupervisorMain: public sandstorm::AbstractMain {
  // Like sandstorm::SupervisorMain, except that it sets itself up on the Blackrock VatNetwork.

public:
  SupervisorMain(kj::ProcessContext& context);

  kj::MainFunc getMain() override;

  kj::MainBuilder::Validity run();

private:
  kj::ProcessContext& context;
  sandstorm::SupervisorMain sandstormSupervisor;

  class SystemConnectorImpl;
};

class MetaSupervisorMain: public sandstorm::AbstractMain {
  // A binary which is responsible for mounting nbd and then exec()ing the supervisor.

public:
  MetaSupervisorMain(kj::ProcessContext& context);

  kj::MainFunc getMain() override;

  kj::MainBuilder::Validity run();

private:
  kj::ProcessContext& context;
  kj::StringPtr packageMount;
  kj::Vector<kj::StringPtr> args;
  bool isNew = false;
};

class UnpackMain: public sandstorm::AbstractMain {
  // Thin wrapper around `spk unpack` for use by Blackrock worker.

public:
  UnpackMain(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() override;

  kj::MainBuilder::Validity run();

private:
  kj::ProcessContext& context;
};

class BackupMain: public sandstorm::AbstractMain {
  // Thin wrapper around backup/restore functionality for use by Blackrock worker.

public:
  BackupMain(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() override;

  kj::MainBuilder::Validity run(kj::StringPtr filename);

private:
  kj::ProcessContext& context;
  bool restore = false;
};

}  // namespace blackrock

#endif // BLACKROCK_WORKER_H_
