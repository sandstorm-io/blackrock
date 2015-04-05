// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
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

  private:
    friend class PackageMountSet;

    PackageMountSet& mountSet;

    kj::Array<byte> id;
    // ID string assigned to this package.

    kj::String path;

    kj::Own<NbdVolumeAdapter> volumeAdapter;

    kj::AsyncIoProvider::PipeThread nbdThread;
    // Thread which mounts the NBD device. Protocol as follows:
    // 1) thread -> main: 1 byte: The mount point is ready.
    // 2) main -> thread: EOF: Please shut down.
    // 3) thread -> main: EOF: I've shut down now; it's safe to destroy the NbdVolumeAdapter and
    //                         join the thread.

    kj::ForkedPromise<void> loaded;
    // Resolves when the thread reports that the mount point is active.
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

class WorkerImpl: public Worker::Server {
public:
  explicit WorkerImpl(kj::AsyncIoContext& ioContext);
  ~WorkerImpl() noexcept(false);

  kj::Maybe<sandstorm::Supervisor::Client> getRunningGrain(kj::ArrayPtr<const byte> id);
  bool childExited(pid_t pid, int status);

  kj::Promise<void> newGrain(NewGrainContext context) override;
  kj::Promise<void> restoreGrain(RestoreGrainContext context) override;

private:
  class RunningGrain;
  class SandstormCoreImpl;
  struct CommandInfo;

  kj::LowLevelAsyncIoProvider& ioProvider;
  PackageMountSet packageMountSet;
  std::unordered_map<kj::ArrayPtr<const byte>, kj::Own<RunningGrain>,
                     ByteStringHash, ByteStringHash> runningGrains;
  std::unordered_map<pid_t, RunningGrain*> runningGrainsByPid;

  sandstorm::Supervisor::Client bootGrain(PackageInfo::Reader packageInfo,
      Assignable<GrainState>::Client grainState, Volume::Client grainVolume,
      sandstorm::spk::Manifest::Command::Reader command);
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

}  // namespace blackrock

#endif // BLACKROCK_WORKER_H_
