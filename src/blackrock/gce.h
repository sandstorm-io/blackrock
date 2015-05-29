// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_GCE_H_
#define BLACKROCK_GCE_H_

#include "master.h"
#include <blackrock/master.capnp.h>
#include <unistd.h>

namespace blackrock {

class GceDriver: public ComputeDriver {
public:
  GceDriver(sandstorm::SubprocessSet& subprocessSet, kj::LowLevelAsyncIoProvider& ioProvider,
            GceConfig::Reader config);
  ~GceDriver() noexcept(false);

  SimpleAddress getMasterBindAddress() override;
  kj::Promise<kj::Array<MachineId>> listMachines() override;
  kj::Promise<void> boot(MachineId id) override;
  kj::Promise<VatPath::Reader> run(MachineId id, VatId::Reader masterVatId,
                                   bool requireRestartProcess) override;
  kj::Promise<void> stop(MachineId id) override;

private:
  sandstorm::SubprocessSet& subprocessSet;
  kj::LowLevelAsyncIoProvider& ioProvider;
  GceConfig::Reader config;
  kj::String image;
  std::map<ComputeDriver::MachineId, kj::Own<capnp::MessageReader>> vatPaths;
  SimpleAddress masterBindAddress;

  LogSink logSink;
  kj::Promise<void> logTask;
  SimpleAddress logSinkAddress;

  kj::Promise<void> gceCommand(kj::ArrayPtr<const kj::StringPtr> args,
                               int stdin = STDIN_FILENO, int stdout = STDOUT_FILENO);
  kj::Promise<void> gceCommand(std::initializer_list<const kj::StringPtr> args,
                               int stdin = STDIN_FILENO, int stdout = STDOUT_FILENO) {
    return gceCommand(kj::arrayPtr(args.begin(), args.size()), stdin, stdout);
  }
};

} // namespace blackrock

#endif // BLACKROCK_GCE_H_
