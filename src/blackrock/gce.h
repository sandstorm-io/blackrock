// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_GCE_H_
#define BLACKROCK_GCE_H_

#include "master.h"

namespace blackrock {

class GceDriver: public ComputeDriver {
public:
  GceDriver(sandstorm::SubprocessSet& subprocessSet, kj::LowLevelAsyncIoProvider& ioProvider,
            kj::StringPtr imageName, kj::StringPtr zone);
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
  kj::StringPtr imageName;
  kj::StringPtr zone;
  std::map<ComputeDriver::MachineId, kj::Own<capnp::MessageReader>> vatPaths;
  SimpleAddress masterBindAddress;

  LogSink logSink;
  kj::Promise<void> logTask;
  SimpleAddress logSinkAddress;
};

} // namespace blackrock

#endif // BLACKROCK_GCE_H_
