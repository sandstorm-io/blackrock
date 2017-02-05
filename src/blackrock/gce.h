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
