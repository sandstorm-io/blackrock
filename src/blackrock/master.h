// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_MASTER_H_
#define BLACKROCK_MASTER_H_

#include "common.h"
#include "cluster-rpc.h"
#include <kj/async-io.h>
#include <blackrock/master.capnp.h>
#include <map>
#include "logs.h"

namespace sandstorm {
  class SubprocessSet;
}

namespace blackrock {

class ComputeDriver {
public:
  enum class MachineType {
    STORAGE,
    WORKER,
    COORDINATOR,
    FRONTEND
  };

  struct MachineId {
    MachineType type;
    uint index;

    inline bool operator==(const MachineId& other) const {
      return type == other.type && index == other.index;
    }
    inline bool operator<(const MachineId& other) const {
      return type < other.type ? true :
             type > other.type ? false :
             index < other.index;
    }

    kj::String toString() const;
    // Makes reasonable hostnames. E.g. { STORAGE, 123 } becomes "storage123".

    MachineId() = default;
    inline MachineId(MachineType type, uint index): type(type), index(index) {}
    MachineId(kj::StringPtr name);
    // Parses results of toString().
  };

  struct MachineStatus {
    MachineId id;
    kj::Maybe<VatPath::Reader> path;
    // Current path, or null if not powered up. Path remains valid until halt() or destroy() is
    // called on the machine.
  };

  virtual SimpleAddress getMasterBindAddress() = 0;
  virtual kj::Promise<kj::Array<MachineId>> listMachines() KJ_WARN_UNUSED_RESULT = 0;
  virtual kj::Promise<VatPath::Reader> start(MachineId id) KJ_WARN_UNUSED_RESULT = 0;
  virtual kj::Promise<void> stop(MachineId id) KJ_WARN_UNUSED_RESULT = 0;
};

void runMaster(kj::AsyncIoContext& ioContext, ComputeDriver& driver, MasterConfig::Reader config);

class VagrantDriver: public ComputeDriver {
public:
  VagrantDriver(sandstorm::SubprocessSet& subprocessSet, kj::LowLevelAsyncIoProvider& ioProvider);
  ~VagrantDriver() noexcept(false);

  SimpleAddress getMasterBindAddress() override;
  kj::Promise<kj::Array<MachineId>> listMachines() override;
  kj::Promise<VatPath::Reader> start(MachineId id) override;
  kj::Promise<void> stop(MachineId id) override;

private:
  sandstorm::SubprocessSet& subprocessSet;
  kj::LowLevelAsyncIoProvider& ioProvider;
  std::map<ComputeDriver::MachineId, kj::Own<capnp::MessageReader>> vatPaths;
  SimpleAddress masterBindAddress;

  LogSink logSink;
  kj::Promise<void> logTask;
  SimpleAddress logSinkAddress;
};

} // namespace blackrock

#endif // BLACKROCK_MASTER_H_
