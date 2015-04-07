// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_MASTER_H_
#define BLACKROCK_MASTER_H_

#include "common.h"
#include "cluster-rpc.h"
#include <kj/async-io.h>
#include <blackrock/master.capnp.h>

namespace blackrock {

class ComputeDriver {
public:
  enum class MachineType {
    STORAGE,
    WORKER,
    COORDINATOR,
    FRONTEND,
    MONGO
  };

  struct MachineId {
    MachineType type;
    uint index;
  };

  struct MachineStatus {
    MachineId id;
    kj::Maybe<VatPath::Reader> path;
    // Current path, or null if not powered up. Path remains valid until halt() or destroy() is
    // called on the machine.
  };

  virtual kj::Promise<kj::Array<MachineStatus>> listMachines();
  virtual kj::Promise<void> create(MachineId id) = 0;
  virtual kj::Promise<void> destroy(MachineId id) = 0;
  virtual kj::Promise<VatPath::Reader> boot(MachineId id) = 0;
  virtual kj::Promise<void> halt(MachineId id) = 0;
};

void runMaster(kj::AsyncIoContext& ioContext, ComputeDriver& driver, MasterConfig::Reader config,
               sa_family_t ipVersion);

} // namespace blackrock

#endif // BLACKROCK_MASTER_H_
