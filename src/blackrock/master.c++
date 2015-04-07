// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "master.h"
#include <map>
#include <set>
#include <kj/debug.h>
#include <kj/vector.h>
#include <blackrock/machine.capnp.h>

namespace blackrock {

namespace {

struct TypeCounts {
  uint count = 0;
  uint maxIndex = 0;
};

class ErrorLogger: public kj::TaskSet::ErrorHandler {
public:
  void taskFailed(kj::Exception&& exception) override {
    KJ_LOG(ERROR, exception);
  }
};

}  // namespace

void runMaster(kj::AsyncIoContext& ioContext, ComputeDriver& driver, MasterConfig::Reader config,
               sa_family_t ipVersion) {
  KJ_REQUIRE(config.getWorkerCount() > 0, "need at least one worker");

  kj::Vector<kj::Promise<void>> startupTasks;

  auto list = driver.listMachines().wait(ioContext.waitScope);

  std::map<ComputeDriver::MachineType, TypeCounts> countsMap;
  ComputeDriver::MachineStatus storageStatus;
  std::map<uint, ComputeDriver::MachineStatus> workerStatus;

  for (auto& machine: list) {
    auto& counts = countsMap[machine.id.type];
    ++counts.count;
    counts.maxIndex = kj::max(counts.maxIndex, machine.id.index);
    switch (machine.id.type) {
      case ComputeDriver::MachineType::STORAGE:
        storageStatus = machine;
        break;
      case ComputeDriver::MachineType::WORKER:
        workerStatus[machine.id.index] = machine;
        break;
      default:
        break;
    }
  }

  {
    auto& storage = countsMap[ComputeDriver::MachineType::STORAGE];
    KJ_REQUIRE(storage.count > 0, "no storage node found; currently you must create manually");
    KJ_REQUIRE(storage.count == 1, "currently we don't support multiple storage nodes");
    KJ_REQUIRE(storage.maxIndex == 0, "storage node had non-zero index");

    if (storageStatus.path == nullptr) {
      KJ_LOG(INFO, "BOOTING: storage");
      startupTasks.add(driver.boot(storageStatus.id)
          .then([&storageStatus](auto path) {
        KJ_LOG(INFO, "BOOTED: storage");
        storageStatus.path = path;
      }));
    }
  }

  {
    uint i = 0;
    uint workerCount = config.getWorkerCount();
    kj::Vector<uint> missingWorkers;

    for (auto& worker: workerStatus) {
      while (i < kj::min(worker.first, workerCount)) {
        // Missing a worker.
        missingWorkers.add(i++);
      }
      if (worker.first >= workerCount) {
        ComputeDriver::MachineId id = worker.second.id;
        KJ_LOG(INFO, "destroying no-longer-wanted worker machine", id.index);
        driver.halt(id);
      }
      i = worker.first + 1;
    }
    while (i < workerCount) {
      missingWorkers.add(i++);
    }

    for (auto index: missingWorkers) {
      ComputeDriver::MachineId id = { ComputeDriver::MachineType::WORKER, index };
      KJ_LOG(INFO, "CREATING: worker", id.index);
      startupTasks.add(driver.create(id)
          .then([&workerStatus,id]() {
        KJ_LOG(INFO, "CREATED: worker", id.index);
        workerStatus[id.index] = ComputeDriver::MachineStatus { id, nullptr };
      }));
    }
  }

  KJ_LOG(INFO, "waiting for startup tasks...");
  kj::joinPromises(startupTasks.releaseAsArray()).wait(ioContext.waitScope);

  // =====================================================================================

  VatNetwork network(ioContext.provider->getNetwork(), ioContext.provider->getTimer(),
                     SimpleAddress::getWildcard(ipVersion));
  auto rpcSystem = capnp::makeRpcClient(network);

  ErrorLogger logger;
  kj::TaskSet tasks(logger);

  // Start storage.
  auto storage = rpcSystem.bootstrap(KJ_ASSERT_NONNULL(storageStatus.path)).castAs<Machine>()
      .becomeStorageRequest().send();

  // For now, tell the storage that it has no back-ends.
  tasks.add(storage.getSiblingSet().resetRequest().send().then([](auto){}));
  tasks.add(storage.getHostedRestorerSet().resetRequest().send().then([](auto){}));
  tasks.add(storage.getGatewayRestorerSet().resetRequest().send().then([](auto){}));

  // Start workers (the ones that are booted, anyway).
  kj::Vector<Worker::Client> workers;
  for (auto& ws: workerStatus) {
    workers.add(rpcSystem.bootstrap(KJ_ASSERT_NONNULL(ws.second.path)).castAs<Machine>()
        .becomeWorkerRequest().send().getWorker());
  }

  // Loop forever handling messages.
  kj::NEVER_DONE.wait(ioContext.waitScope);
  KJ_UNREACHABLE;
}

} // namespace blackrock

