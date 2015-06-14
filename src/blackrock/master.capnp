# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf58bc2dacec400ce;

$import "/capnp/c++.capnp".namespace("blackrock");

struct MasterConfig {
  workerCount @0 :UInt32;

  # For now, we expect exactly one of each of the other machine types.

  frontendConfig @1 :import "frontend.capnp".FrontendConfig;

  union {
    vagrant @2 :VagrantConfig;
    gce @3 :GceConfig;
  }
}

struct VagrantConfig {}

struct GceConfig {
  project @0 :Text;
  zone @1 :Text;
  instanceTypes :group {
    storage @2 :Text = "n1-standard-1";
    worker @3 :Text = "n1-highmem-2";
    coordinator @4 :Text = "n1-standard-1";
    frontend @5 :Text = "n1-standard-1";
    mongo @6 :Text = "n1-standard-1";
  }
}

const vagrantConfig :MasterConfig = (
  workerCount = 2,
  frontendConfig = (
    baseUrl = "http://localrock.sandstorm.io:6080",
    wildcardHost = "*.localrock.sandstorm.io:6080",
    allowDemoAccounts = false,
    isTesting = true
  ),
  vagrant = ()
);

const gceTestConfig :MasterConfig = (
  workerCount = 2,
  frontendConfig = (
    baseUrl = "https://testrock.sandstorm.io",
    wildcardHost = "*.testrock.sandstorm.io",
    allowDemoAccounts = false,
    isTesting = true
  ),
  gce = (
    project = "sandstorm-blackrock-testing",
    zone = "us-central1-f",
    instanceTypes = (
      storage = "g1-small",
      worker = "n1-standard-1",
      coordinator = "g1-small",
      frontend = "g1-small",
      mongo = "g1-small"
    )
  )
);

const gceOasisConfig :MasterConfig = (
  workerCount = 2,
  frontendConfig = (
    baseUrl = "https://oasis.sandstorm.io",
    wildcardHost = "*.oasis.sandstorm.io",
    ddpUrl = "https://ddp.oasis.sandstorm.io",
    allowDemoAccounts = false,
    isTesting = false
  ),
  gce = (
    project = "sandstorm-oasis",
    zone = "us-central1-f"
  )
);
