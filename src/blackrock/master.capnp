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
    zone = "us-central1-f"
  )
);
