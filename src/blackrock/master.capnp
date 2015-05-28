# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf58bc2dacec400ce;

$import "/capnp/c++.capnp".namespace("blackrock");

struct MasterConfig {
  workerCount @0 :UInt32;

  # For now, we expect exactly one of each of the other machine types.

  frontendConfig @1 :import "frontend.capnp".FrontendConfig;
}

const testConfig :MasterConfig = (
  workerCount = 2,
  frontendConfig = (
    baseUrl = "http://localrock.sandstorm.io:6080",
    wildcardHost = "*.localrock.sandstorm.io:6080",
    allowDemoAccounts = false,
    isTesting = true
  )
);
