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
  workerCount = 1,
  frontendConfig = (
    baseUrl = "http://frontend0:6080",
    wildcardHost = "*.frontend0",   # TODO
    mongoUrl = "mongodb://172.28.128.1/meteor",
    mongoOplogUrl = "mongodb://172.28.128.1/local",
    allowDemoAccounts = true,
    isTesting = true
  )
);
