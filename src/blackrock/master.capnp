# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf58bc2dacec400ce;

$import "/capnp/c++.capnp".namespace("blackrock");

struct MasterConfig {
  workerCount @0 :UInt32;

  # For now, we expect exactly one of each of the other machine types.
}

const testConfig :MasterConfig = (workerCount = 1);
