# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf58bc2dacec400ce;

$import "/capnp/c++.capnp".namespace("blackrock");

struct MasterConfig {
  workerCount @0 :UInt32;
  frontendCount @4 :UInt32 = 1;

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
    frontend @5 :Text = "n1-highcpu-2";
    mongo @6 :Text = "n1-standard-1";
  }
}
