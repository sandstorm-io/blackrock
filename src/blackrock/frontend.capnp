# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xfb7fa19ecd585d19;

$import "/capnp/c++.capnp".namespace("blackrock");

using ClusterRpc = import "cluster-rpc.capnp";
using Util = import "/sandstorm/util.capnp";
using Package = import "/sandstorm/package.capnp";
using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;

interface Frontend {
  # Front-ends run the Sandstorm shell UI (a Meteor app). They accept HTTP connections proxied
  # from the Gateways.

  getHttpAddress @0 () -> (address :ClusterRpc.Address);
  # Get the address and port of the frontend's HTTP interface.
}

struct FrontendConfig {
  baseUrl @0 :Text;
  # Equivalent to BASE_URL from sandstorm.conf.

  wildcardHost @1 :Text;
  # Equivalent to WILDCARD_HOST from sandstorm.conf.

  ddpUrl @2 :Text;
  # Equivalent to DDP_DEFAULT_CONNECTION_URL from sandstorm.conf.

  mongoUrl @3 :Text;
  # Mongo URL, in the format desired by Meteor's MONGO_URL environment variable.

  mongoOplogUrl @4 :Text;
  # Mongo oplog URL, in the format desired by Meteor's MONGO_OPLOG_URL environment variable.

  mailUrl @5 :Text;
  # Equivalent to MAIL_URL from sandstorm.conf.

  allowDemoAccounts @6 :Bool;
  # Equivalent to ALLOW_DEMO_ACCOUNTS from sandstorm.conf.

  isTesting @7 :Bool;
  # Equivalent to IS_TESTING from sandstorm.conf.
}
