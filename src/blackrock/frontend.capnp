# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@0xfb7fa19ecd585d19;

$import "/capnp/c++.capnp".namespace("blackrock");

using ClusterRpc = import "cluster-rpc.capnp";
using Util = import "/sandstorm/util.capnp";
using Package = import "/sandstorm/package.capnp";
using Supervisor = import "/sandstorm/supervisor.capnp".Supervisor;
using GatewayRouter = import "/sandstorm/backend.capnp".GatewayRouter;

interface Frontend {
  # Front-ends run the Sandstorm shell UI (a Meteor app). They accept HTTP connections proxied
  # from the Gateways.

  struct Instance {
    replicaNumber @0 :UInt32;
    httpAddress @1 :ClusterRpc.Address;
    smtpAddress @2 :ClusterRpc.Address;
    router @3 :GatewayRouter;
  }

  getInstances @0 () -> (instances :List(Instance));
  # A front-end machine may run multiple instances of the Sandstorm Shell server. This method gets
  # a list of instances, so that the gateway can consitently route requests from a particular user
  # to a particular instance.
}

interface Mongo {
  getConnectionInfo @0 () -> (address :ClusterRpc.Address, username :Text, password :Text);

  # TODO(someday): Support replicas.
}

struct FrontendConfig {
  # Config for shells -- and for gateways, for historical reasons.

  baseUrl @0 :Text;
  # Equivalent to BASE_URL from sandstorm.conf.

  wildcardHost @1 :Text;
  # Equivalent to WILDCARD_HOST from sandstorm.conf.

  ddpUrl @2 :Text;
  # Equivalent to DDP_DEFAULT_CONNECTION_URL from sandstorm.conf.

  mailUrl @3 :Text;
  # Equivalent to MAIL_URL from sandstorm.conf.

  allowDemoAccounts @4 :Bool;
  # Equivalent to ALLOW_DEMO_ACCOUNTS from sandstorm.conf.

  isTesting @5 :Bool;
  # Equivalent to IS_TESTING from sandstorm.conf.

  isQuotaEnabled @13 :Bool = true;

  stripeKey @6 :Text;
  stripePublicKey @7 :Text;
  outOfBeta @12 :Bool;

  mailchimpKey @10 :Text;
  mailchimpListId @11 :Text;

  allowUninvited @8 :Bool;

  replicasPerMachine @9 :UInt32;

  privateKeyPassword @14 :Text;
  termsPublicId @15 :Text;
}
