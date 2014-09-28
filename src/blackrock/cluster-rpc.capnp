# Sandstorm Enterprise Tools
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf49ffd606012a28b;

using Storage = import "storage.capnp";

struct InternalHostId {
  # Parameterization of SturdyRefHostId for Sandstorm internal traffic.

  union {
    storage @0 :Void;
    # This is a storage object. You may connect to any storage node to restore it.

    worker @1 :UInt32;
    # Identifies a worker node. This capability may either refer to the worker itself or a
    # HostedGrain active there.
    #
    # Each worker node is identified by a 32-bit ID.

    appHosted :group {
      # This is an application-hosted object.
      #
      # The corresponding ObjectId is in a format defined by the Sandstorm supervisor, which in
      # turn maps to the application's internal format.

      grainState @2 :Storage.StoredObjectId;
      # Storage ID for an Assignable(GrainState) representing the grain.
    }
  }
}
