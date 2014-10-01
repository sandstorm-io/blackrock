# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf49ffd606012a28b;

$import "/capnp/c++.capnp".namespace("sandstorm::blackrock");

using Storage = import "storage.capnp";

struct VatId {
  # Identifies a machine in the cluster.
  #
  # Note that "vats" are expected to be somewhat ephemeral, as machines may be rotated in and out
  # of the cluster on a regular basis. In particular, it is important that upon compromise of a
  # vat's private key, the machine can simply be wiped and restarted with a new key without
  # significant long-term damage. Vats should probably be cycled in this way on a regular basis
  # (perhaps, every few days) even if no compromise is known to have occurred.

  publicKey0 @0 :UInt64;
  publicKey1 @1 :UInt64;
  publicKey2 @2 :UInt64;
  publicKey3 @3 :UInt64;
  # The Vat's ed25519 public key.

  # TODO(soon): If every VatId included a Diffie-Hellman ingredient then any vat would be able
  #   to determine a symmentic key to use with some other vat based only on its VatId, which
  #   would mean it could start sending encrypted packets without waiting for a handshake.
}

struct Address {
  # Address at which you might connect to a vat. Used for three-party hand-offs.

  union {
    ipv4 :group {
      addr @0 :UInt32;
      port @1 :UInt16;
    }

    ipv6 :group {
      addrLow @2 :UInt64;
      addrHigh @3 :UInt64;
      port @4 :UInt16;
    }
  }
}

struct SturdyRefHostId {
  # Parameterization of SturdyRefHostId for Sandstorm internal traffic.
  #
  # Sealing
  # =======
  #
  # When a Sandstorm node receives a "Save" request for a presistable capability, it shall return
  # a SturdyRef which is sealed to the caller's "trust domain" as follows:
  # - All storage servers are part of the "storage" domain.
  # - All coordinators are part of the "coordinator" domain.
  # - All gateways are part of the "gateway" domain.
  # - Any other vat is its own restricted domain (limited to the VatId).
  #
  # These SturdyRefs may only later be restored by vats in the same domain. This rule is a
  # defense-in-depth measure -- in theory it should never be strictly necessary to protect security,
  # but it may defend against attacks that are possible if a machine (especially a worker) is
  # compromised by an attacker.

  union {
    vat @0 :VatId;
    # Identifies a specific vat in the cluster.

    storage @1 :Void;
    # This is a storage object. To restore it, you must first obtain a Datastore interface.
    #
    # The objectId part of the SturdyRef is a StoredObjectId, from storage.capnp.

    grainHosted :group {
      # This object is hosted within a grain.
      #
      # The corresponding ObjectId is in a format defined by the Sandstorm supervisor, which in
      # turn maps to the application's internal format.

      grainState @2 :Storage.StoredObjectId;
      # Storage ID for an Assignable(GrainState) representing the grain.
      #
      # This stored object is sealed for coordinators, so that holding a SturdyRef to a capability
      # hosted by some grain does not grant direct access to the grain's storage.
    }

    external @3 :Void;
    # This reference points outside of the Sandstorm cluster.
    #
    # The ObjectId part of the reference is a StoredObjectId for an Immutable(SturdyRef), where
    # that SturdyRef is designed for use on the public internet. The stored object is sealed for
    # the cluster's Cap'n Proto gateway machines.
  }
}

struct ProvisionId {
  provider @0 :VatId;
  questionId @1 :UInt32;
}

struct RecipientId {
  recipient @0 :VatId;
}

struct ThirdPartyCapId {
  provider @0 :VatId;
  questionId @1 :UInt32;
  vatAddress @2 :Address;
}

struct JoinKeyPart {

}

struct JoinResult {
}
