# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf49ffd606012a28b;

$import "/capnp/c++.capnp".namespace("sandstorm::blackrock");

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

struct SturdyRef {
  # Parameterization of SturdyRef for Sandstorm internal traffic.

  struct Owner {
    # Owner of a SturdyRef, for sealing purposes. See discussion of sealing in
    # import "/capnp/persistent.capnp".Persistent.

    union {
      vat @0 :VatId;
      # The domain of a single vat. Use this domain when saving refs in the vat's local storage.

      storage @1 :Void;
      # The domain of the storage system. Use when saving refs in long-term storage.

      coordinator @2 :Void;
      # The domain of the coordinators. Use when generating a `hosted` SturdyRef.

      gateway @3 :Void;
      # The domain of the gateways. Use when generating an `external` SturdyRef.

      frontend @4 :Void;
      # The domain of the front-end shell.
    }
  }

  union {
    transient @0 :Transient;
    stored @1 :Stored;
    hosted @2 :Hosted;
    external @3 :External;
  }

  struct Transient {
    # Referece to an object hosted by some specific vat in the cluster, which will eventually
    # become invalid when that vat is taken out of rotation.

    vat @0 :VatId;
    # The vat where the object is located.

    address @1 :Address;
    # Address of the vat.

    localRef @2 :AnyPointer;
    # A SturdyRef in the format defined by the vat.
  }

  struct Stored {
    # Reference to an object in long-term storage.

    key0 @0 :UInt64;
    key1 @1 :UInt64;
    key2 @2 :UInt64;
    key3 @3 :UInt64;
    # 256-bit object key. This both identifies the object and may serve as a symmetric key for
    # decrypting the object.
  }

  struct Hosted {
    # Reference to an object hosted within a grain.

    grainState @0 :Stored;
    # Storage ID for an Assignable(GrainState) representing the grain.
    #
    # This stored object is sealed for coordinators, so that holding a SturdyRef to a capability
    # hosted by some grain does not grant direct access to the grain's storage.

    supervisorRef @1 :AnyPointer;
    # A SturdyRef in the format defined by the Sandstorm supervisor.
  }

  struct External {
    # Reference to an object living outside the Sandstorm cluster.

    gatewayRef @0 :Stored;
    # Reference to a stored Immutable(SturdyRef), where that SturdyRef is designed for use on
    # the public internet. The stored object is sealed for the cluster's Cap'n Proto gateway
    # machines.
  }
}

using Persistent = import "/capnp/persistent.capnp".Persistent(SturdyRef, SturdyRef.Owner);

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
  # TODO(someday)
}

struct JoinResult {
  # TODO(someday)
}
