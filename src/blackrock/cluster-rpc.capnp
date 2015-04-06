# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xf49ffd606012a28b;

$import "/capnp/c++.capnp".namespace("blackrock");
using GenericPersistent = import "/capnp/persistent.capnp".Persistent;

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
  # The Vat's Curve25519 public key, interpreted as little-endian.
}

struct Address {
  # Address at which you might connect to a vat. Used for three-party hand-offs.
  #
  # Note that any vat that listens for connections on a port should also listen for unix domain
  # socket connections on the "abstract" name "sandstorm-<port>", so that other vats on the same machine
  # can connect via Unix sockets rather than IP.

  lower64 @0 :UInt64;
  upper64 @1 :UInt64;
  # Bits of the IPv6 address. Since IP is a big-endian spec, the "lower" bits are on the right, and
  # the "upper" bits on the left. E.g., if the address is "1:2:3:4:5:6:7:8", then the lower 64 bits
  # are "5:6:7:8" or 0x0005000600070008 while the upper 64 bits are "1:2:3:4" or 0x0001000200030004.
  #
  # Note that for an IPv4 address, according to the standard IPv4-mapped IPv6 address rules, you
  # would use code like this:
  #     uint32 ipv4 = (octet[0] << 24) | (octet[1] << 16) | (octet[2] << 8) | octet[3];
  #     dest.setLower64(0x0000FFFF00000000 | ipv4);
  #     dest.setUpper64(0);

  port @2 :UInt16;
}

struct VatPath {
  # Enough information to connect to a vat securely.

  id @0 :VatId;
  address @1 :Address;
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

    vat @0 :VatPath;
    # The vat where the object is located.

    localRef @1 :AnyPointer;
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
    #
    # TODO(soon): This doesn't work: there's no way for the coordinator to enforce the seal on
    #   this ref, because the owner isn't stored anywhere. Possible solutions:
    #   1) Use a reference to a wrapper object in storage owned by the coordinators, which itself
    #      stores the actual object and Owner for enforcement. Problem: won't be cleaned up when
    #      the grain is deleted.
    #   2) Extend Persistent.save() to accept a tag which is returned later on load. Or have
    #      it return the Owner on load, and we can make our Owner type include information about
    #      who is allowed to invoke the coordinator. But note that remote entities and apps won't
    #      be expected to maintain such storage.

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

interface Persistent extends(GenericPersistent(SturdyRef, SturdyRef.Owner)) {}

interface Restorer(Ref) {
  restore @0 (sturdyRef :Ref) -> (cap :Capability);
  drop @1 (sturdyRef :Ref);
}

struct ProvisionId {
  provider @0 :VatId;
  # ID of the vat providing the capability (aka the introducer).

  nonce0 @1 :UInt64;
  nonce1 @2 :UInt64;
  # 128-bit nonce randomly chosen by the introducer.
}

struct RecipientId {
  recipient @0 :VatPath;
  # ID of the vat receiving the capability.

  nonce0 @1 :UInt64;
  nonce1 @2 :UInt64;
  # 128-bit nonce randomly chosen by the introducer.
}

struct ThirdPartyCapId {
  provider @0 :VatPath;
  # ID and path to the host of this capability.

  nonce0 @1 :UInt64;
  nonce1 @2 :UInt64;
  # 128-bit nonce randomly chosen by the introducer.
}

struct JoinKeyPart {
  # TODO(someday)
}

struct JoinResult {
  # TODO(someday)
}

# ========================================================================================
# Transport Protocol
#
# We assume an underlying sequential datagram transport supporting:
# - Reliable and ordered delivery.
# - Arbitrary-size datagrams.
# - Congestion cotrol.
# - Peer identified by VatId (not by sending IP/port).
# - At the admin's option, encryption for privacy and integrity. (This is optional because many
#   Blackrock clusters may be on physically secure networks where encryption is not needed.)
#
# The simplest implementation of this protocol -- called "the simple protocol" -- is based on
# unencrypted TCP, where we assume that the network infrastructure is secure enough to ensure
# integrity and privacy when delivering packets. The protocol still uses crypto to authenticate
# the connection upfront.
#
# TODO(security): The following protocol has not been reviewed by a crypto expert, and therefore
#   may be totally stupid.
#
# In the simple protocol, a connection is initiated by sending the following header:
# - 32 bytes: The sender's X25519 public key.
# - 8 bytes: Connection number (little-endian). Each time a new connection is initiated from the
#     sending vat to the same receiving vat, this number must increase. If the sender's public key
#     is less than the receiver's, this number must be even, otherwise it must be odd, so that
#     connection IDs in opposite directions between the same vats never collide. Any existing
#     connections with lower connection IDs must be invalidated when a new connection starts.
# - 8 bytes: minIgnoredConnection, the minimum connection number which the sender guarantees that
#     it had not received at the time that it sent this message. The sender promises that it if
#     later receives an incoming connection with this number or greater, but less than the
#     connection number that the sender is initiating with this header, then it will reject any
#     such connection without reading any messages from it. This gives the receiver of this header
#     some assurance that if it had tried to form a connection previously and optimistically sent
#     messages on it, it is safe to send those messages again.
# - 16 bytes: poly1305 MAC of the above *plus* the sender's IPv6 address (or IPv6-mapped IPv4
#     address) and port number (18 bytes). The key is constructed by taking the first 32 bytes of
#     the ChaCha20 stream generated using the two vats' shared secret as a key, and the connection
#     number as a nonce. The purpose of this MAC is to prevent an arbitrary node on the network
#     from impersonating an arbitrary vat by simply sending its public key, which would otherwise
#     be possible even assuming a secure physical network.
#
# Upon accepting a conneciton, the acceptor does the following:
# - Wait for the header.
# - Verify the header MAC (closing the connection immediately if invalid).
# - If the connection number is less than that of any existing connection to the same remote vat --
#   especially, one recently initiated in the opposite direction -- close it and do not continue.
# - Send a reply header on the newly-accepted connection, which is similar to the received header
#   except that it bears the accepting vat's public key and the connection number (and MAC nonce)
#   is incremented by one. (Notice that this connection number could not possibly already have been
#   used because of the previous step.)
# - If there is any other outstanding connection to the same remote vat (with a lower number),
#   close that other connection. If this vat had sent messages on said other connection but had not
#   yet received any data (including the header) from the peer, then re-send those messages on the
#   newly-accepted connection instead.
#
# Note that, for the initiator of the connection, between the time that the connection starts and
# the time that the reply header is received, it is not yet known if the IP address connected to
# really does correspond to the intended VatId. However, since the IP address was given to us by
# the introducer, and the introducer could have introduced us to anybody, we can safely send
# plaintext messages meant for the entity to whom we were introduced. The only problem is if
# we receive another introduction for the same target VatId but a different IP/port pair in the
# interim. In this case, we must wait until we've received the reply on our existing connection
# authenticating it. If we receive no reply in a reasonable time, or we receive a bogus reply,
# we must close the connection and create a new one with the new address. At this point we cannot
# send *any* messages until the new connection comes back with a valid header, at which point we
# can re-send the messages we had sent to the old connection.
