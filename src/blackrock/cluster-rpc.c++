// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "cluster-rpc.h"
#include <kj/debug.h>
#include <sodium/core.h>
#include <sodium/randombytes.h>
#include <sodium/crypto_scalarmult.h>
#include <sodium/crypto_stream_chacha20.h>
#include <sodium/crypto_onetimeauth_poly1305.h>
#include <sodium/utils.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>
#include <ifaddrs.h>
#include <capnp/serialize-async.h>
#include <sandstorm/util.h>
#include <unordered_map>

namespace sandstorm {
namespace blackrock {

static inline uint64_t fromLittleEndian64(const kj::byte* bytes) {
  return (static_cast<uint64_t>(bytes[0]) <<  0) |
         (static_cast<uint64_t>(bytes[1]) <<  8) |
         (static_cast<uint64_t>(bytes[2]) << 16) |
         (static_cast<uint64_t>(bytes[3]) << 24) |
         (static_cast<uint64_t>(bytes[4]) << 32) |
         (static_cast<uint64_t>(bytes[5]) << 40) |
         (static_cast<uint64_t>(bytes[6]) << 48) |
         (static_cast<uint64_t>(bytes[7]) << 56);
}

static inline uint64_t fromBigEndian64(const kj::byte* bytes) {
  return (static_cast<uint64_t>(bytes[0]) << 56) |
         (static_cast<uint64_t>(bytes[1]) << 48) |
         (static_cast<uint64_t>(bytes[2]) << 40) |
         (static_cast<uint64_t>(bytes[3]) << 32) |
         (static_cast<uint64_t>(bytes[4]) << 24) |
         (static_cast<uint64_t>(bytes[5]) << 16) |
         (static_cast<uint64_t>(bytes[6]) <<  8) |
         (static_cast<uint64_t>(bytes[7]) <<  0);
}

static inline void toBigEndian64(kj::byte* bytes, uint64_t value) {
  bytes[0] = (value >> 56) & 0xffu;
  bytes[1] = (value >> 48) & 0xffu;
  bytes[2] = (value >> 40) & 0xffu;
  bytes[3] = (value >> 32) & 0xffu;
  bytes[4] = (value >> 24) & 0xffu;
  bytes[5] = (value >> 16) & 0xffu;
  bytes[6] = (value >>  8) & 0xffu;
  bytes[7] = (value >>  0) & 0xffu;
}

static inline void toLittleEndian64(kj::byte* bytes, uint64_t value) {
  bytes[0] = (value >>  0) & 0xffu;
  bytes[1] = (value >>  8) & 0xffu;
  bytes[2] = (value >> 16) & 0xffu;
  bytes[3] = (value >> 24) & 0xffu;
  bytes[4] = (value >> 32) & 0xffu;
  bytes[5] = (value >> 40) & 0xffu;
  bytes[6] = (value >> 48) & 0xffu;
  bytes[7] = (value >> 56) & 0xffu;
}

// =======================================================================================

class VatNetwork::LittleEndian64 {
public:
  inline LittleEndian64(decltype(nullptr)) {}
  inline LittleEndian64(uint64_t value) { toLittleEndian64(bytes, value); }

  inline uint64_t get() const { return fromLittleEndian64(bytes); }

  inline const byte* getBytes() const { return bytes; }

private:
  byte bytes[8];
};

// -------------------------------------------------------------------

VatNetwork::SimpleAddress::SimpleAddress(struct sockaddr_in ip4): ip4(ip4) {}
VatNetwork::SimpleAddress::SimpleAddress(struct sockaddr_in6 ip6): ip6(ip6) {}
VatNetwork::SimpleAddress::SimpleAddress(Address::Reader reader) {
  memset(this, 0, sizeof(*this));

  uint64_t lower = reader.getLower64();
  uint64_t upper = reader.getUpper64();

  if (upper == 0 &&
      (lower & 0xffffffff00000000ull) == 0x0000ffff00000000ull) {
    ip4.sin_family = AF_INET;
    ip4.sin_addr.s_addr = htonl(lower & 0x00000000ffffffffull);
    ip4.sin_port = htons(reader.getPort());
  } else {
    ip6.sin6_family = AF_INET6;
    toBigEndian64(ip6.sin6_addr.s6_addr + 0, upper);
    toBigEndian64(ip6.sin6_addr.s6_addr + 8, lower);
    ip6.sin6_port = htons(reader.getPort());
  }
}

auto VatNetwork::SimpleAddress::getPeer(kj::AsyncIoStream& socket) -> SimpleAddress {
  SimpleAddress result;
  uint len = sizeof(result);
  socket.getpeername(&result.addr, &len);
  switch (result.addr.sa_family) {
    case AF_INET:
    case AF_INET6:
      break;
    default:
      KJ_FAIL_REQUIRE("Not an IP address!");
  }
  return result;
}

auto VatNetwork::SimpleAddress::getLocal(kj::AsyncIoStream& socket) -> SimpleAddress {
  SimpleAddress result;
  uint len = sizeof(result);
  socket.getsockname(&result.addr, &len);
  switch (result.addr.sa_family) {
    case AF_INET:
    case AF_INET6:
      break;
    default:
      KJ_FAIL_REQUIRE("Not an IP address!");
  }
  return result;
}

void VatNetwork::SimpleAddress::setPort(uint16_t port) {
  switch (addr.sa_family) {
    case AF_INET:
      ip4.sin_port = htons(port);
      break;
    case AF_INET6:
      ip6.sin6_port = htons(port);
      break;
    default:
      KJ_UNREACHABLE;
  }
}

void VatNetwork::SimpleAddress::copyTo(Address::Builder builder) const {
  switch (addr.sa_family) {
    case AF_INET:
      builder.setLower64(0x0000ffff00000000ull | ntohl(ip4.sin_addr.s_addr));
      builder.setUpper64(0);
      builder.setPort(ntohs(ip4.sin_port));
      break;
    case AF_INET6:
      builder.setLower64(fromBigEndian64(ip6.sin6_addr.s6_addr + 8));
      builder.setUpper64(fromBigEndian64(ip6.sin6_addr.s6_addr + 0));
      builder.setPort(ntohs(ip6.sin6_port));
      break;
    default:
      KJ_UNREACHABLE;
  }
}

void VatNetwork::SimpleAddress::getFlat(byte* target) const {
  memset(target, 0, FLAT_SIZE);
  switch (addr.sa_family) {
    case AF_INET:
      memcpy(target, &ip4.sin_addr.s_addr, 4);
      target[4] = 0xff;
      target[5] = 0xff;
      memcpy(target + 16, &ip4.sin_port, 2);
      break;
    case AF_INET6:
      memcpy(target, ip6.sin6_addr.s6_addr, 16);
      memcpy(target + 16, &ip6.sin6_port, 2);
      break;
    default:
      KJ_UNREACHABLE;
  }
}

kj::Own<kj::NetworkAddress> VatNetwork::SimpleAddress::onNetwork(kj::Network& network) {
  return network.getSockaddr(&addr, addr.sa_family == AF_INET ? sizeof(ip4) : sizeof(ip6));
}

bool VatNetwork::SimpleAddress::operator==(const SimpleAddress& other) const {
  switch (addr.sa_family) {
    case AF_INET:
      return other.addr.sa_family == AF_INET &&
          ip4.sin_addr.s_addr == other.ip4.sin_addr.s_addr &&
          ip4.sin_port == other.ip4.sin_port;
    case AF_INET6:
      return other.addr.sa_family == AF_INET6 &&
          memcmp(ip6.sin6_addr.s6_addr, other.ip6.sin6_addr.s6_addr, 16) == 0 &&
          ip6.sin6_port == other.ip6.sin6_port;
    default:
      KJ_UNREACHABLE;
  }
}

// -------------------------------------------------------------------

class VatNetwork::Mac {
public:
  inline Mac(decltype(nullptr)) {}

  inline bool operator==(const Mac& other) const {
    return sodium_memcmp(bytes, other.bytes, sizeof(bytes)) == 0;
  }
  inline bool operator!=(const Mac& other) const {
    return sodium_memcmp(bytes, other.bytes, sizeof(bytes)) != 0;
  }

private:
  byte bytes[crypto_onetimeauth_poly1305_BYTES];

  friend class SymmetricKey;
  Mac(kj::ArrayPtr<const byte> data, byte key[crypto_onetimeauth_poly1305_KEYBYTES]) {
    crypto_onetimeauth_poly1305(bytes, data.begin(), data.size(), key);
  }
};

// -------------------------------------------------------------------

class VatNetwork::SymmetricKey {
public:
  // TODO(security): Allocate shared secrets inside pinned memory, but don't use sodium_malloc()
  //   for each one because it allocates three whole pages. It's probably OK to pack session keys
  //   together.

  ~SymmetricKey() {
    sodium_memzero(secret, sizeof(secret));
  }

  Mac authenticate(kj::ArrayPtr<const byte> data, LittleEndian64 nonce, uint64_t offset) const {
    static_assert(crypto_stream_chacha20_NONCEBYTES == 8, "Bad nonce size.");
    static_assert(sizeof(secret) == crypto_stream_chacha20_KEYBYTES, "Bad key size.");

    // There is no crypto_stream_chacha20_ic(), only xor_ic, so we need to xor against zero.
    byte zero[crypto_onetimeauth_poly1305_KEYBYTES] = {0};
    byte macKey[crypto_onetimeauth_poly1305_KEYBYTES];
    crypto_stream_chacha20_xor_ic(macKey, zero, sizeof(zero), nonce.getBytes(), offset, secret);

    return Mac(data, macKey);
  }

private:
  friend class PrivateKey;
  SymmetricKey(const byte theirPublic[], const byte myPrivate[]) {
    crypto_scalarmult_curve25519(secret, myPrivate, theirPublic);
  }

  byte secret[crypto_scalarmult_curve25519_BYTES];
};

// -------------------------------------------------------------------

VatNetwork::PublicKey::PublicKey(const byte* privateBytes) {
  static_assert(sizeof(key) == crypto_scalarmult_curve25519_BYTES, "Wrong key size.");
  KJ_ASSERT(crypto_scalarmult_curve25519_base(key, privateBytes) == 0);
}

VatNetwork::PublicKey::PublicKey(VatId::Reader id) {
  toLittleEndian64(key +  0, id.getPublicKey0());
  toLittleEndian64(key +  8, id.getPublicKey1());
  toLittleEndian64(key + 16, id.getPublicKey2());
  toLittleEndian64(key + 24, id.getPublicKey3());
}

void VatNetwork::PublicKey::copyTo(VatId::Builder id) {
  id.setPublicKey0(fromLittleEndian64(key +  0));
  id.setPublicKey1(fromLittleEndian64(key +  8));
  id.setPublicKey2(fromLittleEndian64(key + 16));
  id.setPublicKey3(fromLittleEndian64(key + 24));
}

struct VatNetwork::PublicKey::Hash {
  inline size_t operator()(const PublicKey& key) const {
    // TODO(security): Since x25519 public keys should be diffuse over the entire key space, using
    //   the first 8 bytes as a hash for hashtables works as long as we're only using it for real
    //   keys. But, someone could in theory fill up this table with fake keys by sending us a lot
    //   of bogus introductions, and could trivially make those keys collide. Longer-term, we
    //   should add a (non-cryptographic) hash library to KJ and use it here to hash the whole key.
    return fromLittleEndian64(key.key);
  }
};

// -------------------------------------------------------------------

VatNetwork::PrivateKey::PrivateKey() {
  // Apparently libsodium doesn't currently support generating a key for you. According to
  // http://cr.yp.to/ecdh.html, the following should produce a key.

  sodium_init();

  key = reinterpret_cast<byte*>(sodium_malloc(crypto_scalarmult_curve25519_BYTES));
  randombytes_buf(key, crypto_scalarmult_curve25519_BYTES);
  key[0] &= 248;
  key[31] &= 127;
  key[31] |= 64;
}
VatNetwork::PrivateKey::~PrivateKey() {
  sodium_memzero(key, sizeof(key));
}

auto VatNetwork::PrivateKey::getPublic() const -> PublicKey {
  return PublicKey(key);
}

auto VatNetwork::PrivateKey::getSharedSecret(PublicKey otherPublic) const -> SymmetricKey {
  return SymmetricKey(otherPublic.key, key);
}

// -------------------------------------------------------------------

class VatNetwork::Header {
public:
  Header(decltype(nullptr))
      : senderPublicKey(nullptr), connectionNumber(nullptr),
        minIgnoredConnection(nullptr), mac(nullptr) {}

  Header(const PublicKey& myPublic, uint64_t connectionNumber, uint64_t minIgnoredConnection,
         const SimpleAddress& myAddress, const PublicKey& peerPublic, const PrivateKey& myPrivate)
      : senderPublicKey(myPublic),
        connectionNumber(connectionNumber),
        minIgnoredConnection(minIgnoredConnection),
        mac(computeMac(myPrivate.getSharedSecret(peerPublic), myAddress)) {}

  kj::Maybe<PublicKey> verify(const PrivateKey& privateKey, const SimpleAddress& address) {
    // Check the mac.
    if (computeMac(privateKey.getSharedSecret(senderPublicKey), address) != mac) {
      return nullptr;
    }

    return senderPublicKey;
  }

  uint64_t getConnectionNumber() {
    return connectionNumber.get();
  }
  uint64_t getMinIgnoredConnection() {
    return minIgnoredConnection.get();
  }

private:
  PublicKey senderPublicKey;
  LittleEndian64 connectionNumber;
  LittleEndian64 minIgnoredConnection;
  Mac mac;

  Mac computeMac(const SymmetricKey& secret, const SimpleAddress& address) {
    size_t headerSize = sizeof(*this) - sizeof(mac);
    byte data[headerSize + SimpleAddress::FLAT_SIZE];

    memcpy(data, this, headerSize);
    address.getFlat(data + headerSize);

    return secret.authenticate(kj::arrayPtr(data, sizeof(data)), connectionNumber, 0);
  }
};

// -------------------------------------------------------------------

struct VatNetwork::ConnectionMap {
  std::unordered_map<PublicKey, kj::Maybe<kj::Own<ConnectionImpl>>, PublicKey::Hash> map;
};

// =======================================================================================

VatNetwork::VatNetwork(kj::Network& network, kj::Timer& timer, struct sockaddr_in6 addr)
    : VatNetwork(network, timer, SimpleAddress(addr)) {}
VatNetwork::VatNetwork(kj::Network& network, kj::Timer& timer, struct sockaddr_in addr)
    : VatNetwork(network, timer, SimpleAddress(addr)) {}

VatNetwork::VatNetwork(kj::Network& network, kj::Timer& timer, SimpleAddress address)
    : network(network),
      timer(timer),
      publicKey(privateKey.getPublic()),
      address(address),
      connectionReceiver(address.onNetwork(network)->listen()),
      connectionMap(kj::heap<ConnectionMap>()) {
  address.setPort(connectionReceiver->getPort());

  auto path = self.initRoot<VatPath>();
  publicKey.copyTo(path.getId());
  address.copyTo(path.getAddress());
}

VatNetwork::~VatNetwork() {}

class VatNetwork::ConnectionImpl final: public Connection, public kj::Refcounted,
                                        private kj::TaskSet::ErrorHandler {
public:
  ConnectionImpl(VatNetwork& network, PublicKey peerKey, uint64_t minConnectionNumber)
      : ConnectionImpl(network, peerKey, minConnectionNumber, kj::newPromiseAndFulfiller<void>()) {}

  inline uint64_t getMinConnectionNumber() { return minConnectionNumber; }

  bool connect(SimpleAddress address) {
    // If this connection is not already established and authenticated, try connecting to the given
    // address.

    if (state == FAILED) {
      // This connection has failed, so we'll need to reconnect.
      return false;
    }

    if (state == AUTHENTICATED) {
      // If we've already shut down the connection, then the caller will need to create a whole new
      // ConnectionImpl. Otherwise, we're already authenticated, so ignore the new address.
      return previousWrite != nullptr && !receivedShutdown;
    }

    if (state == OPTIMISTIC) {
      KJ_IF_MAYBE(a, streamConnectAddress) {
        if (address == *a) {
          // Same address, so we'll keep working on it.
          return true;
        }
      }

      // Crap, the address changed. We need to shift to pessimistic mode until we figure out
      // which address is correct, because if our first introducer forged this address, we don't
      // want to send messages to it on behalf of our second introducer.
      state = PESSIMISTIC;
      KJ_LOG(WARNING, "Received multiple addresses for same VatId. Possible spoofing.");
    }

    if (state == PESSIMISTIC) {
      // TODO(security): To prevent DoS attacks via bogus introductions, while waiting for our main
      //   connection to authenticate, probe this new address to see if maybe it's the right one.
      //   If it is, give up on the main one and connect to this address instead. Note: It would
      //   make things a lot easier if we could just make several connections at once with the
      //   same connection number, but they'd then all have the same nonce. Maybe we should hash
      //   the receiver's IP address into the poly1305 key? Or what if the connection numbers were
      //   actually timestamps?

      // Ignore new address. Keep trying old address.
      return true;
    } else {
      KJ_ASSERT(state == WAITING);
    }

    // OK, let's try to connenct.
    tasks.add(address.onNetwork(network.network)->connect()
        .then([this](kj::Own<kj::AsyncIoStream>&& connection) -> kj::Promise<void> {
      if (state == AUTHENTICATED) {
        // Apparently we got a connection in the other direction in the meantime. Ignore.
        return kj::READY_NOW;
      }

      auto connectAddress = SimpleAddress::getPeer(*connection);

      bool isOdd = minConnectionNumber % 2;
      bool shouldBeOdd = !(network.publicKey < peerKey);

      if (isOdd != shouldBeOdd) {
        ++minConnectionNumber;
      }

      uint64_t connectionNumber = minConnectionNumber++;
      setStream(kj::mv(connection), connectionNumber, connectAddress);

      if (state == WAITING) {
        state = OPTIMISTIC;
        resendOptimisticMessages();
      }

      // Wait for response header.
      auto header = kj::heap<Header>(nullptr);
      auto promise = stream->read(header.get(), sizeof(Header));
      return promise.then([this,KJ_MVCAP(header),connectAddress,connectionNumber]() mutable {
        auto verifiedKey = KJ_ASSERT_NONNULL(header->verify(network.privateKey, connectAddress),
            "peer responded with invalid handshake header");

        KJ_ASSERT(verifiedKey == peerKey, "peer responded with wrong public key");

        KJ_ASSERT(header->getConnectionNumber() == connectionNumber + 1,
            "peer used wrong connection number in handshake reply");

        // Check if it's possible that a previous attempt at connecting to this peer, on which we
        // sent some optimistic messages, actually reached the peer. Since we guarantee that
        // messages will be delivered no more than once, we cannot send them again, and therefore
        // in this case this ConnectionImpl object is in a state where it cannot continue.
        KJ_ASSERT(sentConnectionNumber >= header->getMinIgnoredConnection(),
            "previous optimistic connection attempt actually succeeded when we thought it "
            "failed; must abort");

        setAuthenticated();
      });
    }).exclusiveJoin(handshakeDone.addBranch()));  // Cancel if another handshake succeeds.

    return true;
  }

  bool accept(kj::Own<kj::AsyncIoStream>&& stream,
              uint64_t connectionNumber, uint64_t minIgnoredConnection) {
    // Receive an already-authenticated connection.
    //
    // Returns false if a full connection had already been established, in which case the
    // ConnectionImpl object needs to be entirely replaced.

    if (connectionNumber < minConnectionNumber) {
      // Ignore accepted connection; our existing one takes priority.
      // If we return true here, the stream will simply be discarded by the caller.
      return true;
    }

    if (sentConnectionNumber < minIgnoredConnection) {
      // We have already sent some messages which may have been received by the peer. Therefore,
      // we cannot start a new connection using this ConnectionImpl object; a new one must be
      // created.
      return false;
    }

    if (state == AUTHENTICATED || state == FAILED) {
      // We already completed connection setup on a previous connection. The peer shouldn't have
      // started a new one unless the old one dropped.
      return false;
    }

    minConnectionNumber = kj::max(minConnectionNumber, connectionNumber + 2);

    streamIncomingConnectionNumber = connectionNumber;
    setStream(kj::mv(stream), connectionNumber + 1, nullptr);
    setAuthenticated();
    return true;
  }

  kj::Own<capnp::OutgoingRpcMessage> newOutgoingMessage(uint firstSegmentWordSize) override {
    return kj::refcounted<OutgoingMessageImpl>(*this, firstSegmentWordSize);
  }

  kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> receiveIncomingMessage() override {
    return kj::evalLater([&]() -> kj::Promise<kj::Maybe<kj::Own<capnp::IncomingRpcMessage>>> {
      if (state == AUTHENTICATED) {
        return capnp::tryReadMessage(*stream)
            .then([&](kj::Maybe<kj::Own<capnp::MessageReader>>&& message)
                  -> kj::Maybe<kj::Own<capnp::IncomingRpcMessage>> {
          KJ_IF_MAYBE(m, message) {
            return kj::Own<capnp::IncomingRpcMessage>(kj::heap<IncomingMessageImpl>(kj::mv(*m)));
          } else {
            receivedShutdown = true;
            return nullptr;
          }
        });
      } else if (state == FAILED) {
        return kj::Exception(KJ_ASSERT_NONNULL(failureReason));
      } else {
        return handshakeDone.addBranch().then([this]() {
          return receiveIncomingMessage();
        });
      }
    });
  }

  kj::Promise<void> shutdown() override {
    if (state == AUTHENTICATED) {
      kj::Promise<void> result = KJ_ASSERT_NONNULL(previousWrite, "already shut down")
          .then([this]() {
        stream->shutdownWrite();
      });
      previousWrite = nullptr;
      return kj::mv(result);
    } else if (state == FAILED) {
      return kj::Exception(KJ_ASSERT_NONNULL(failureReason));
    } else {
      // Wait for handshake completion before shutting down.
      return handshakeDone.addBranch().then([this]() {
        return shutdown();
      });
    }
  }

private:
  VatNetwork& network;
  kj::TaskSet tasks;
  PublicKey peerKey;

  uint64_t minConnectionNumber;
  // The minimum connection number that we can create or accept.

  enum {
    WAITING,
    // We're still waiting for a connection to use.

    OPTIMISTIC,
    // We have a connection. It's not authenticated yet but we can optimistically send messages
    // on it, though we need to queue copies of the message in case they need to be re-sent later.

    PESSIMISTIC,
    // We have conflicting reports on the peer's proper IP address. We have to wait until these
    // resolve before sending anything.

    AUTHENTICATED,
    // The peer has authenticated. It's safe to send messages and we don't have to queue them.

    FAILED
    // This connection failed before it was ever authenticated.
  } state = WAITING;

  bool receivedShutdown = false;

  kj::ForkedPromise<void> handshakeDone;
  kj::Own<kj::PromiseFulfiller<void>> handshakeDoneFulfiller;
  // Resolves as soon as we reach the AUTHENTICATED or FAILED state.

  kj::Own<kj::AsyncIoStream> stream;
  // The connection to use. Valid in OPTIMISTIC and AUTHENTICATED states. Once in the AUTHENTICATED
  // state, the stream cannot change.

  kj::Maybe<SimpleAddress> streamConnectAddress;
  // If `stream` is an outgoing connection, the address we tried to connect to.

  uint64_t streamIncomingConnectionNumber = kj::maxValue;
  uint64_t streamOutgoingConnectionNumber = kj::maxValue;
  // If `stream` is valid, these are its incoming and outgoing connection numbers.

  class OutgoingMessageImpl;
  kj::Vector<kj::Own<OutgoingMessageImpl>> optimisticMessages;
  // In all states except AUTHENTICATED and FAILED, contains a list of all outgoing messages sent
  // so far, so that they can be (re-)sent whenever a new stream is introduced. Once in the
  // AUTHENTICATED or FAILED state, this list is cleared.

  uint sentCount = 0;
  // Number of messages from `optimisticMessages` that had been sent on the current `stream`.

  uint64_t sentConnectionNumber = kj::maxValue;
  // If any messages have been written to a connection, this is the minimum connection number that
  // they were written to. If, on eventual authentication, we discover that the other side may have
  // received these messages on a connection other than the one we finally ended up using, then
  // we have to fail out.

  uint64_t minIgnoredConnectionNumber = 0;
  // The connection number of the last connection that was fully authenticated (and therefore
  // on which we may have received messages), plus one.

  kj::Maybe<kj::Promise<void>> previousWrite;
  // Promise for the completion of the last `write()` call on `stream`. Only valid if `stream` is
  // valid. Becomes null when `shutdownWrite()` is called.

  kj::Maybe<kj::Promise<void>> pessimisticTimeout;
  // In PESSIMISTIC mode, promise which resolves when we've given up on the current address we're
  // working on and can move on.

  kj::Maybe<kj::Exception> failureReason;
  // In FAILED state, the reason for the failure.

  ConnectionImpl(VatNetwork& network, PublicKey peerKey, uint64_t minConnectionNumber,
                 kj::PromiseFulfillerPair<void> paf)
      : network(network), tasks(*this), peerKey(peerKey), minConnectionNumber(minConnectionNumber),
        handshakeDone(paf.promise.fork()), handshakeDoneFulfiller(kj::mv(paf.fulfiller)) {}

  void setStream(kj::Own<kj::AsyncIoStream>&& newStream, uint64_t newOutgoingConnectionNumber,
                 kj::Maybe<SimpleAddress> newConnectAddress) {
    // Cancel all writes.
    previousWrite = nullptr;

    // Accept the new stream.
    stream = kj::mv(newStream);
    streamConnectAddress = newConnectAddress;
    streamOutgoingConnectionNumber = newOutgoingConnectionNumber;
    sentCount = 0;

    // Write the new header.
    auto header = kj::heap<Header>(
        network.publicKey, streamOutgoingConnectionNumber, minIgnoredConnectionNumber,
        SimpleAddress::getLocal(*stream), peerKey, network.privateKey);
    previousWrite = stream->write(header.get(), sizeof(Header)).attach(kj::mv(header));
  }

  void resendOptimisticMessages() {
    // Send any messages we haven't already sent on this stream.
    for (uint i = sentCount; i < optimisticMessages.size(); i++) {
      optimisticMessages[i]->sendOnCurrentStream();
    }
    sentCount = optimisticMessages.size();
  }

  void setAuthenticated() {
    KJ_ASSERT(state != AUTHENTICATED,
        "successfully reached 'authenticated' state twice; shouldn't be possible");
    KJ_ASSERT(state != FAILED);

    state = AUTHENTICATED;

    resendOptimisticMessages();

    // We can drop these now.
    optimisticMessages = kj::Vector<kj::Own<OutgoingMessageImpl>>();

    // We're now accepting messages on this connection.
    minIgnoredConnectionNumber = streamOutgoingConnectionNumber + 1;

    // We can start reading!
    handshakeDoneFulfiller->fulfill();
  }

  void taskFailed(kj::Exception&& exception) override {
    // This method is called when our attempt to establish a new outbound connection fails. In
    // the meantime, though, we may have received and authenticated an inbound connection from
    // the same peer, in which case we can ignore the outbound failure.

    if (state == AUTHENTICATED) {
      KJ_LOG(WARNING, "ignoring connection setup failure because we successfully set up some "
                      "other, equivalent connection", exception);
    } else if (state == FAILED) {
      KJ_LOG(WARNING, "ignoring connection setup failure because we've already failed", exception);
    } else {
      state = FAILED;
      failureReason = kj::mv(exception);
      optimisticMessages = kj::Vector<kj::Own<OutgoingMessageImpl>>();
      minIgnoredConnectionNumber = streamOutgoingConnectionNumber + 1;
      handshakeDoneFulfiller->fulfill();
    }
  }

  class OutgoingMessageImpl final
      : public capnp::OutgoingRpcMessage, public kj::Refcounted {
  public:
    OutgoingMessageImpl(ConnectionImpl& connection, uint firstSegmentWordSize)
        : connection(connection),
          message(firstSegmentWordSize == 0 ? capnp::SUGGESTED_FIRST_SEGMENT_WORDS
                                            : firstSegmentWordSize) {}

    capnp::AnyPointer::Builder getBody() override {
      return message.getRoot<capnp::AnyPointer>();
    }

    kj::ArrayPtr<kj::Maybe<kj::Own<capnp::ClientHook>>> getCapTable() override {
      return message.getCapTable();
    }

    void send() override {
      if (connection.state != AUTHENTICATED) {
        connection.optimisticMessages.add(kj::addRef(*this));

        if (connection.state != OPTIMISTIC) {
          // Not connected, nothing more to do.
          return;
        }

        ++connection.sentCount;
      }

      sendOnCurrentStream();
    }

    void sendOnCurrentStream() {
      connection.sentConnectionNumber = kj::min(
          connection.sentConnectionNumber, connection.streamOutgoingConnectionNumber);

      connection.previousWrite = KJ_ASSERT_NONNULL(connection.previousWrite, "already shut down")
          .then([&]() {
        // Note that if the write fails, all further writes will be skipped due to the exception.
        // We never actually handle this exception because we assume the read end will fail as well
        // and it's cleaner to handle the failure there.
        return capnp::writeMessage(*connection.stream, message);
      }).attach(kj::addRef(*this))
        // Note that it's important that the eagerlyEvaluate() come *after* the attach() because
        // otherwise the message (and any capabilities in it) will not be released until a new
        // message is written! (Kenton once spent all afternoon tracking this down...)
        .eagerlyEvaluate(nullptr);
    }

  private:
    ConnectionImpl& connection;
    capnp::MallocMessageBuilder message;
  };

  class IncomingMessageImpl final: public capnp::IncomingRpcMessage {
  public:
    IncomingMessageImpl(kj::Own<capnp::MessageReader> message): message(kj::mv(message)) {}

    capnp::AnyPointer::Reader getBody() override {
      return message->getRoot<capnp::AnyPointer>();
    }

    void initCapTable(kj::Array<kj::Maybe<kj::Own<capnp::ClientHook>>>&& capTable) override {
      message->initCapTable(kj::mv(capTable));
    }

  private:
    kj::Own<capnp::MessageReader> message;
  };
};

auto VatNetwork::connect(VatPath::Reader hostId) -> kj::Maybe<kj::Own<Connection>> {
  PublicKey peerKey(hostId.getId());
  if (peerKey == publicKey) {
    // Requested connection to self.
    return nullptr;
  }

  SimpleAddress peerAddr(hostId.getAddress());

  auto& slot = connectionMap->map[peerKey];

  uint64_t oldMinConnectionNumber = 0;

  KJ_IF_MAYBE(connection, slot) {
    if (connection->get()->connect(peerAddr)) {
      return kj::Own<Connection>(kj::addRef(**connection));
    } else {
      // This connection is dead. Drop it.
      oldMinConnectionNumber = connection->get()->getMinConnectionNumber();
      slot = nullptr;
    }
  }

  auto connection = kj::refcounted<ConnectionImpl>(*this, peerKey, oldMinConnectionNumber);
  connection->connect(peerAddr);
  slot = kj::addRef(*connection);
  return kj::Own<Connection>(kj::mv(connection));
}

auto VatNetwork::accept() -> kj::Promise<kj::Own<Connection>> {
  return connectionReceiver->accept().then([this](kj::Own<kj::AsyncIoStream>&& stream)
      -> kj::Promise<kj::Own<Connection>> {
    // TODO(security): Someone could DoS this by connecting and not sending any data. We really
    //   need to start listening for the next connection while waiting for the handshake on this
    //   one, but then we have to set up a producer/consumer queue for incoming connections. For
    //   now we use a timeout.

    auto header = kj::heap<Header>(nullptr);

    // Use evalNow() to catch exceptions here.
    auto promise = kj::evalNow([&]() {
      return stream->read(header.get(), sizeof(Header));
    });

    // By the time accept() completes, the other end should have already sent a header, so we can
    // expect the header to show up quickly even if the round trip time is long.
    promise = timer.timeoutAfter(100 * kj::MILLISECONDS, kj::mv(promise));

    return promise.then([this,KJ_MVCAP(header),KJ_MVCAP(stream)]() mutable
                        -> kj::Promise<kj::Own<Connection>> {
      auto verifiedKey = KJ_ASSERT_NONNULL(
          header->verify(privateKey, SimpleAddress::getPeer(*stream)),
          "Incoming connection had invalid handshake header.");

      // Check that connectionNumber is correctly odd or even.
      bool isOdd = header->getConnectionNumber() % 2;
      bool shouldBeOdd = !(verifiedKey < publicKey);
      KJ_ASSERT(isOdd == shouldBeOdd, "Bad connection number on incoming connection.");

      auto& slot = connectionMap->map[verifiedKey];
      uint64_t oldMinConnectionNumber = 0;
      KJ_IF_MAYBE(connection, slot) {
        if (connection->get()->accept(kj::mv(stream),
              header->getConnectionNumber(), header->getMinIgnoredConnection())) {
          // This is not a new connection, so don't return it. Keep waiting for something new.
          return accept();
        } else {
          // This connection is dead. Drop it.
          oldMinConnectionNumber = connection->get()->getMinConnectionNumber();
          slot = nullptr;
        }
      }

      auto connection = kj::refcounted<ConnectionImpl>(*this, verifiedKey, oldMinConnectionNumber);
      KJ_ASSERT(connection->accept(kj::mv(stream),
          header->getConnectionNumber(), header->getMinIgnoredConnection()));
      slot = kj::addRef(*connection);
      return kj::Own<Connection>(kj::mv(connection));
    }).catch_([this](kj::Exception&& exception) {
      KJ_LOG(ERROR, "accepting connection failed", exception);
      return accept();
    });
  });
}

}  // namespace blackrock
}  // namespace sandstorm
