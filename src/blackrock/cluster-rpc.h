// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_CLUSTERRPC_H_
#define BLACKROCK_CLUSTERRPC_H_

#include "common.h"
#include <capnp/rpc.h>
#include <capnp/message.h>
#include <blackrock/cluster-rpc.capnp.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <kj/async-io.h>

namespace blackrock {

class SimpleAddress {
public:
  SimpleAddress(decltype(nullptr)) {}
  SimpleAddress(struct sockaddr_in ip4);
  SimpleAddress(struct sockaddr_in6 ip6);
  SimpleAddress(struct sockaddr& addr, socklen_t addrLen);
  SimpleAddress(Address::Reader reader);

  static SimpleAddress getPeer(kj::AsyncIoStream& socket);
  static SimpleAddress getLocal(kj::AsyncIoStream& socket);
  static SimpleAddress getWildcard(sa_family_t family);
  static SimpleAddress getLocalhost(sa_family_t family);

  inline sa_family_t family() const { return addr.sa_family; }

  void setPort(uint16_t port);

  void copyTo(Address::Builder builder) const;

  static constexpr size_t FLAT_SIZE = 18;
  void getFlat(byte* target) const;

  kj::Own<kj::NetworkAddress> onNetwork(kj::Network& network);

  bool operator==(const SimpleAddress& other) const;
  inline bool operator!=(const SimpleAddress& other) const { return !operator==(other); }

private:
  union {
    struct sockaddr addr;
    struct sockaddr_in ip4;
    struct sockaddr_in6 ip6;
  };
};

class VatNetwork final: public capnp::VatNetwork<VatPath, ProvisionId, RecipientId,
                                                 ThirdPartyCapId, JoinResult> {
public:
  VatNetwork(kj::Network& network, kj::Timer& timer, SimpleAddress address);
  // Create a new VatNetwork exported on the given local address. If the port is zero, an arbitrary
  // unused port will be chosen.

  ~VatNetwork();

  VatPath::Reader getSelf() { return self.getRoot<VatPath>(); }

  kj::Maybe<kj::Own<Connection>> connect(VatPath::Reader hostId) override;
  kj::Promise<kj::Own<Connection>> accept() override;

private:
  class LittleEndian64;
  class Mac;
  class SymmetricKey;
  class PrivateKey;
  class PublicKey;
  class Header;

  class PublicKey {
  public:
    inline PublicKey(decltype(nullptr)) {}
    PublicKey(VatId::Reader id);

    void copyTo(VatId::Builder id);

    inline bool operator<(const PublicKey& other) const {
      return memcmp(key, other.key, sizeof(key)) < 0;
    }
    inline bool operator==(const PublicKey& other) const {
      return memcmp(key, other.key, sizeof(key)) == 0;
    }
    inline bool operator!=(const PublicKey& other) const {
      return memcmp(key, other.key, sizeof(key)) != 0;
    }

    struct Hash;

  private:
    friend class PrivateKey;

    explicit PublicKey(const byte* privateBytes);
    byte key[32];
  };

  class PrivateKey {
  public:
    PrivateKey();
    ~PrivateKey();
    KJ_DISALLOW_COPY(PrivateKey);

    PublicKey getPublic() const;
    SymmetricKey getSharedSecret(PublicKey otherPublic) const;

  private:
    byte* key;  // Allocated with sodium_malloc.
  };

  class PinnedMemory {
    // Some memory that will be pinned into RAM (prevented from swapping). Good for storing keys.
  public:
    explicit PinnedMemory(size_t size);
    ~PinnedMemory();

    template <typename T>
    inline T* as() { return reinterpret_cast<T*>(location); }

  private:
    void* location;
    size_t size;
  };

  class ConnectionImpl;
  struct ConnectionMap;

  kj::Network& network;
  kj::Timer& timer;
  PrivateKey privateKey;
  PublicKey publicKey;
  SimpleAddress address;
  capnp::MallocMessageBuilder self;
  kj::Own<kj::ConnectionReceiver> connectionReceiver;
  kj::Own<ConnectionMap> connectionMap;
};

}  // namespace blackrock

#endif // BLACKROCK_CLUSTERRPC_H_
