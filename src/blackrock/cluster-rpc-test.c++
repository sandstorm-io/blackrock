// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "cluster-rpc.h"
#include <kj/test.h>

namespace blackrock {
namespace {

struct sockaddr_in localhostIp4() {
  struct sockaddr_in result;
  memset(&result, 0, sizeof(result));
  result.sin_family = AF_INET;
  result.sin_addr.s_addr = htonl(0x7F000001);
  result.sin_port = 0;
  return result;
}

void logException(kj::Exception&& e) {
  KJ_LOG(ERROR, e);
};

kj::Promise<void> pump(kj::AsyncIoStream& in, kj::AsyncIoStream& out) {
  auto buffer = kj::heapArray<byte>(4096);
  auto promise = in.tryRead(buffer.begin(), 1, buffer.size());
  return promise.then([&,KJ_MVCAP(buffer)](size_t n) mutable -> kj::Promise<void> {
    if (n == 0) {
      out.shutdownWrite();
      return kj::READY_NOW;
    } else {
      auto result = out.write(buffer.begin(), n);
      return result.attach(kj::mv(buffer)).then([&]() {
        return pump(in, out);
      });
    }
  }, [&](kj::Exception&& e) {
    // Input died. Close output.
    out.shutdownWrite();
  });
}

struct TestEnv {
  kj::AsyncIoContext ioContext;
  VatNetwork network1;
  VatNetwork network2;
  kj::WaitScope& waitScope;
  kj::TimePoint start;

  TestEnv()
      : ioContext(kj::setupAsyncIo()),
        network1(ioContext.provider->getNetwork(), ioContext.provider->getTimer(), localhostIp4()),
        network2(ioContext.provider->getNetwork(), ioContext.provider->getTimer(), localhostIp4()),
        waitScope(ioContext.waitScope),
        start(ioContext.provider->getTimer().now()) {}

  ~TestEnv() {}

  void sendMessage(VatNetwork::Connection& conn, kj::StringPtr text) {
    auto msg = conn.newOutgoingMessage(32);
    msg->getBody().setAs<capnp::Text>(text);
    msg->send();
  }

  void expectMessage(VatNetwork::Connection& conn, kj::StringPtr expectedText) {
    auto msg = KJ_ASSERT_NONNULL(conn.receiveIncomingMessage().wait(waitScope));
    auto msgText = msg->getBody().getAs<capnp::Text>();
    KJ_EXPECT(msgText == expectedText, msgText, expectedText);
  }

  kj::Promise<void> shutdown(VatNetwork::Connection& conn) KJ_WARN_UNUSED_RESULT {
    // Asynchronously shut down the given connection.
    return conn.shutdown().eagerlyEvaluate(logException);
  }

  void expectShutdown(VatNetwork::Connection& conn) {
    // Expect to receive shutdown on the given connection.
    KJ_EXPECT(conn.receiveIncomingMessage().wait(waitScope) == nullptr);
  }

  kj::Promise<void> nextIo() {
    // Resolves the next time we check for I/O.

    static char scratch[4];
    auto pipe = ioContext.provider->newOneWayPipe();
    auto read = pipe.in->read(&scratch, 4);
    auto write = pipe.out->write("foo", 4);
    auto promises = kj::heapArrayBuilder<kj::Promise<void>>(2);
    promises.add(read.attach(kj::mv(pipe.in)));
    promises.add(write.attach(kj::mv(pipe.out)));
    return kj::joinPromises(promises.finish());
  }

  template <typename T>
  kj::Promise<void> expectNever(
      kj::Promise<T>&& promise, kj::StringPtr what) KJ_WARN_UNUSED_RESULT {
    // Expect that the promise never resolves.
    return promise.then([what](T&&) { KJ_FAIL_ASSERT("unexpected return", what); })
        .eagerlyEvaluate(logException);
  }

  kj::Promise<void> expectNever(
      kj::Promise<void>&& promise, kj::StringPtr what) KJ_WARN_UNUSED_RESULT {
    // Expect that the promise never resolves.
    return promise.then([what]() { KJ_FAIL_ASSERT("unexpected return", what); })
        .eagerlyEvaluate(logException);
  }
};

KJ_TEST("can connect and send messages") {
  TestEnv env;

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  kj::Own<VatNetwork::Connection> conn2 = env.network2.accept().wait(env.waitScope);

  // We send conn2 -> conn1 first to be sure no optimistic messages are used.
  env.sendMessage(*conn2, "foo");
  env.expectMessage(*conn1, "foo");
  env.sendMessage(*conn1, "bar");
  env.expectMessage(*conn2, "bar");
  env.sendMessage(*conn1, "baz");
  env.sendMessage(*conn2, "qux");
  env.expectMessage(*conn2, "baz");
  env.expectMessage(*conn1, "qux");

  // Test clean shutdown.
  auto promise1 = env.shutdown(*conn1);
  env.expectShutdown(*conn2);
  auto promise2 = env.shutdown(*conn2);
  env.expectShutdown(*conn1);
}

KJ_TEST("can optimistically send messages") {
  TestEnv env;

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));

  env.sendMessage(*conn1, "foo");
  env.sendMessage(*conn1, "bar");

  kj::Own<VatNetwork::Connection> conn2 = env.network2.accept().wait(env.waitScope);

  // Verify that the next two messages are received immediately.
  bool didIo = false;
  auto ioPromise = env.nextIo().then([&]() { didIo = true; }).eagerlyEvaluate(nullptr);

  env.expectMessage(*conn2, "foo");
  env.expectMessage(*conn2, "bar");

  KJ_EXPECT(!didIo);

  env.sendMessage(*conn2, "baz");
  env.expectMessage(*conn1, "baz");
  env.sendMessage(*conn1, "qux");
  env.sendMessage(*conn2, "quux");
  env.expectMessage(*conn2, "qux");
  env.expectMessage(*conn1, "quux");

  KJ_EXPECT(didIo);

  // Test clean shutdown.
  auto promise1 = env.shutdown(*conn1);
  env.expectShutdown(*conn2);
  auto promise2 = env.shutdown(*conn2);
  env.expectShutdown(*conn1);
}

KJ_TEST("can connect both ways simultaneously") {
  TestEnv env;

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  kj::Own<VatNetwork::Connection> conn2 =
      KJ_ASSERT_NONNULL(env.network2.connect(env.network1.getSelf()));

  auto promise1 = env.expectNever(env.network1.accept(), "network1.accept() returned");
  auto promise2 = env.expectNever(env.network2.accept(), "network2.accept() returned");

  env.sendMessage(*conn1, "foo");
  env.sendMessage(*conn2, "bar");
  env.expectMessage(*conn2, "foo");
  env.expectMessage(*conn1, "bar");
  env.sendMessage(*conn1, "baz");
  env.sendMessage(*conn2, "qux");
  env.expectMessage(*conn2, "baz");
  env.expectMessage(*conn1, "qux");

  // Test clean shutdown.
  auto promise3 = env.shutdown(*conn1);
  env.expectShutdown(*conn2);
  auto promise4 = env.shutdown(*conn2);
  env.expectShutdown(*conn1);
}

KJ_TEST("rejects bogus send header") {
  TestEnv env;
  VatNetwork network3(env.ioContext.provider->getNetwork(),
                      env.ioContext.provider->getTimer(), localhostIp4());

  KJ_EXPECT_LOG(ERROR, "connection had invalid handshake header");

  capnp::MallocMessageBuilder message;
  message.setRoot(env.network2.getSelf());
  auto path = message.getRoot<VatPath>();
  path.setAddress(network3.getSelf().getAddress());

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(path));

  auto promise1 = env.expectNever(env.network1.accept(), "network1.accept() returned");
  auto promise2 = env.expectNever(env.network2.accept(), "network2.accept() returned");
  auto promise3 = env.expectNever(network3.accept(), "network3.accept() returned");

  env.sendMessage(*conn1, "foo");

  KJ_EXPECT_THROW(DISCONNECTED, conn1->receiveIncomingMessage().wait(env.waitScope));
}

KJ_TEST("rejects bogus reply header") {
  TestEnv env;

  auto listener = env.ioContext.provider->getNetwork()
      .parseAddress("127.0.0.1")
      .wait(env.waitScope)
      ->listen();

  capnp::MallocMessageBuilder message;
  message.setRoot(env.network2.getSelf());
  auto path = message.getRoot<VatPath>();
  path.getAddress().setPort(listener->getPort());

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(path));

  auto promise1 = env.expectNever(env.network1.accept(), "network1.accept() returned");
  auto promise2 = env.expectNever(env.network2.accept(), "network2.accept() returned");

  env.sendMessage(*conn1, "foo");

  auto conn2 = listener->accept().wait(env.waitScope);
  byte header[64];
  conn2->read(header, sizeof(header)).wait(env.waitScope);
  ++header[32];  // increment connection number
  conn2->write(header, sizeof(header)).wait(env.waitScope);

  KJ_EXPECT_THROW_MESSAGE("peer responded with invalid handshake header",
      conn1->receiveIncomingMessage().wait(env.waitScope));
}

KJ_TEST("can't man in the middle") {
  TestEnv env;

  auto listener = env.ioContext.provider->getNetwork()
      .parseAddress("127.0.0.1")
      .wait(env.waitScope)
      ->listen();

  capnp::MallocMessageBuilder message;
  message.setRoot(env.network2.getSelf());
  auto path = message.getRoot<VatPath>();
  auto addr = path.getAddress();
  uint32_t realPort = addr.getPort();
  addr.setPort(listener->getPort());

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(path));

  auto promise1 = env.expectNever(env.network1.accept(), "network1.accept() returned");
  auto promise2 = env.expectNever(env.network2.accept(), "network2.accept() returned");

  env.sendMessage(*conn1, "foo");

  KJ_EXPECT_LOG(ERROR, "connection had invalid handshake header");

  // Receive the connection from network1 and attempt to forward it to network2, thus becoming
  // a MITM.
  auto conn2 = listener->accept().wait(env.waitScope);
  auto conn3 = env.ioContext.provider->getNetwork()
      .parseAddress("127.0.0.1", realPort)
      .wait(env.waitScope)
      ->connect()
      .wait(env.waitScope);
  auto pumpUpTask = pump(*conn2, *conn3).eagerlyEvaluate(nullptr);
  auto pumpDownTask = pump(*conn3, *conn2).eagerlyEvaluate(nullptr);

  KJ_EXPECT_THROW(FAILED, conn1->receiveIncomingMessage().wait(env.waitScope));
}

KJ_TEST("can reconnect after disconnect") {
  TestEnv env;

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  kj::Own<VatNetwork::Connection> conn2 = env.network2.accept().wait(env.waitScope);

  env.sendMessage(*conn1, "foo");
  env.expectMessage(*conn2, "foo");

  // As long as we're still connected, connect() will return the same connection.
  kj::Own<VatNetwork::Connection> conn1b =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  KJ_ASSERT(conn1b.get() == conn1.get());

  // In fact connect() will return the same connection even for the accepter.
  kj::Own<VatNetwork::Connection> conn2b =
      KJ_ASSERT_NONNULL(env.network2.connect(env.network1.getSelf()));
  KJ_ASSERT(conn2b.get() == conn2.get());

  // But let's say we shut down.
  auto promise1 = env.shutdown(*conn1);
  env.expectShutdown(*conn2);

  // Now the next connect creates an all-new connection.
  kj::Own<VatNetwork::Connection> conn1c =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  KJ_ASSERT(conn1c.get() != conn1.get());

  kj::Own<VatNetwork::Connection> conn2c =
      KJ_ASSERT_NONNULL(env.network2.connect(env.network1.getSelf()));
  KJ_ASSERT(conn2c.get() != conn2.get());

  auto promise2 = env.expectNever(env.network1.accept(), "network1.accept() returned");
  auto promise3 = env.expectNever(env.network2.accept(), "network2.accept() returned");

  env.sendMessage(*conn1c, "bar");
  env.expectMessage(*conn2c, "bar");

  // Test clean shutdown.
  auto promise4 = env.shutdown(*conn1c);
  env.expectShutdown(*conn2c);
  auto promise5 = env.shutdown(*conn2c);
  env.expectShutdown(*conn1c);
}

KJ_TEST("can reconnect after authentication failure") {
  TestEnv env;
  VatNetwork network3(env.ioContext.provider->getNetwork(),
                      env.ioContext.provider->getTimer(), localhostIp4());

  KJ_EXPECT_LOG(ERROR, "connection had invalid handshake header");

  capnp::MallocMessageBuilder message;
  message.setRoot(env.network2.getSelf());
  auto path = message.getRoot<VatPath>();
  path.setAddress(network3.getSelf().getAddress());

  kj::Own<VatNetwork::Connection> conn1 =
      KJ_ASSERT_NONNULL(env.network1.connect(path));

  auto promise1 = env.expectNever(network3.accept(), "network3.accept() returned");

  env.sendMessage(*conn1, "foo");

  KJ_EXPECT_THROW(DISCONNECTED, conn1->receiveIncomingMessage().wait(env.waitScope));

  // Now the next connect creates an all-new connection.
  kj::Own<VatNetwork::Connection> conn1b =
      KJ_ASSERT_NONNULL(env.network1.connect(env.network2.getSelf()));
  KJ_ASSERT(conn1b.get() != conn1.get());

  kj::Own<VatNetwork::Connection> conn2 = env.network2.accept().wait(env.waitScope);

  env.sendMessage(*conn1b, "bar");
  env.expectMessage(*conn2, "bar");

  // Test clean shutdown.
  auto promise2 = env.shutdown(*conn1b);
  env.expectShutdown(*conn2);
  auto promise3 = env.shutdown(*conn2);
  env.expectShutdown(*conn1b);
}

}  // namespace
}  // namespace blackrock
