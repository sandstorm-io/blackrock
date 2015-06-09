// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "fs-storage.h"
#include <kj/test.h>
#include <sandstorm/util.h>
#include <stdlib.h>
#include <errno.h>
#include <kj/async-io.h>
#include <sched.h>
#include <sys/mount.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fs-storage-test.capnp.h"

namespace blackrock {
namespace {

#if 0
// TODO(someday): Currently this trick does not work because tmpfs does not allow user-defined
//   xattrs. A kernel patch like this is needed:
//   https://dl.dropboxusercontent.com/u/61413222/0001-Enable-user-xattr-for-tmpfs.patch

class UnshareTmp {
  // Overmounts /tmp with a tmpfs at startup.
public:
  UnshareTmp() {
    KJ_SYSCALL(unshare(CLONE_NEWUSER | CLONE_NEWNS));
    KJ_SYSCALL(mount("fs-storage-test-tmp", "/tmp", "tmpfs", MS_NOSUID, nullptr));
  }
};
UnshareTmp unshareTmp;
#endif


struct TestTempdir {
  static constexpr char PATH[] = "/tmp/blackrock-fs-storage-test";
  kj::AutoCloseFd fd;

  TestTempdir() {
    if (access(PATH, F_OK) >= 0) {
      sandstorm::recursivelyDelete(PATH);
    }
    KJ_SYSCALL(mkdir(PATH, 0777));
    fd = sandstorm::raiiOpen(PATH, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  }

  // We don't delete on shutdown because it's useful to poke at the files for debugging. We always
  // use the same directory so it will be deleted on the next run.
};
constexpr char TestTempdir::PATH[];

TestTempdir testTempdir;
// For this test, we set up a temporary directory once for the whole test, and each test case runs
// on it. This means that each test case will potentially see the data left from the previous.

struct StorageTestFixture {
  StorageTestFixture()
      : io(kj::setupAsyncIo()),
        storage(kj::heap<FilesystemStorage>(testTempdir.fd,
                io.unixEventPort, io.provider->getTimer(),
                nullptr)),
        factory(storage.getFactoryRequest().send().getFactory()) {}

  kj::AsyncIoContext io;

  StorageRootSet::Client storage;
  StorageFactory::Client factory;

  template <typename InitFunc>
  OwnedAssignable<TestStoredObject>::Client newObject(InitFunc&& init) {
    auto req = factory.newAssignableRequest<TestStoredObject>();
    auto value = req.getInitialValue();
    init(value);
    return req.send().getAssignable();
  }

  OwnedAssignable<TestStoredObject>::Client newTextObject(kj::StringPtr text) {
    return newObject([&](auto value) { value.setText(text); });
  }

  void setRoot(kj::StringPtr name, OwnedStorage<Assignable<TestStoredObject>>::Client object) {
    auto req = storage.setRequest<Assignable<TestStoredObject>>();
    req.setName(name);
    req.setObject(kj::mv(object));
    req.send().wait(io.waitScope);
  }

  OwnedAssignable<TestStoredObject>::Client getRoot(kj::StringPtr name) {
    auto req = storage.getRequest<Assignable<TestStoredObject>>();
    req.setName(name);
    return req.send().getObject().castAs<OwnedAssignable<TestStoredObject>>();
  }
};

KJ_TEST("basic assignables") {
  StorageTestFixture env;

  env.setRoot("root", env.newTextObject("foo"));

  {
    auto root = env.getRoot("root");
    auto response = root.getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(response.getValue().getText() == "foo");

    KJ_EXPECT(root.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096);

    // Modify the value.
    auto req = response.getSetter().setRequest();
    auto newVal = req.initValue();
    newVal.setText("bar");
    newVal.setSub1(env.newTextObject("baz"));
    newVal.setSub2(env.newObject([&](auto value) {
      value.setText("qux");
      value.setSub1(env.newTextObject("corge"));
    }));
    auto promise = req.send();

    auto response2 = root.getRequest().send().wait(env.io.waitScope);
    auto readback = response2.getValue();
    KJ_EXPECT(readback.getText() == "bar");

    auto subResponse1 = readback.getSub1().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse1.getValue().getText() == "baz");
    auto subResponse2 = readback.getSub2().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse2.getValue().getText() == "qux");

    KJ_EXPECT(root.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes()==4096*4);

    promise.wait(env.io.waitScope);
  }

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);

  {
    auto root = env.getRoot("root");
    auto response = root.getRequest().send().wait(env.io.waitScope);
    auto value = response.getValue();
    KJ_EXPECT(value.getText() == "bar");

    auto subResponse1 = value.getSub1().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse1.getValue().getText() == "baz");
    auto subResponse2 = value.getSub2().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse2.getValue().getText() == "qux");
  }
}

// Current state of storage:
//
// root = (text = "bar", sub1 = x, sub2 = y)
// x = (text = "baz", sub1 = z)
// y = (text = "qux")
// z = (text = "corge")

KJ_TEST("use after reload") {
  StorageTestFixture env;

  auto root = env.getRoot("root");
  auto response = root.getRequest().send().wait(env.io.waitScope);
  auto value = response.getValue();
  KJ_EXPECT(value.getText() == "bar");

  auto subResponse1 = value.getSub1().getRequest().send().wait(env.io.waitScope);
  KJ_EXPECT(subResponse1.getValue().getText() == "baz");
  auto subResponse2 = value.getSub2().getRequest().send().wait(env.io.waitScope);
  KJ_EXPECT(subResponse2.getValue().getText() == "qux");

  auto subSubResponse = subResponse2.getValue().getSub1()
      .getRequest().send().wait(env.io.waitScope);
  KJ_EXPECT(subSubResponse.getValue().getText() == "corge");

  KJ_EXPECT(root.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096*4);
}

// Current state of storage:
//
// root = (text = "bar", sub1 = x, sub2 = y)
// x = (text = "baz")
// y = (text = "qux", sub1 = z)
// z = (text = "corge")

KJ_TEST("delete some children") {
  StorageTestFixture env;

  auto root = env.getRoot("root");

  auto main = sandstorm::raiiOpenAt(testTempdir.fd, "main", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  auto deathRow = sandstorm::raiiOpenAt(testTempdir.fd, "death-row",
      O_RDONLY | O_DIRECTORY | O_CLOEXEC);

  KJ_EXPECT(sandstorm::listDirectoryFd(main).size() == 4);
  KJ_EXPECT(sandstorm::listDirectoryFd(deathRow).size() == 0);

  OwnedAssignable<TestStoredObject>::Client zombie = ({
    auto response = root.getRequest().send().wait(env.io.waitScope);
    auto req = response.getSetter().setRequest();
    auto value = req.initValue();
    value.setText("quux");
    value.setSub1(response.getValue().getSub1());
    // don't set sub2
    req.send().wait(env.io.waitScope);

    uint64_t size = root.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
    KJ_EXPECT(size == 4096 * 2, size);

    response.getValue().getSub2();
  });

  // Death row is cleaned up asynchronously, so wait a bit before checking.
  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);

  // Verify that tree was deleted. We'll need to give the death-row deleter thread some time, too.
  KJ_EXPECT(sandstorm::listDirectoryFd(main).size() == 2);
  KJ_EXPECT(sandstorm::listDirectoryFd(deathRow).size() == 0);

  // Try overwriting our zombie reference.
  {
    auto req = zombie.asSetterRequest().send().getSetter().setRequest();
    req.initValue().setText("doesnt matter already deleted");
    req.send().wait(env.io.waitScope);
  }

  // That shouldn't have added a file to main.
  KJ_EXPECT(sandstorm::listDirectoryFd(main).size() == 2);

  // It adds a file directly to death row, which should get cleaned up.
  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);
  KJ_EXPECT(sandstorm::listDirectoryFd(deathRow).size() == 0);

  // Expect parent's size wasn't unexpectedly modified.
  KJ_EXPECT(root.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096*2);
}

struct TestByteStream final: public sandstorm::ByteStream::Server, public kj::Refcounted {
  kj::Promise<void> write(WriteContext context) override {
    KJ_ASSERT(!gotDone);
    content.addAll(context.getParams().getData());
    return kj::READY_NOW;
  }

  kj::Promise<void> done(DoneContext context) override {
    KJ_ASSERT(!gotDone);
    gotDone = true;
    return kj::READY_NOW;
  }

  kj::Promise<void> expectSize(ExpectSizeContext context) override {
    KJ_ASSERT(!gotDone);
    KJ_ASSERT(expectedSize == nullptr);
    expectedSize = context.getParams().getSize();
    return kj::READY_NOW;
  }

  kj::Vector<byte> content;
  bool gotDone = false;
  kj::Maybe<uint64_t> expectedSize;
};

KJ_TEST("blob") {
  StorageTestFixture env;

  auto blob = ({
    auto req = env.factory.newBlobRequest();
    req.setContent(kj::StringPtr("foobar").asBytes());
    req.send().getBlob();
  });

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  auto stream = kj::refcounted<TestByteStream>();
  {
    auto req = blob.writeToRequest();
    req.setStream(kj::addRef(*stream));
    req.send().wait(env.io.waitScope);
  }
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foobar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 6);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  {
    auto req = env.storage.setRequest<Blob>();
    req.setName("blob");
    req.setObject(kj::mv(blob));
    req.send().wait(env.io.waitScope);
  }
}

KJ_TEST("blob load") {
  StorageTestFixture env;

  auto blob = ({
    auto req = env.storage.getRequest<Assignable<TestStoredObject>>();
    req.setName("blob");
    req.send().getObject().castAs<OwnedBlob>();
  });

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  auto stream = kj::refcounted<TestByteStream>();
  {
    auto req = blob.writeToRequest();
    req.setStream(kj::addRef(*stream));
    req.send().wait(env.io.waitScope);
  }
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foobar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 6);
}

KJ_TEST("blob partial download") {
  StorageTestFixture env;

  auto blob = ({
    auto req = env.factory.newBlobRequest();
    req.setContent(kj::StringPtr("foobar").asBytes());
    req.send().getBlob();
  });

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  auto stream = kj::refcounted<TestByteStream>();
  {
    auto req = blob.writeToRequest();
    req.setStartAtOffset(3);
    req.setStream(kj::addRef(*stream));
    req.send().wait(env.io.waitScope);
  }
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "bar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 3);
}

KJ_TEST("blob streaming initialization") {
  StorageTestFixture env;

  auto blobInit = env.factory.uploadBlobRequest().send();

  auto blob = blobInit.getBlob();

  {
    auto upStream = blobInit.getStream();
    {
      auto req = upStream.writeRequest();
      req.setData(kj::StringPtr("foobar").asBytes());
      req.send().wait(env.io.waitScope);
    }
    upStream.doneRequest().send().wait(env.io.waitScope);
  }
  blobInit.wait(env.io.waitScope);

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  auto stream = kj::refcounted<TestByteStream>();
  {
    auto req = blob.writeToRequest();
    req.setStream(kj::addRef(*stream));
    req.send().wait(env.io.waitScope);
  }
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foobar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 6);
}

KJ_TEST("blob interleaved initialization") {
  StorageTestFixture env;

  auto blobInit = env.factory.uploadBlobRequest().send();

  auto blob = blobInit.getBlob();

  auto upStream = blobInit.getStream();
  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("foo").asBytes());
    req.send().wait(env.io.waitScope);
  }

  auto stream = kj::refcounted<TestByteStream>();
  auto download = ({
    auto req = blob.writeToRequest();
    req.setStream(kj::addRef(*stream));
    req.send();
  });

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foo");
  KJ_EXPECT(!stream->gotDone);
  KJ_EXPECT(stream->expectedSize == nullptr);

  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("bar").asBytes());
    req.send().wait(env.io.waitScope);
  }
  upStream.doneRequest().send().wait(env.io.waitScope);
  blobInit.wait(env.io.waitScope);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foobar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(stream->expectedSize == nullptr);
}

KJ_TEST("blob interleaved initialization with expected size") {
  StorageTestFixture env;

  auto blobInit = env.factory.uploadBlobRequest().send();

  auto blob = blobInit.getBlob();

  auto upStream = blobInit.getStream();
  {
    auto req = upStream.expectSizeRequest();
    req.setSize(6);
    req.send().wait(env.io.waitScope);
  }
  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("foo").asBytes());
    req.send().wait(env.io.waitScope);
  }

  auto stream = kj::refcounted<TestByteStream>();
  auto download = ({
    auto req = blob.writeToRequest();
    req.setStream(kj::addRef(*stream));
    req.send();
  });

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);
  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foo");
  KJ_EXPECT(!stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 6);

  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("bar").asBytes());
    req.send().wait(env.io.waitScope);
  }
  upStream.doneRequest().send().wait(env.io.waitScope);
  blobInit.wait(env.io.waitScope);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "foobar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 6);
}

KJ_TEST("blob interleaved initialization partial download") {
  StorageTestFixture env;

  auto blobInit = env.factory.uploadBlobRequest().send();

  auto blob = blobInit.getBlob();

  auto upStream = blobInit.getStream();
  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("foo").asBytes());
    req.send().wait(env.io.waitScope);
  }

  auto stream = kj::refcounted<TestByteStream>();
  auto download = ({
    auto req = blob.writeToRequest();
    req.setStartAtOffset(4);
    req.setStream(kj::addRef(*stream));
    req.send();
  });

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);
  KJ_EXPECT(stream->content.size() == 0);
  KJ_EXPECT(!stream->gotDone);
  KJ_EXPECT(stream->expectedSize == nullptr);

  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("bar").asBytes());
    req.send().wait(env.io.waitScope);
  }
  upStream.doneRequest().send().wait(env.io.waitScope);
  blobInit.wait(env.io.waitScope);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "ar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(stream->expectedSize == nullptr);
}

KJ_TEST("blob interleaved initialization partial download with expected size") {
  StorageTestFixture env;

  auto blobInit = env.factory.uploadBlobRequest().send();

  auto blob = blobInit.getBlob();

  auto upStream = blobInit.getStream();
  {
    auto req = upStream.expectSizeRequest();
    req.setSize(6);
    req.send().wait(env.io.waitScope);
  }
  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("foo").asBytes());
    req.send().wait(env.io.waitScope);
  }

  auto stream = kj::refcounted<TestByteStream>();
  auto download = ({
    auto req = blob.writeToRequest();
    req.setStartAtOffset(4);
    req.setStream(kj::addRef(*stream));
    req.send();
  });

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);
  KJ_EXPECT(stream->content.size() == 0);
  KJ_EXPECT(!stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 2);

  {
    auto req = upStream.writeRequest();
    req.setData(kj::StringPtr("bar").asBytes());
    req.send().wait(env.io.waitScope);
  }
  upStream.doneRequest().send().wait(env.io.waitScope);
  blobInit.wait(env.io.waitScope);

  uint64_t size = blob.getStorageUsageRequest().send().wait(env.io.waitScope).getTotalBytes();
  KJ_EXPECT(size == 4096, size);

  KJ_EXPECT(blob.getSizeRequest().send().wait(env.io.waitScope).getSize() == 6);

  KJ_EXPECT(kj::heapString(stream->content.asPtr().asChars()) == "ar");
  KJ_EXPECT(stream->gotDone);
  KJ_EXPECT(KJ_ASSERT_NONNULL(stream->expectedSize) == 2);
}

// TODO(test): journal recovery
// TODO(test): recursive delete
// TODO(test): volumes
// TODO(test): outgoing SturdyRefs
// TODO(test): incoming SturdyRefs

}  // namespace
}  // namespace blackrock
