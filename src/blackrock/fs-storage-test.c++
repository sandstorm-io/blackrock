// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
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

  TestTempdir() {
    if (access(PATH, F_OK) >= 0) {
      sandstorm::recursivelyDelete(PATH);
    }
    KJ_SYSCALL(mkdir(PATH, 0777));
  }

  kj::AutoCloseFd subdir(kj::StringPtr name) {
    auto fullname = kj::str(PATH, '/', name);
    mkdir(fullname.cStr(), 0777);  // ignore already-exists
    return sandstorm::raiiOpen(fullname, O_RDONLY | O_DIRECTORY);
  }

  FilesystemStorage::ObjectKey rootKey;  // initialized in first test

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
        staging(testTempdir.subdir("staging")),
        main(testTempdir.subdir("main")),
        deathRow(testTempdir.subdir("deathRow")),
        journal(sandstorm::raiiOpen("/tmp", O_RDWR | O_TMPFILE)),
        storage(staging, main, deathRow, journal,
                io.unixEventPort, io.provider->getTimer(),
                nullptr) {}

  kj::AsyncIoContext io;
  kj::AutoCloseFd staging;
  kj::AutoCloseFd main;
  kj::AutoCloseFd deathRow;
  kj::AutoCloseFd journal;
  FilesystemStorage storage;

  template <typename InitFunc>
  OwnedAssignable<TestStoredObject>::Client newObject(InitFunc&& init) {
    auto req = storage.getFactory().newAssignableRequest<TestStoredObject>();
    auto value = req.getInitialValue();
    init(value);
    return req.send().getAssignable();
  }

  OwnedAssignable<TestStoredObject>::Client newTextObject(kj::StringPtr text) {
    return newObject([&](auto value) { value.setText(text); });
  }
};

KJ_TEST("basic assignables") {
  StorageTestFixture env;

  testTempdir.rootKey = env.storage.setRoot(env.newTextObject("foo")).wait(env.io.waitScope);

  {
    auto root = env.storage.getRoot<TestStoredObject>(testTempdir.rootKey);
    auto response = root.getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(response.getValue().getText() == "foo");

    KJ_EXPECT(root.getSizeRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096);

    // Modify the value.
    auto req = response.getSetter().setRequest();
    auto newVal = req.initValue();
    newVal.setText("bar");
    newVal.setSub1(env.newTextObject("baz"));
    newVal.setSub2(env.newTextObject("qux"));
    auto promise = req.send();

    auto response2 = root.getRequest().send().wait(env.io.waitScope);
    auto readback = response2.getValue();
    KJ_EXPECT(readback.getText() == "bar");

    auto subResponse1 = readback.getSub1().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse1.getValue().getText() == "baz");
    auto subResponse2 = readback.getSub2().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse2.getValue().getText() == "qux");

    KJ_EXPECT(root.getSizeRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096 * 3);

    promise.wait(env.io.waitScope);
  }

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);

  {
    auto root = env.storage.getRoot<TestStoredObject>(testTempdir.rootKey);
    auto response = root.getRequest().send().wait(env.io.waitScope);
    auto value = response.getValue();
    KJ_EXPECT(value.getText() == "bar");

    auto subResponse1 = value.getSub1().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse1.getValue().getText() == "baz");
    auto subResponse2 = value.getSub2().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse2.getValue().getText() == "qux");
  }
}

KJ_TEST("use after reload") {
  StorageTestFixture env;

  auto root = env.storage.getRoot<TestStoredObject>(testTempdir.rootKey);
  auto response = root.getRequest().send().wait(env.io.waitScope);
  auto value = response.getValue();
  KJ_EXPECT(value.getText() == "bar");

  auto subResponse1 = value.getSub1().getRequest().send().wait(env.io.waitScope);
  KJ_EXPECT(subResponse1.getValue().getText() == "baz");
  auto subResponse2 = value.getSub2().getRequest().send().wait(env.io.waitScope);
  KJ_EXPECT(subResponse2.getValue().getText() == "qux");

  KJ_EXPECT(root.getSizeRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096 * 3);
}

KJ_TEST("delete some children") {
  StorageTestFixture env;

  auto root = env.storage.getRoot<TestStoredObject>(testTempdir.rootKey);

  {
    auto response = root.getRequest().send().wait(env.io.waitScope);
    auto req = response.getSetter().setRequest();
    auto value = req.initValue();
    value.setText("quux");
    value.setSub1(response.getValue().getSub1());
    // don't set sub2
    req.send().wait(env.io.waitScope);

    KJ_EXPECT(root.getSizeRequest().send().wait(env.io.waitScope).getTotalBytes() == 4096 * 2);

    // TODO(test): verify that sub2 is deleted from disk somehow
  }
}

// TODO(test): journal recovery
// TODO(test): recursive delete
// TODO(test): volumes
// TODO(test): outgoing SturdyRefs
// TODO(test): incoming SturdyRefs

}  // namespace
}  // namespace blackrock
