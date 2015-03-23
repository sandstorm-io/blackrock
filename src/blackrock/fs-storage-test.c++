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

class Tempdir {
public:
  Tempdir() {
    char path[] = "/tmp/fs-storage-test.XXXXXX";
    if (mkdtemp(path) == nullptr) {
      KJ_FAIL_SYSCALL("mkdtemp", errno);
    }

    this->path = kj::heapString(path);
  }

  ~Tempdir() {
    sandstorm::recursivelyDelete(path);
  }

  operator const char*() { return path.cStr(); }
  operator kj::StringPtr() { return path; }

private:
  kj::String path;
};

class TempdirFd {
public:
  TempdirFd(): fd(sandstorm::raiiOpen(dir, O_RDONLY | O_DIRECTORY)) {}

  operator int() { return fd; }

private:
  Tempdir dir;
  kj::AutoCloseFd fd;
};

struct StorageTestFixture {
  StorageTestFixture()
      : io(kj::setupAsyncIo()),
        journal(sandstorm::raiiOpen("/tmp", O_RDWR | O_TMPFILE)),
        storage(staging, main, deathRow, journal,
                io.unixEventPort, io.provider->getTimer(),
                nullptr) {}

  kj::AsyncIoContext io;
  TempdirFd staging;
  TempdirFd main;
  TempdirFd deathRow;
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

KJ_TEST("set root") {
  StorageTestFixture env;

  FilesystemStorage::ObjectKey rootKey =
      env.storage.setRoot(env.newTextObject("foo")).wait(env.io.waitScope);

  {
    auto root = env.storage.getRoot<TestStoredObject>(rootKey);
    auto response = root.getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(response.getValue().getText() == "foo");

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

    promise.wait(env.io.waitScope);
  }

  env.io.provider->getTimer().afterDelay(10 * kj::MILLISECONDS).wait(env.io.waitScope);

  {
    auto root = env.storage.getRoot<TestStoredObject>(rootKey);
    auto response = root.getRequest().send().wait(env.io.waitScope);
    auto value = response.getValue();
    KJ_EXPECT(value.getText() == "bar");

    auto subResponse1 = value.getSub1().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse1.getValue().getText() == "baz");
    auto subResponse2 = value.getSub2().getRequest().send().wait(env.io.waitScope);
    KJ_EXPECT(subResponse2.getValue().getText() == "qux");
  }
}

}  // namespace
}  // namespace blackrock
