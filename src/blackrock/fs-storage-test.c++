// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "fs-storage.h"
#include <kj/test.h>
#include <sandstorm/util.h>
#include <stdlib.h>
#include <errno.h>
#include <kj/async-io.h>

namespace blackrock {
namespace {

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
};

KJ_TEST("basic storage test") {
  StorageTestFixture env;


}

}  // namespace
}  // namespace blackrock
