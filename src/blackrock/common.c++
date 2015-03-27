// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "common.h"
#include <sandstorm/util.h>
#include <unistd.h>
#include <sys/eventfd.h>

namespace blackrock {

kj::AutoCloseFd newEventFd(uint value, int flags) {
  int fd;
  KJ_SYSCALL(fd = eventfd(0, flags));
  return kj::AutoCloseFd(fd);
}

uint64_t readEvent(int fd) {
  ssize_t n;
  uint64_t result;
  KJ_SYSCALL(n = read(fd, &result, sizeof(result)));
  KJ_ASSERT(n == 8, "wrong-sized read from eventfd", n);
  return result;
}

void writeEvent(int fd, uint64_t value) {
  ssize_t n;
  KJ_SYSCALL(n = write(fd, &value, sizeof(value)));
  KJ_ASSERT(n == 8, "wrong-sized write on eventfd", n);
}

}  // namespace blackrock
