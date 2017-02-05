// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
