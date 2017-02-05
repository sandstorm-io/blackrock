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

#ifndef BLACKROCK_COMMON_H_
#define BLACKROCK_COMMON_H_

#include <kj/common.h>
#include <kj/io.h>
#include <inttypes.h>

namespace blackrock {

#define KJ_MVCAP(var) var = ::kj::mv(var)
// Capture the given variable by move.  Place this in a lambda capture list.  Requires C++14.
//
// TODO(cleanup):  Move to libkj.

using kj::uint;
using kj::byte;

kj::AutoCloseFd newEventFd(uint value, int flags);
uint64_t readEvent(int fd);
void writeEvent(int fd, uint64_t value);
// TODO(cleanup): Find a better home for these.

}  // namespace blackrock

#endif // BLACKROCK_COMMON_H_
