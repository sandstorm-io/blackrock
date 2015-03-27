// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_COMMON_H_
#define BLACKROCK_COMMON_H_

#include <kj/common.h>
#include <kj/io.h>
#include <inttypes.h>

namespace blackrock {

#if __QTCREATOR
#define KJ_MVCAP(var) var
// QtCreator dosen't understand C++14 syntax yet.
#else
#define KJ_MVCAP(var) var = ::kj::mv(var)
// Capture the given variable by move.  Place this in a lambda capture list.  Requires C++14.
//
// TODO(cleanup):  Move to libkj.
#endif

using kj::uint;
using kj::byte;

kj::AutoCloseFd newEventFd(uint value, int flags);
uint64_t readEvent(int fd);
void writeEvent(int fd, uint64_t value);
// TODO(cleanup): Find a better home for these.

}  // namespace blackrock

#endif // BLACKROCK_COMMON_H_
