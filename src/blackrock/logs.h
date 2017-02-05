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

#ifndef BLACKROCK_LOGS_H_
#define BLACKROCK_LOGS_H_

#include "common.h"
#include <kj/async-io.h>
#include <set>

namespace sandstorm {
  class Subprocess;
}

namespace blackrock {

class SimpleAddress;

class LogSink: private kj::TaskSet::ErrorHandler {
public:
  LogSink();

  kj::Promise<void> acceptLoop(kj::Own<kj::ConnectionReceiver> receiver);

private:
  class ClientHandler;

  std::set<kj::String> namesSeen;

  kj::TaskSet tasks;

  void write(kj::ArrayPtr<const char> part1, kj::ArrayPtr<const char> part2 = nullptr);
  // Write a line to the log file, prefixed by a timestamp.

  void taskFailed(kj::Exception&& exception) override;
};

void rotateLogs(int input, int logDirFd);
// Read logs on `input` and write them to files in `logDirFd`, rotated to avoid any file becoming
// overly large.

void runLogClient(kj::StringPtr name, kj::StringPtr logAddressFile, kj::StringPtr backlogDir);
// Reads logs from standard input and upload them to the log sink server, reconnecting to the
// server as needed, buffering logs to a local file when the log server is unreachable. Note that
// some logs may be lost around the moment of a disconnect; this is not intended to be 100%
// reliable, only as reliable as is reasonable.
//
// `logAddressFile` is the name of a file on the hard drive which contains the address (in
// SimpleAddress format). The file is re-read every time a reconnect is attempted. This allows an
// external entity to update the log server address without restarting the process.

} // namespace blackrock

#endif // BLACKROCK_LOGS_H_
