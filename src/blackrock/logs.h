// Sandstorm Blackrock
// Copyright (c) 2014 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_LOGS_H_
#define BLACKROCK_LOGS_H_

#include "common.h"
#include <kj/async-io.h>

namespace sandstorm {
  class Subprocess;
}

namespace blackrock {

class SimpleAddress;

class LogSink: private kj::TaskSet::ErrorHandler {
public:
  explicit LogSink(kj::Maybe<int> logDirFd);

  kj::Promise<void> acceptLoop(kj::Own<kj::ConnectionReceiver> receiver);

private:
  class ClientHandler;

  kj::Maybe<int> logDirFd;

  struct LogFile {
    kj::AutoCloseFd fd;
    size_t size;
  };

  kj::Maybe<LogFile> currentFile;

  kj::TaskSet tasks;

  void write(kj::ArrayPtr<const char> part1, kj::ArrayPtr<const char> part2 = nullptr);
  // Write a line to the log file, prefixed by a timestamp.

  void taskFailed(kj::Exception&& exception) override;
};

void sendStderrToLogSink(kj::StringPtr name, SimpleAddress destination, kj::StringPtr backlogDir);

} // namespace blackrock

#endif // BLACKROCK_LOGS_H_
