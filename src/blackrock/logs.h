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

void sendStderrToLogSink(kj::StringPtr name, kj::StringPtr logAddressFile,
                         kj::StringPtr backlogDir);
// Redirect standard error to a subprocess which will upload it to the log sink server. The
// subprocess will handle reconnecting to the server as needed, and will buffer logs to a local
// file when the log server is unreachable. Note that some logs may be lost around the moment of
// a disconnect; this is not intended to be 100% reliable, only as reliable as is reasonable.
//
// `logAddressFile` is the name of a file on the hard drive which contains the address (in
// SimpleAddress format). The file is re-read every time a reconnect is attempted. This allows an
// external entity to update the log server address without restarting the process.

} // namespace blackrock

#endif // BLACKROCK_LOGS_H_
