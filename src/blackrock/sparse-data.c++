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

#include <blackrock/sparse-data.capnp.h>
#include <kj/main.h>
#include <kj/encoding.h>
#include <sandstorm/util.h>
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <unistd.h>
#include <errno.h>

namespace blackrock {

class SparseDataMain {
  // Main class for a simple program that produces a SparseData from an input sparse file.
  // The output is written as a single-segment message (no leading segment table).

public:
  SparseDataMain(kj::ProcessContext& context): context(context) {}

  kj::MainFunc getMain() {
    return kj::MainBuilder(context, "unknown version",
                           "Given a sparse file, output (on stdout) a blackrock::SparseData "
                           "Cap'n Proto representation of the file content.")
        .expectArg("<file>", KJ_BIND_METHOD(*this, run))
        .build();
  }

  kj::MainBuilder::Validity run(kj::StringPtr arg) {
    auto fd = sandstorm::raiiOpen(arg, O_RDONLY | O_CLOEXEC);

    capnp::MallocMessageBuilder message(1 << 17);  // start with 1MB
    auto root = message.getRoot<SparseData>();
    auto orphanage = message.getOrphanage();

    kj::Vector<Chunk> chunks;
    Chunk chunk;
    chunk.offset = 0;

    kj::byte block[4096];

    off_t offset = 0;
    for (;;) {
    retry:
      offset = lseek(fd, offset, SEEK_DATA);
      if (offset < 0) {
        int error = errno;
        if (error == EINTR) {
          goto retry;
        } else if (error == ENXIO) {
          // reached EOF
          break;
        } else {
          KJ_FAIL_SYSCALL("lseek", error);
        }
      }

      KJ_ASSERT(offset % sizeof(block) == 0);

      size_t n = kj::FdInputStream(fd.get()).tryRead(block, sizeof(block), sizeof(block));
      KJ_ASSERT(n > 0);

      KJ_LOG(INFO, kj::hex((uint64_t)offset / sizeof(block)), kj::encodeHex(block));

      for (kj::byte b: block) {
        if (b != 0) {
          // This block has non-zero bytes. We need to add it to the results. Note that we write
          // a whole block even if it contains runs of zeros because block-aligned writes probably
          // will make our main use case (initializing ext4 block devices) more efficient.
          if (chunk.data == nullptr) {
          newChunk:
            chunk.offset = offset;
            chunk.data = orphanage.newOrphanCopy(capnp::Data::Reader(block, n));
          } else {
            size_t chunkSize = chunk.data.getReader().size();
            if (chunk.offset + chunkSize == offset) {
              // Extend the chunk.
              chunk.data.truncate(chunkSize + n);
              memcpy(chunk.data.get().begin() + chunkSize, block, n);
            } else {
              // Start new chunk.
              chunks.add(kj::mv(chunk));
              goto newChunk;
            }
          }
          break;
        }
      }

      offset += n;
    }

    if (chunk.data != nullptr) {
      chunks.add(kj::mv(chunk));
    }

    auto list = root.initChunks(chunks.size());
    for (auto i: kj::indices(chunks)) {
      auto chunkBuilder = list[i];
      chunkBuilder.setOffset(chunks[i].offset);
      chunkBuilder.adoptData(kj::mv(chunks[i].data));
    }

    capnp::writeMessageToFd(STDOUT_FILENO, message);

    return true;
  }

private:
  kj::ProcessContext& context;

  struct Chunk {
    uint64_t offset;
    capnp::Orphan<capnp::Data> data;
  };
};

}  // namespace blackrock

KJ_MAIN(blackrock::SparseDataMain)
