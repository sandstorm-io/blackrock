# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

@0xb4ec463ef590911d;

$import "/capnp/c++.capnp".namespace("blackrock");

struct SparseData {
  # Represents a chunk of "sparse" data, i.e. bytes with a lot of long runs of zeros. We only
  # include the non-zero bytes.
  #
  # This is used in particular to store a blank ext4 filesystem template directly into the
  # Blackrock binary so that we can quickly format new volumes.

  chunks @0 :List(Chunk);
  struct Chunk {
    offset @0 :UInt64;
    data @1 :Data;
  }
}
