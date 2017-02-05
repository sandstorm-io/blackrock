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

@0x8eb19add5ef5349e;

$import "/capnp/c++.capnp".namespace("blackrock");

using import "sparse-data.capnp".SparseData;

const blankExt4 :SparseData = embed "blank-ext4.sparse";
# blank-ext4.sparse is created by blank-ext4.ekam-rule which runs mkfs.ext4 to create a new
# ext4 FS and then uses sparse-data.c++ to turn its contents into SparseData.
