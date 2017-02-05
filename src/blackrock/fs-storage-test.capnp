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

@0xed33f8595b36bba5;

$import "/capnp/c++.capnp".namespace("blackrock");
using Storage = import "storage.capnp";

struct TestStoredObject {
  text @0 :Text;
  sub1 @1 :Storage.OwnedAssignable(TestStoredObject);
  sub2 @2 :Storage.OwnedAssignable(TestStoredObject);
  volume @3 :Storage.OwnedVolume;
}
