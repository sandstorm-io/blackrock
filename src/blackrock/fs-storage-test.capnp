# Sandstorm Blackrock
# Copyright (c) 2014 Sandstorm Development Group, Inc.
# All Rights Reserved

@0xed33f8595b36bba5;

$import "/capnp/c++.capnp".namespace("blackrock");
using Storage = import "storage.capnp";

struct TestStoredObject {
  text @0 :Text;
  sub1 @1 :Storage.OwnedAssignable(TestStoredObject);
  sub2 @2 :Storage.OwnedAssignable(TestStoredObject);
  volume @3 :Storage.OwnedVolume;
}
