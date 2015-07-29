# Sandstorm Blackrock
# Copyright (c) 2015 Sandstorm Development Group, Inc.
# All Rights Reserved

@0x8eb19add5ef5349e;

$import "/capnp/c++.capnp".namespace("blackrock");

using import "sparse-data.capnp".SparseData;

const blankExt4 :SparseData = embed "blank-ext4.sparse";
# blank-ext4.sparse is created by blank-ext4.ekam-rule which runs mkfs.ext4 to create a new
# ext4 FS and then uses sparse-data.c++ to turn its contents into SparseData.
