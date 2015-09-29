// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#ifndef BLACKROCK_BUNDLE_H_
#define BLACKROCK_BUNDLE_H_

#include "common.h"
#include <sandstorm/package.capnp.h>

namespace blackrock {

void createSandstormDirectories();
// Call before enterSandstormBundle() (before forking) to ensure directory tree is initialized.

void enterSandstormBundle();
// Call to cause the current process (typically newly-forked) to enter the Sandstorm bundle.
// Its directory tree will then appear to be Sandstorm's.

kj::Maybe<kj::String> checkPgpSignatureInBundle(
    kj::StringPtr appIdString, sandstorm::spk::Metadata::Reader metadata);
// Runs sandstorm::ctheckPgpSignature() inside the Sandstorm bundle, since it invokes gpg.

} // namespace blackrock

#endif // BLACKROCK_BUNDLE_H_
