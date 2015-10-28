// Sandstorm Blackrock
// Copyright (c) 2015 Sandstorm Development Group, Inc.
// All Rights Reserved

#include "basics.h"
#include <kj/debug.h>
#include <sodium/randombytes.h>
#include <sodium/crypto_generichash_blake2b.h>

namespace blackrock {
namespace storage {

ObjectKey ObjectKey::generate() {
  ObjectKey result;
  randombytes_buf(result.key, sizeof(result.key));

  return result;
}

ObjectId::ObjectId(const ObjectKey &key) {
  KJ_ASSERT(crypto_generichash_blake2b(
      reinterpret_cast<byte*>(id), sizeof(id),
      reinterpret_cast<const byte*>(key.key), sizeof(key.key),
      nullptr, 0) == 0);
}

kj::FixedArray<char, 24> ObjectId::filename(char prefix) const {
  // base64 the ID to create a filename.

  static const char DIGITS[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  kj::FixedArray<char, 24> result;

  const byte* __restrict__ input = reinterpret_cast<const byte*>(id);
  const byte* end = input + sizeof(id);
  char* __restrict__ output = result.begin();

  *output++ = prefix;

  uint window = 0;
  uint windowBits = 0;
  while (input < end) {
    window <<= 8;
    window |= *input++;
    windowBits += 8;
    while (windowBits >= 6) {
      windowBits -= 6;
      *output++ = DIGITS[(window >> windowBits) & 0x37];
    }
  }

  if (windowBits > 0) {
    window <<= 6 - windowBits;
    *output++ = DIGITS[window & 0x37];
  }

  *output++ = '\0';

  KJ_ASSERT(output == result.end());
  return result;
}

kj::String KJ_STRINGIFY(ObjectId id) {
  return kj::heapString(id.filename('o').begin());
}

kj::StringPtr KJ_STRINGIFY(RecoveryType type) {
  switch (type) {
    case RecoveryType::STAGING:    return "staging";
    case RecoveryType::JOURNAL:    return "journal";
    case RecoveryType::BACKBURNER: return "backburner";
    case RecoveryType::SWITCH:     return "switch";
    case RecoveryType::SYNC_STATE: return "sync-state";
  }
}

}  // namespace storage
}  // namespace blackrock
