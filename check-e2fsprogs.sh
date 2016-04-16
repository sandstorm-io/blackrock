#! /bin/bash

# Verify that we've correctly built e2fsprogs and then copy them into the bundle.

set -euo pipefail

fail() {
  echo "check-e2fsprogs: $@" >&2
  exit 1
}

PROGS="tmp/e2fsprogs/e2fsck/e2fsck tmp/e2fsprogs/misc/tune2fs tmp/e2fsprogs/resize/resize2fs"

for PROG in $PROGS; do
  if [ ! -e $PROG ]; then
    fail "$PROG does not exist"
  elif [ "$(ldd $PROG 2>&1 | tr -d '\t')" != "not a dynamic executable" ]; then
    fail "$PROG is not statically-linked"
  fi
done

rm -f /var/tmp/test-ext4fs-uuid-bug
truncate -s 8G /var/tmp/test-ext4fs-uuid-bug >/dev/null 2>&1
tmp/e2fsprogs/misc/mke2fs -t ext4 -U 00000000-0000-0000-0000-000000000000 /var/tmp/test-ext4fs-uuid-bug >/dev/null 2>&1 || fail "mke2fs failed"
tmp/e2fsprogs/e2fsck/e2fsck -p /var/tmp/test-ext4fs-uuid-bug >tmp/check-e2fsck.out 2>&1 || fail "e2fsck failed"
if grep -q UUID tmp/check-e2fsck.out; then
  fail "e2fsck not compiled to ignore null UUID"
fi
tmp/e2fsprogs/e2fsck/e2fsck -p /var/tmp/test-ext4fs-uuid-bug >/dev/null 2>&1 || fail "e2fsck repeat failed"
rm /var/tmp/test-ext4fs-uuid-bug

cp $PROGS bin
