#! /bin/bash

# Startup script to be run on every boot on all slave machines on GCE.

set -euo pipefail

if [ -e /dev/disk/by-id/google-blackrock-storage ]; then
  mkdir -p /var/blackrock/bundle/storage
  mount /dev/disk/by-id/google-blackrock-storage /var/blackrock/storage
fi

if [ -e /dev/disk/by-id/google-blackrock-mongo ]; then
  mkdir -p /var/blackrock/bundle/mongo
  mount /dev/disk/by-id/google-blackrock-mongo /var/blackrock/bundle/mongo
fi

