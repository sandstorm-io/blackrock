#! /bin/bash

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 test|prod [-n|-m]" >&2
  exit 1
fi

case $1 in
  test )
    GCE_PROJECT=sandstorm-blackrock-testing
    export CLOUDSDK_COMPUTE_ZONE=us-central1-f
    BUILD=0
    BUILDSTAMP=$(date -u +%Y%m%d-%H%M%S)
    ;;
  prod )
    GCE_PROJECT=sandstorm-blackrock
    export CLOUDSDK_COMPUTE_ZONE=us-central1-f
    
    # We always do a Blackrock prod release shortly after a Sandstorm release.
    BUILD=$(curl -s https://install.sandstorm.io/dev)
    BUILDSTAMP=$BUILD
    ;;
  * )
    echo "no such target: $1" >&2
    exit 1
    ;;
esac

shift

DRY_RUN=no
CONFIRM_EACH=no

while [ $# -gt 0 ]; do
  case $1 in
    -n )
      DRY_RUN=yes
      ;;
    -m )
      CONFIRM_EACH=yes
      ;;
    * )
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
  shift
done

gce() {
  gcloud --project=$GCE_PROJECT compute "$@"
}

doit() {
  local ANSWER
  if [ "$CONFIRM_EACH" != "no" ]; then
    printf "\033[0;33m=== RUN? $* ===\033[0m"
    read -sn 1 ANSWER
    if [ -z "$ANSWER" ]; then
      printf "\r\033[K"
    else
      printf "\033[0;31m\r=== SKIPPED: $* ===\033[0m\n"
      return
    fi
  fi

  printf "\033[0;35m=== $* ===\033[0m\n"

  if [ "$DRY_RUN" = "no" ]; then
    "$@"
  fi
}

doit make clean BUILD=$BUILD
doit make BUILD=$BUILD

# Create a new image.
doit gce instances create build --image debian-7-backports
doit gce copy-files blackrock.tar.xz root@build:/
doit gce ssh root@build --command "cd / && tar Jxof blackrock.tar.xz && rm /blackrock.tar.xz"
doit gce instances delete build -q --keep-disks boot
doit gce images create blackrock-$BUILDSTAMP --source-disk build
doit gce disks delete -q build

# also upload to master
doit gce copy-files bin/blackrock root@master:/blackrock/bin/blackrock-$BUILDSTAMP
doit gce ssh root@master --command "cd /blackrock && ln -sfT bin/blackrock-$BUILDSTAMP latest.next && mv latest.next latest && service blackrock reload"

