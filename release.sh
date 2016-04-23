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
    GCE_PROJECT=sandstorm-oasis
    export CLOUDSDK_COMPUTE_ZONE=us-central1-f

    # We always do a Blackrock prod release shortly after a Sandstorm release.
    BUILD=$(curl -s https://install.sandstorm.io/dev)
    BUILDSTAMP=$BUILD-$(date -u +%Y%m%d-%H%M%S)

    if (grep -r KJ_DBG src/* | egrep -v '/(debug(-test)?|exception)[.]'); then
      echo '*** Error:  There are instances of KJ_DBG in the code.' >&2
      exit 1
    fi

    if egrep -r 'TODO\(now\)' src/*; then
      echo '*** Error:  There are release-blocking TODOs in the code.' >&2
      exit 1
    fi

    if [ "x$(git status --porcelain)" != "x" ]; then
      echo "Please commit changes to git before releasing." >&2
      exit 1
    fi
    ;;
  * )
    echo "no such target: $1" >&2
    exit 1
    ;;
esac

shift

DRY_RUN=no
CONFIRM_EACH=no
HOTFIX=no

while [ $# -gt 0 ]; do
  case $1 in
    -n )
      DRY_RUN=yes
      ;;
    -m )
      CONFIRM_EACH=yes
      ;;
    -h )
      HOTFIX=yes
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
    printf "\033[0;33m=== RUN? %s ===\033[0m" "$*"
    read -sn 1 ANSWER
    if [ -z "$ANSWER" ]; then
      printf "\r\033[K"
    else
      printf "\033[0;31m\r=== SKIPPED: %s ===\033[0m\n" "$*"
      return
    fi
  fi

  printf "\033[0;35m=== %s ===\033[0m\n" "$*"

  if [ "$DRY_RUN" = "no" ]; then
    "$@"
  fi
}

doit make clean BUILD=$BUILD
doit make BUILD=$BUILD

if [ "$HOTFIX" = "yes" ]; then
  FRONTENDS=$(gce instances list --format=text | grep '^name:' | sed -e 's/^name: *//g' | grep '^frontend' | grep -v 'frontend0$')
  FRONTENDS="$FRONTENDS frontend0"

  for FRONTEND in $FRONTENDS; do
    doit gce copy-files blackrock.tar.xz "root@$FRONTEND:/root"
  done

  for FRONTEND in $FRONTENDS; do
    doit gce ssh "root@$FRONTEND" --command 'cd /root && rm -rf blackrock /blackrock/bundle.new && tar Jxof blackrock.tar.xz && mv blackrock/bundle /blackrock/bundle.new && cd /blackrock && mv bundle bundle.$(date -u +%Y%m%d-%H%M%S) && mv bundle.new bundle'
  done

  for FRONTEND in $FRONTENDS; do
    doit gce ssh "root@$FRONTEND" --command 'kill $(pidof node)'
  done

  exit 0
fi

# Keep unstripped binary for debugging.
mkdir -p dbg
cp bin/blackrock.unstripped dbg/blackrock-$BUILDSTAMP

# Create a new image.
doit gce instances create build --image debian-7-backports
doit sleep 10 # make sure instance is up
doit gce ssh build 'sudo sed -i -e "s/PermitRootLogin no/PermitRootLogin without-password/g" /etc/ssh/sshd_config; sudo service ssh restart'
doit gce copy-files blackrock.tar.xz root@build:/
doit gce ssh root@build --command "cd / && tar Jxof blackrock.tar.xz && rm /blackrock.tar.xz"
doit gce instances delete build -q --keep-disks boot
doit gce images create blackrock-$BUILDSTAMP --source-disk build
doit gce disks delete -q build

# Also upload to master.
doit gce copy-files bin/blackrock root@master:/blackrock/bin/blackrock-$BUILDSTAMP
