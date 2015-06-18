#!/bin/bash
#
# Sandstorm - Personal Cloud Sandbox
# Copyright (c) 2014 Sandstorm Development Group, Inc. and contributors
# All rights reserved.
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

set -euo pipefail

XVFB_PID=""
RUN_SELENIUM="true"

cleanExit () {
  rc=$1

  if [ -n "$XVFB_PID" ] ; then
    # Send SIGINT to the selenium-server child of the backgrounded xvfb-run, so
    # it will exit cleanly and the Xvfb process will also be cleaned up.
    # We don't actually know that PID, so we find it with pgrep.
    kill -SIGINT $(pgrep --parent $XVFB_PID node)
    wait $XVFB_PID
  fi
  exit $rc
}

checkInstalled() {
  if ! $(which $1 >/dev/null 2>/dev/null) ; then
    echo "Couldn't find executable '$1' - try installing the $2 package?"
    exit 1
  fi
}


THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

# Parse arguments.
while [ $# -gt 0 ] ; do
  case $1 in
    --no-selenium)
      RUN_SELENIUM="false"
      ;;
    *)
      ;;
  esac
  shift
done

cd "$THIS_DIR"/../deps/sandstorm/tests

checkInstalled npm npm
checkInstalled firefox firefox

npm install

if [ "$RUN_SELENIUM" != "false" ] ; then
  checkInstalled java default-jre-headless
  checkInstalled xvfb-run Xvfb
  checkInstalled pgrep procps
  xvfb-run ./node_modules/selenium-standalone/bin/selenium-standalone start &
  XVFB_PID=$!
fi

export LAUNCH_URL="https://testrock.sandstorm.io"
export DISABLE_DEMO=true
export SKIP_UNITTESTS=true
set +e

npm test

cleanExit $?
