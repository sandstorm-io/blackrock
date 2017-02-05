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

# Blackrock's Makefile augments Sandstorm's.

# You may override the following vars on the command line to suit
# your config.
CC=clang
CXX=clang++
CFLAGS=-O2 -g -Wall
CXXFLAGS=$(CFLAGS)
BUILD=0
PARALLEL=$(shell nproc)

.PHONY: all fast clean continuous deps update-deps

define color
  printf '\033[0;34m==== $1 ====\033[0m\n'
endef

all: blackrock.tar.xz

fast: blackrock-fast.tar.xz

clean:
	rm -rf blackrock*.tar.xz shell/.meteor local-config
	make -f deps/sandstorm/Makefile clean
	find shell/packages -type l | xargs -r rm

continuous: tmp/.deps
	make -f deps/sandstorm/Makefile continuous

# These rules generate shell/.meteor by copying over files from Sandstorm and
# then adding all packages named "blackrock-*" found in shell/packages to the
# dependency list. You should never ever edit things in Blackrock's
# shell/.meteor directly; edit Sandstorm's version instead, and then re-run
# make.
shell/.meteor:
	@mkdir shell/.meteor
	@touch shell/.meteor/DO-NOT-EDIT-THESE
shell/.meteor/%: deps/sandstorm/shell/.meteor/% tmp/.deps shell/.meteor
	@cp $< $@
shell/.meteor/packages: deps/sandstorm/shell/.meteor/packages tmp/.deps shell/.meteor shell/packages/blackrock-*
	@$(call color,generating meteor package list)
	@cp $< $@
	@(echo && cd shell/packages && ls -d blackrock-*) >> $@
	@find shell/packages -type l | xargs -r rm
	@(cd shell/packages && ln -s ../../deps/sandstorm/shell/packages/* .)
meteor-env: shell/.meteor/cordova-plugins shell/.meteor/platforms shell/.meteor/release shell/.meteor/versions shell/.meteor/packages
	@ # Hack: If we don't "npm install" inside Sandstorm itself, we may have broken symlinks, e.g. introjs.css.
	@cd deps/sandstorm/shell && meteor npm install

bundle: meteor-env tmp/.deps
	make -f deps/sandstorm/Makefile bundle

bin/e2fsck: tmp/e2fsprogs/e2fsck/e2fsck check-e2fsprogs.sh
	./check-e2fsprogs.sh

shell-env: meteor-env tmp/.deps
	make -f deps/sandstorm/Makefile shell-env

deps: tmp/.deps

tmp/.deps: deps/sandstorm
	cd deps/sandstorm && make deps
	@mkdir -p tmp
	@touch tmp/.deps

deps/sandstorm:
	@$(call color,downloading sandstorm)
	@mkdir -p deps
	git clone https://github.com/sandstorm-io/sandstorm.git deps/sandstorm

deps/e2fsprogs:
	@$(call color,downloading e2fsprogs)
	@mkdir -p deps
	git clone https://github.com/tytso/e2fsprogs.git deps/e2fsprogs

tmp/e2fsprogs/e2fsck/e2fsck: deps/e2fsprogs
	@$(call color,build e2fsprogs)
	@mkdir -p tmp/e2fsprogs
	cd tmp/e2fsprogs && ../../deps/e2fsprogs/configure CFLAGS='-Os -DEXT2_SKIP_UUID' LDFLAGS='-static' && make -j$(PARALLEL)

update-deps:
	@$(call color,updating sandstorm)
	@cd deps/sandstorm && echo "pulling sandstorm..." && git pull && make update-deps

bin/blackrock.unstripped: bundle
	@ # TODO(cleanup): This is ugly.
	@$(call color,strip binaries)
	@cp bin/blackrock bin/blackrock.unstripped
	@strip bin/blackrock

blackrock.tar.xz: bundle bin/e2fsck bin/blackrock.unstripped
	@$(call color,compress release bundle)
	@tar c --transform="s,^,blackrock/,S" bin/blackrock bin/e2fsck bin/tune2fs bin/resize2fs bundle | xz -c -9e > blackrock.tar.xz

blackrock-fast.tar.xz: bundle bin/e2fsck bin/blackrock.unstripped
	@$(call color,compress fast bundle)
	@tar c --transform="s,^,blackrock/,S" bin/blackrock bin/e2fsck bin/tune2fs bin/resize2fs bundle | xz -c -0 > blackrock-fast.tar.xz

# ========================================================================================
# Local testing

.local/mongo:
	@mkdir -p .local
	truncate -s 10737418240 .local/mongo
	/sbin/mkfs.ext4 .local/mongo

.local/storage:
	@mkdir -p .local
	truncate -s 10737418240 .local/storage
	/sbin/mkfs.ext4 .local/storage

local-config: test-config.capnp
	capnp eval --binary -Isrc test-config.capnp vagrant > local-config

run-local: bundle bin/e2fsck local-config .local/mongo .local/storage
	# We need to bring up one VM in advance to make the vboxnet0 network interface appear.
	(vagrant status --machine-readable | grep -q 'storage0,state,running') || vagrant up storage0
	bin/blackrock master local-config -r

kill-local:
	vagrant destroy -f

local-mongo:
	mongo -u sandstorm -p "$$(vagrant ssh mongo0 -c 'cat /var/blackrock/bundle/mongo/passwd')" --authenticationDatabase admin 172.28.128.50/meteor

local-admintoken:
	vagrant ssh frontend0 -c 'echo -n testtoken > /var/blackrock/bundle/sandstorm/adminToken'
	@echo "Now go to: http://localrock.sandstorm.io:6080/setup/token/testtoken"
