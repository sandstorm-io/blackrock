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
	rm -rf blackrock*.tar.xz
	make -f deps/sandstorm/Makefile clean

continuous: tmp/.deps
	make -f deps/sandstorm/Makefile continuous

# These rules generate shell/.meteor by copying over files from Sandstorm and
# then adding all packages named "blackrock-*" found in shell/packages to the
# dependency list. You should never ever edit things in Blackrock's
# shell/.meteor directly; edit Sandstorm's version instead, and then re-run
# make.
shell/.meteor:
	@mkdir shell/.meteor
shell/.meteor/%: deps/sandstorm/shell/.meteor/% tmp/.deps shell/.meteor
	cp $< $@
shell/.meteor/packages: deps/sandstorm/shell/.meteor/packages tmp/.deps shell/.meteor
	cp $< $@
	(echo && cd shell/packages && ls -d blackrock-*) >> $@
meteor-env: shell/.meteor/cordova-plugins shell/.meteor/platforms shell/.meteor/release shell/.meteor/versions shell/.meteor/packages

bundle: meteor-env tmp/.deps
	make -f deps/sandstorm/Makefile bundle

deps: tmp/.deps

tmp/.deps: deps/sandstorm
	cd deps/sandstorm && make deps
	@mkdir -p tmp
	@touch tmp/.deps

deps/sandstorm:
	@$(call color,downloading sandstorm)
	@mkdir -p deps
	git clone https://github.com/sandstorm-io/sandstorm.git deps/sandstorm

update-deps:
	@$(call color,updating sandstorm)
	@cd deps/sandstorm && echo "pulling sandstorm..." && git pull && make update-deps

bin/blackrock.unstripped: bundle
	# TODO(cleanup): This is ugly.
	@$(call color,strip binaries)
	@cp bin/blackrock bin/blackrock.unstripped
	@strip bin/blackrock

blackrock.tar.xz: bundle bin/blackrock.unstripped
	@$(call color,compress release bundle)
	@tar c --transform="s,^,blackrock/,S" bin/blackrock bundle | xz -c -9e > blackrock.tar.xz

blackrock-fast.tar.xz: bundle bin/blackrock.unstripped
	@$(call color,compress fast bundle)
	@tar c --transform="s,^,blackrock/,S" bin/blackrock bundle | xz -c -0 > blackrock-fast.tar.xz

