# Blackrock's Makefile augments Sandstorm's.

# You may override the following vars on the command line to suit
# your config.
CC=clang
CXX=clang++
CFLAGS=-O2 -Wall
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

bundle: tmp/.deps
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

blackrock.tar.xz: bundle
	@$(call color,compress release bundle)
	@tar c --transform="s,^,blackrock/," bin/blackrock bundle | xz -c -9e > blackrock.tar.xz

blackrock-fast.tar.xz: bundle
	@$(call color,compress fast bundle)
	@tar c --transform="s,^,blackrock/," bin/blackrock bundle | xz -c -0 > blackrock-fast.tar.xz

