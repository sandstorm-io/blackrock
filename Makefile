# Blackrock's Makefile augments Sandstorm's.

# You may override the following vars on the command line to suit
# your config.
CC=clang
CXX=clang++
CFLAGS=-O2 -Wall
CXXFLAGS=$(CFLAGS)
BUILD=0
PARALLEL=$(shell nproc)

.PHONY: all fast clean continuous

define color
  printf '\033[0;34m==== $1 ====\033[0m\n'
endef

all: blackrock.tar.xz

fast: blackrock-fast.tar.xz

clean:
	rm -rf blackrock*.tar.xz
	make -f deps/sandstorm/Makefile clean

continuous:
	make -f deps/sandstorm/Makefile continuous

bundle:
	make -f deps/sandstorm/Makefile bundle

blackrock.tar.xz: bundle
	@$(call color,compress release bundle)
	@tar c --transform="s,^,blackrock/," bin/blackrock bundle | xz -c -9e > blackrock.tar.xz

blackrock-fast.tar.xz: bundle
	@$(call color,compress fast bundle)
	@tar c --transform="s,^,blackrock/," bin/blackrock bundle | xz -c -0 > blackrock-fast.tar.xz

