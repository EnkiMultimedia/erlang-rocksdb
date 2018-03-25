# Based on c_src.mk from erlang.mk by Loic Hoguin <essen@ninenines.eu>

.PHONY: clean deps help

CURDIR := $(shell pwd)
BASEDIR := $(abspath $(CURDIR)/..)

PROJECT := rocksdb

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/include/\", [code:root_dir(), erlang:system_info(version)]).")
ERL_INTERFACE_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s\", [code:lib_dir(erl_interface, include)]).")
ERL_INTERFACE_LIB_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s\", [code:lib_dir(erl_interface, lib)]).")
ERL_SCHEDULERS ?= $(shell erl -noshell -s init stop -eval "io:format(\"~p\", [erlang:max(3, erlang:system_info(schedulers))]).")

C_SRC_DIR = $(CURDIR)
C_SRC_OUTPUT ?= $(CURDIR)/../priv/$(PROJECT).so


# System type and C compiler/flags.

UNAME_SYS := $(shell uname -s)
ifeq ($(UNAME_SYS), Darwin)
	CC ?= cc
	CFLAGS ?= -O2 -std=c99 -arch x86_64 -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O2 -arch x86_64 -Wall
	LDFLAGS ?= -arch x86_64 -flat_namespace -undefined suppress
else ifeq ($(UNAME_SYS), FreeBSD)
	CC = clang
	CXX = clang++
	CFLAGS ?= -O2 -std=c99 -Wall -Wmissing-prototypes
	CFLAGS += -D_GCLIBCXX_USE_C99
	CXXFLAGS ?= -O2 -Wall
	CXXFLAGS += -std=c++11 -stdlib=libc++ -D_GCLIBCXX_USE_C99
else ifeq ($(UNAME_SYS), OpenBSD)
	CC = egcc
	CXX = eg++
	CFLAGS ?= -O2 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CFLAGS += -D_GCLIBCXX_USE_C99
	CXXFLAGS ?= -O2 -Wall
	CXXFLAGS += -std=c++11 -pthread -finline-functions -D_GCLIBCXX_USE_C99
else ifeq ($(UNAME_SYS), DragonFly)
	CC = c++48
	CXX = c++48
	CFLAGS ?= -O2 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CFLAGS += -D_GCLIBCXX_USE_C99
	CXXFLAGS ?= -O2 -finline-functions -Wall
	CXXFLAGS += -D_GCLIBCXX_USE_C99
else ifeq ($(UNAME_SYS), NetBSD)
	CC = c++48
	CXX = c++48
	CFLAGS ?= -O2 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CFLAGS += -D_GCLIBCXX_USE_C99
	CXXFLAGS ?= -O2 -finline-functions -Wall
	CXXFLAGS += -D_GCLIBCXX_USE_C99
else ifeq ($(UNAME_SYS), SunOS)
	ifeq ($(shell uname -v | sed 's/_.*$$//'), joyent)
		CC = gcc
		CXX = g++
	else
		CC = c++48
		CXX = c++48
	endif
	CFLAGS ?= -O2 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CFLAGS += -D_GCLIBCXX_USE_C99
	CXXFLAGS ?= -O2 -finline-functions -Wall
	CXXFLAGS += -D_GCLIBCXX_USE_C99
else ifeq ($(UNAME_SYS), Linux)
	CC ?= gcc
	CFLAGS ?= -O2 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O2 -finline-functions -Wall
endif

ROCKSDBLIBS = $(BASEDIR)/deps/rocksdb/librocksdb.a $(BASEDIR)/deps/snappy/.libs/libsnappy.a $(BASEDIR)/deps/lz4/lib/liblz4.a

#ROCKSDB_PLATFORM_LDFLAGS=$(shell cat ../deps/rocksdb/make_config.mk | grep PLATFORM_LDFLAGS | cut -d' ' -f2- | sed -e 's/-lsnappy//' | sed -e 's/-llz4//')

CFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR) -I $(BASEDIR)/deps/rocksdb/include
CXXFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR) -I $(BASEDIR)/deps/rocksdb/include -std=c++11

LDLIBS += -L $(ERL_INTERFACE_LIB_DIR) -lerl_interface -lei
ifeq ($(UNAME_SYS), OpenBSD)
	LDLIBS += -lestdc++
else
	LDLIBS += -lstdc++
endif
LDLIBS += $(ROCKSDBLIBS)

LDFLAGS += -shared

# Verbosity.

c_verbose_0 = @echo " C     " $(?F);
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(?F);
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

SOURCES := $(shell find $(C_SRC_DIR) -maxdepth 1 -type f \( -name "*.c" -o -name "*.C" -o -name "*.cc" -o -name "*.cpp" \))
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

ROCKSDBLIB = $(BASEDIR)/deps/rocksdb/librocksdb.a

help: ## This documentation
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

all: $(C_SRC_OUTPUT) ## Build all

$(C_SRC_OUTPUT): ROCKSDB_PLATFORM_LDFLAGS=$(shell cat ../deps/rocksdb/make_config.mk | grep PLATFORM_LDFLAGS | cut -d' ' -f2- | sed -e 's/-lsnappy//' | sed -e 's/-llz4//')

$(C_SRC_OUTPUT): $(ROCKSDBLIBS) $(OBJECTS)
	@mkdir -p $(BASEDIR)/priv/
	$(link_verbose) $(CC) $(OBJECTS) $(LDLIBS) $(LDFLAGS) $(ROCKSDB_PLATFORM_LDFLAGS) -o $(C_SRC_OUTPUT)

%.o: %.c rocksdb
	$(COMPILE_C) $(OUTPUT_OPTION) $<

%.o: %.cc
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.C
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.cpp
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<


$(ROCKSDBLIBS): deps 

deps:
	@-(cd ../deps && $(MAKE) rocksdb)

clean: distclean 
	@rm -f $(C_SRC_OUTPUT) $(OBJECTS)

distclean:
	@-(cd ../deps && $(MAKE) distclean)
