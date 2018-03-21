# Barrel dependecy Makefile

UNAME_SYS := $(shell sh -c 'uname -s 2>/dev/null || echo not')
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


RDB_CXXFLAGS="$(CXXFLAGS) -DNDEBUG -Isnappy/ -Ilz4/lib" 
RDB_CFLAGS="$(CFLAGS) -DNDEBUG -Isnappy/ -Ilz4/lib"
RDB_LDFLAGS="$(LDFLAGS) -Lsnappy/.libs -Llz4/lib"

ifeq ($(UNAME_SYS),OpenBSD)
	RDB_CXXFLAGS="$(RDB_CXXFLAGS) -DOS_OPENBSD"
	RDB_CFLAGS="$(RDB_CFLAGS) -DOS_OPENBSD"
	RDB_LDFLAGS="$(LDFLAGS) -lstdc++"
	RDB_CPPFLAGS="-DOS_OPENBSD"
endif

SNAPPYLIB=snappy/.libs/libsnappy.a
LZ4LIB=lz4/lib/liblz4.a

default:
	@echo "Explicit target required"

.PHONY: default

distclean:
	-(cd rocksdb && $(MAKE) clean) > /dev/null || true
	-(cd snappy && $(MAKE) clean) > /dev/null || true
	-(cd lz4 && $(MAKE) clean) > /dev/null || true

.PHONT: distclean

$(SNAPPYLIB):
	cd snappy &&  ./configure --with-pic --disable-gtest
	cd snappy && $(MAKE)

.PHONY: snappy

$(LZ4LIB):
	cd lz4 && $(MAKE) CFLAGS="-O3 -fPIC"

.PHONY: lz4

rocksdb: $(SNAPPYLIB) $(LZ4LIB)
	cd rocksdb && \
		USE_RTTI=1 CXXFLAGS=$(RDB_CXXFLAGS) LDFLAGS=$(RDB_LDFLAGS) PORTABLE=1 $(MAKE) static_lib

.PHONY: rocksdb

