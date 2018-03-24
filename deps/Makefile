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

OUTDIR=$(CURDIR)/out

RDB_CXXFLAGS="$(CXXFLAGS) -DNDEBUG -I$(OUTDIR) -fPIC"
RDB_CFLAGS="$(CFLAGS) -DNDEBUG -I$(OUTDIR)"
RDB_LDFLAGS="$(LDFLAGS) -L$(OUTDIR)/lib"

ifeq ($(UNAME_SYS),OpenBSD)
	RDB_CXXFLAGS="$(RDB_CXXFLAGS) -DOS_OPENBSD"
	RDB_CFLAGS="$(RDB_CFLAGS) -DOS_OPENBSD"
	RDB_LDFLAGS="$(LDFLAGS) -lstdc++"
	RDB_CPPFLAGS="-DOS_OPENBSD"
endif

SNAPPYLIB=out/lib/libsnappy.a
LZ4LIB=out/lib/liblz4.a
RDBLIB=rocksdb/librocksdb.a

default:
	@echo "Explicit target required"

.PHONY: default

distclean:
	@-(cd rocksdb && $(MAKE) clean) > /dev/null || true
	@-(cd snappy && $(MAKE) clean) > /dev/null || true
	@-(cd lz4 && $(MAKE) clean) > /dev/null || true
	@-(rm -rf $(OUTDIR)) > /dev/null || true

.PHONT: distclean

$(SNAPPYLIB):
	@cd snappy && touch configure.ac aclocal.m4 configure Makefile.am Makefile.in
	@cd snappy &&  ./configure --prefix=$(OUTDIR) --libdir=$(OUTDIR)/lib --with-pic --disable-gtest
	@cd snappy && $(MAKE) && $(MAKE) install

.PHONY: snappy

$(LZ4LIB):
	@cd lz4 && $(MAKE) CFLAGS="-O3 -fPIC" && $(MAKE) install PREFIX=$(OUTDIR)

.PHONY: lz4

rocksdb: $(RDBLIB)

$(RDBLIB): $(SNAPPYLIB) $(LZ4LIB)
	@cd rocksdb && \
		USE_RTTI=1 CXXFLAGS=$(RDB_CXXFLAGS) LDFLAGS=$(RDB_LDFLAGS) DISABLE_JEMALLOC=1 DEBUG_LEVEL=0 PORTABLE=1 $(MAKE) static_lib

.PHONY: rocksdb

