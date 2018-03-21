# Barrel dependecy Makefile

uname_S:= $(shell sh -c 'uname -s 2>/dev/null || echo not')


ifeq ($(uname_S),FreeBSD)
	CC="clang"
    CXX="clang++"
    CFLAGS="$CFLAGS -D_GLIBCXX_USE_C99"
    CXXFLAGS="-std=c++11 -stdlib=libc++ -D_GLIBCXX_USE_C99"
endif

ifeq ($(uname_S),OpenBSD)
	CC="egcc"
    CXX="eg++"
    CFLAGS="$CFLAGS -D_GLIBCXX_USE_C99"
    CXXFLAGS="-std=c++11 -pthread -D_GLIBCXX_USE_C99"
endif

RDB_CXXFLAGS="-std=c++11 -pthread -D_GLIBCXX_USE_C99 -DNDEBUG"
RDB_CFLAGS="$(CFLAGS) -DNDEBUG -I./snappy/ -I./lz4/"
RDB_LDFLAGS="$(LDFLAGS) -Lsnappy/ -Llz4/"

ifeq ($(uname_S),OpenBSD)
	RDB_CXXFLAGS="$(RDB_CXXFLAGS) -DOS_OPENBSD"
	RDB_CFLAGS="$(RDB_CFLAGS) -DOS_OPENBSD"
	RDB_LDFLAGS="$(LDFLAGS) -lstdc++"
	RDB_CPPFLAGS="-DOS_OPENBSD"
endif

default:
	@echo "Explicit target required"

distclean:
	-(cd rocksdb && $(MAKE) clean) > /dev/null || true
	-(cd snappy && $(MAKE) clean) > /dev/null || true
	-(cd lz4 && $(MAKE) clean) > /dev/null || true


snappy:
	cd snappy &&  ./configure --prefix=$BASEDIR/system --libdir=$BASEDIR/system/lib --with-pic --disable-gtest
	cd snappy && $(MAKE)

lz4:
	cd lz4 && $(MAKE) CFLAGS="-O3 -fPIC"

rocksdb: snappy lz4
	cd rocksdb && \
		USE_RTTI=1 CXXFLAGS="$(RDB_CXXFLAGS)" LDFLAGS=$(RDB_LDFLAGS) CFLAGS=$(RDB_CFLAGS)PORTABLE=1 $(MAKE) static_lib

