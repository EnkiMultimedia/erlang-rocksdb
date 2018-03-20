#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

if [ `uname -s` = "FreeBSD" ]; then
    export CC="clang"
    export CXX="clang++"
    export CFLAGS="$CFLAGS -D_GLIBCXX_USE_C99"
    export CXXFLAGS="-std=c++11 -stdlib=libc++ -D_GLIBCXX_USE_C99"

fi

if [ `uname -s` = "OpenBSD" ]; then
    export CC="egcc"
    export CXX="eg++"
    export CFLAGS="$CFLAGS -D_GLIBCXX_USE_C99"
    export CXXFLAGS="-std=c++11 -pthread -D_GLIBCXX_USE_C99"
fi

SCRIPTPATH=$( cd $(dirname $0) ; pwd -P )
SCRIPT=$SCRIPTPATH/${0##*/}
BASEDIR=$SCRIPTPATH
BUILD_CONFIG=$BASEDIR/rocksdb/make_config.mk

ROCKSDB_VSN="5.10.4"
SNAPPY_VSN="1.1.4"
LZ4_VSN="1.8.1.2"
ROCKSDB_PREFIX="v5.10.4"


set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

MAKEFLAGS=
BUILD_JOBS=$( erl -noshell -s init stop -eval "io:format(\"~p\", [erlang:max(3, erlang:system_info(schedulers))])." )


case "$1" in
    rm-deps)
        rm -rf rocksdb system snappy-$SNAPPY_VSN lz4-$LZ4_VSN
        ;;

    clean)
        rm -rf system snappy-$SNAPPY_VSN lz4-$LZ4_VSN
        if [ -d rocksdb ]; then
            (cd rocksdb && $MAKE clean)
        fi
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/rocksdb:$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        (cd rocksdb && $MAKE ldb_tests)

        ;;

    get-deps)
        if [ ! -d rocksdb ]; then
            #ROCKSDBURL="https://github.com/facebook/rocksdb/archive/rocksdb-$ROCKSDB_VSN.tar.gz"
            ROCKSDBTARGZ="rocksdb-$ROCKSDB_VSN.tar.gz"
            #echo Downloading $ROCKSDBURL...
            #curl -L -o $ROCKSDBTARGZ $ROCKSDBURL
            echo "==> Building rocksdb"
            tar -xzf $ROCKSDBTARGZ
            mv rocksdb-$ROCKSDB_VSN rocksdb
            #patch -p0 < rocksdb-env-env_posix.cc.patch
            patch -p0 < rocksdb-port-stack_trace.cc.patch

        fi
        ;;

    *)
        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --prefix=$BASEDIR/system --libdir=$BASEDIR/system/lib --with-pic --disable-gtest)
        fi

        if [ ! -f system/lib/libsnappy.a ]; then
            (cd snappy-$SNAPPY_VSN && $MAKE  -j${BUILD_JOBS} && $MAKE install)
        fi

        if [ ! -d lz4-$LZ4_VSN ]; then
            tar -xzf lz4-$LZ4_VSN.tar.gz
        fi

        if [ ! -f system/lib/liblz4.a ]; then
            (cd lz4-$LZ4_VSN/lib && $MAKE  -j${BUILD_JOBS} CFLAGS="-O3 -fPIC" && $MAKE install PREFIX=$BASEDIR/system)
        fi

        export CXXFLAGS="-std=c++11 -pthread -D_GLIBCXX_USE_C99 -DNDEBUG"
        export CFLAGS="$CFLAGS -DNDEBUG -I$BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        if [ `uname -s` = "OpenBSD" ]; then
            export CXXFLAGS="$CXXFLAGS -DOS_OPENBSD"
            export CFLAGS="$CFLAGS -DOS_OPENBSD"
            export LDFLAGS="$LDFLAGS -lstdc++"
            export CPPFLAGS="-DOS_OPENBSD"
        fi
	    export CXXFLAGS="$CXXFLAGS -fPIC -I$BASEDIR/system/include"

        sh $SCRIPT get-deps
        if [ ! -f rocksdb/librocksdb.a ]; then
            (cd rocksdb && MAKEFLAGS= USE_RTTI=1 CXXFLAGS="$CXXFLAGS" PORTABLE=1 $MAKE -j$BUILD_JOBS static_lib)
        fi
        ;;
esac
