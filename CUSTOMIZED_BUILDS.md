# Customized builds #

Since version 0.26.0 Erlang wrapper for RocksDB support customized build.

There are manyoptions added to customize build (all are `OFF` by default):

* `WITH_SYSTEM_ROCKSDB=ON` the driver will link with RocksDB shared library provided by host system

* `WITH_SNAPPY=ON` the driver will link with snappy shared library provided by host system

* `WITH_LZ4=ON` the driver will link with lz4 shared library provided by host system

* `WITH_BZ2=ON` the driver will link with bzip2 shared library provided by host system

* `WITH_ZLIB=ON` the driver will link with zlib shared library provided by host system

* `WITH_ZSTD=ON` (default) the driver will link with zstd. If system library is not found, bundled zstd is used automatically.

* `WITH_BUNDLE_ZSTD=OFF` disable bundled zstd fallback. If system zstd is not found, zstd support is disabled.

* `WITH_CCACHE=ON` the driver will use ccache to cache part of the build. you can also set the `CCACHE_DIR` 

* `WITH_TBB=ON` to build with [Threading Building Blocks (TBB)](https://software.intel.com/en-us/intel-tbb)


> ⚠️ with the 1.0.0 release `erlang-rocksdb` doesn't build statically with `LZ4` and s`SNAPPY`. To reintroduce this behaviour
> you can build it with the options `WITH_BUNDLE_SNAPPY` and `WITH_BUNDLE_LZ4`.


All options passes via `ERLANG_ROCKSDB_OPTS` environment variable or via `rebar.config.script`. For example, command

```
 $ export ERLANG_ROCKSDB_OPTS="-DERLANG_ROCKSDB_USE_SYSTEM_ROCKSDB=ON -DERLANG_ROCKSDB_USE_SYSTEM_SNAPPY=ON -DERLANG_ROCKSDB_USE_SYSTEM_LZ4=ON"
 $ make
```

gives following output on macOS

```
===> Verifying dependencies...
===> Compiling rocksdb
-- Using OTP lib: /usr/local/Cellar/erlang/21.1.4/lib/erlang/lib - found
-- Using erl_interface version: erl_interface-3.10.4
-- Using erts version: erts-10.1.3
-- Found LZ4: /usr/local/lib/liblz4.dylib
-- Found SNAPPY: /usr/local/lib/libsnappy.dylib
-- Found RocksDB: /usr/local/lib/librocksdb.dylib (found suitable version "5.17.2", minimum required is "5.17.2")
-- Configuring done
-- Generating done

...
[SKIP BUILD OUTPUT]
...

```

### Build in parallel

You can  build `erlang-rocksdb` in parallel by passing the following option `ERLANG_ROCKSDB_BUILDOPTS=-j` 
or  `ERLANG_ROCKSDB_BUILDOPTS=-j N` where N is the number of threads you want to use.

