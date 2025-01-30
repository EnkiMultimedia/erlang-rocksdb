

# erlang-rocksdb - Erlang wrapper for RocksDB. #

Copyright (c) 2016-2022 Benoît Chesneau.

__Version:__ 1.8.0 Erlang wrapper for RocksDB.

Feedback and pull requests welcome! If a particular feature of RocksDB is important to you, please let me know by opening an issue, and I'll prioritize it.

## Features

- rocksdb 7.7.3 with snappy 1.1.10, lz4 1.8.3
- Erlang 19.3 and sup with dirty-nifs enabled
- all basics db operations
- batchs support
- snapshots support
- checkpoint support
- column families support
- transaction logs
- backup support
- erlang merge operator
- customized build support
- Tested on macosx, freebsd, solaris and linux

## Usage

See the [Gitlab Wiki](https://gitlab.com/barrel-db/erlang-rocksdb/wikis/home) for more explanation.

> Note: since the version **0.26.0**, `cmake>=3.4` is required to install `erlang-rocksdb`.

## Customized build ##

See the [Customized builds](http://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/customize_rocksdb_build.md) for more information.

## Support

Support, Design and discussions are done via the [Gitlab Tracker](https://gitlab.com/barrel-db/erlang-rocksdb/issues) or [email](mailto:incoming+barrel-db/erlang-rocksdb@gitlab.com).

## License

Erlang RocksDB is licensed under the Apache License 2.


## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="http://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/rocksdb.md" class="module">rocksdb</a></td></tr></table>

