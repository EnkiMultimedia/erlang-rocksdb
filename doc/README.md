

# erlang-rocksdb - Erlang wrapper for RocksDB. #

Copyright (c) 2016-2019 Benoît Chesneau.

__Version:__ 1.0.1 Erlang wrapper for RocksDB-Cloud.

This binding wrap [rocksdb-cloud](https://github.com/rockset/rocksdb-cloud/) . It's based on
the [Erlang rocksdb binding](https://gitlab.com/barrel-db/erlang-rocksdb).

Feedback and pull requests welcome! If a particular feature of RocksDB is important to you, please let me know by opening an issue, and I'll prioritize it.

## Features

- rocksdb-cloud 5.18.3 with snappy 1.1.7, lz4 1.8.3
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

See the [Gitlab Wiki](https://gitlab.com/barrel-db/erlang-rocksdb/wikis/home) for more explanation and specifically
the [RocksDB Cloud usage](https://gitlab.com/barrel-db/erlang-rocksdb/wikis/Getting-Started-with-Rocksdb-Cloud) page.

> Note: `cmake>=3.4` is required to install `erlang-rocksdb`.

## Customized build ##

See the [Customized builds](customize_rocksdb_build.md) for more information.

## Support

Support, Design and discussions are done via the [Gitlab Tracker](https://gitlab.com/barrel-db/erlang-rocksdb/issues) or [email](mailto:incoming+barrel-db/erlang-rocksdb@gitlab.com).

## License

Erlang RocksDB is licensed under the Apache License 2.


## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="rocksdb.md" class="module">rocksdb</a></td></tr></table>

