

# erlang-rocksdb - Erlang wrapper for RocksDB. #

Copyright (c) 2016-2018 Benoît Chesneau.

__Version:__ 0.16.0 Erlang wrapper for RocksDB.

Feedback and pull requests welcome! If a particular feature of RocksDB is important to you, please let me know by opening an issue, and I'll prioritize it.

## Features

- rocksdb 5.10.4 with snappy 1.4.4, lz4 1.8.1.2
- Erlang 19.3 and sup with dirty-nifs enabled
- all basics db operations
- batchs support
- snapshots support
- checkpoint support
- column families support
- transaction logs
- backup support
- Tested on macosx, freebsd, solaris and linux

## Usage

This binding is statically linked with a specific version of RocksDB. If you want to build it yourself, make sure you've also cloned the RocksDB, LZ4 and Snappy submodules:

```
git submodule update --init --recursive
```

See the [Gitlab Wiki](https://gitlab.com/barrel-db/erlang-rocksdb/wikis/home) for more explanation.

## Support

Support, Design and discussions are done via the [Gitlab Tracker](https://gitlab.com/barrel-db/erlang-rocksdb/issues) or [email](mailto:incoming+barrel-db/erlang-rocksdb@gitlab.com).

## License

Erlang RocksDB is licensed under the Apache License 2.


## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="http://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/rocksdb.md" class="module">rocksdb</a></td></tr>
<tr><td><a href="http://gitlab.com/barrel-db/erlang-rocksdb/blob/master/doc/rocksdb_bump.md" class="module">rocksdb_bump</a></td></tr></table>

