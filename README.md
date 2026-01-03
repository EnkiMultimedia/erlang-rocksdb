# erlang-rocksdb - Erlang wrapper for RocksDB

[![Build Status](https://github.com/EnkiMultimedia/erlang-rocksdb/workflows/build/badge.svg)](https://github.com/EnkiMultimedia/erlang-rocksdb/actions?query=workflow%3Abuild)
[![Hex pm](http://img.shields.io/hexpm/v/rocksdb.svg?style=flat)](https://hex.pm/packages/rocksdb)

**Current version: 2.3.0**

Copyright (c) 2016-2026 Benoît Chesneau

Feedback and pull requests welcome! If a particular feature of RocksDB is important to you, please let me know by opening an issue, and I'll prioritize it.

## Features

- RocksDB 10.7.5 with snappy 1.2.1, lz4 1.10.0
- Erlang 22+ with dirty-nifs enabled
- All basic db operations (get, put, delete, merge, multi_get)
- Wide-column entity API (put_entity, get_entity, iterator_columns)
- Extended statistics API (45+ tickers, 13+ histograms)
- BlobDB support with statistics and lazy loading
- Batch operations support
- Snapshots support
- Checkpoint support
- Column families support with coalescing iterator
- Transaction logs
- Pessimistic transactions with row-level locking
- Backup support
- Erlang merge operator
- SST file support (write, ingest, read)
- Customized build support
- Tested on macOS, FreeBSD, Solaris and Linux

## Usage

See the [Doc](https://hexdocs.pm/rocksdb/) for more explanation.

> Note: since the version **0.26.0**, `cmake>=3.4` is required to install `erlang-rocksdb`.

## Customized build ##

See the [Customized builds](https://hexdocs.pm/rocksdb/CUSTOMIZED_BUILDS.html) for more information.

## Support

Support, Design and discussions are done via the [Github Tracker](https://github.com/EnkiMultimedia/erlang-rocksdb/issues).

## License

Erlang RocksDB is licensed under the Apache License 2.
