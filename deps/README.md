This directory contains all erlang-rocksdb dependencies, except for the libc and utilities that should be provided by the operating system.

## Dependencies

- **rocksdb** 10.7.5: key-value storage engine
- **lz4** 1.8.3: compression library
- **snappy**: compression library
- **CRoaring** 4.5.1: roaring bitmap library (used for posting list V2 format)

## How to upgrade dependencies:

- **rocksdb**: download the latest archive from [RocksDB releases](https://github.com/facebook/rocksdb/releases), extract and replace the `rocksdb` folder

- **snappy**: download the latest archive from [Snappy releases](https://github.com/google/snappy/releases), replace the `snappy` folder

- **lz4**: download the latest archive from [LZ4 releases](https://github.com/lz4/lz4/releases), replace the `lz4` folder

- **CRoaring**: download the latest archive from [CRoaring releases](https://github.com/RoaringBitmap/CRoaring/releases), replace the `CRoaring` folder
