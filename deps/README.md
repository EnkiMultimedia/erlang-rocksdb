This directory contains all Barrel dependencies, except for the libc and utilities that should be provided by the operating system.


- **lz4** 1.8.3 : compression library
- **snappy** 1.1.10 : compression library
  - Commit [27f34a58](https://github.com/google/snappy/tree/27f34a580be4a3becf5f8c0cba13433f53c21337) due to an unreleased fix.
- **rocksdb** 5.18.3: db backend to store the data

## How to upgrade dependencies:

- **rocksdb** follow instructions on our [rocksdb](https://gitlab.com/barrel-db/Deps/rocksdb) repository

- **snappy**: download the latest archive, replace the `snappy` folder and  and make sure the configure script is available

- **lz4**: download the latest archive, replace the `lz4` folder by the new sources extracted from it
