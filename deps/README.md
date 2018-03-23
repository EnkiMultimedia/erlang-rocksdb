This directory contains all Barrel dependencies, except for the libc and utilities that should be provided by the operating system.


- **lz4** 1.8.1.2 : compression library
- **snappy** 1.1.4 : compression library
- **rocksdb** 5.10.4: db backend to store the data


## How to upgrade dependencies:

- **rocksdb** can be upgraded using the `update_rocksdb.sh`  script. Or just replace the rocksdb foolder by the last
  version

- **snappy**: download the latest archive, replace the `snappy` folder and  and make sure the configure script is available

- **lz4**: download the latest archive, replace the `lz4` folder by the new sources extracted from it
