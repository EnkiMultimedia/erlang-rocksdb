set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy REQUIRED)
find_package(GFlags REQUIRED)
find_package(Erlang  REQUIRED)

include_directories(${LZ4_INCLUDE_DIR})
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${ZLIB_INCLUDE_DIR})
include_directories(${SNAPPY_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIRS})