# Get absolute path to rocksdb sources (PROJECT_SOURCE_DIR is c_src)
get_filename_component(ROCKSDB_ROOT_DIR "${PROJECT_SOURCE_DIR}/../deps/rocksdb" ABSOLUTE)
message(STATUS "RocksDB source directory: ${ROCKSDB_ROOT_DIR}")

set(rocksdb_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

if (WITH_CCACHE AND CCACHE_FOUND)
    list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_CXX_COMPILER=ccache)
    list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_CXX_COMPILER_ARG1=${CMAKE_CXX_COMPILER})
  else()
    list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  endif()

list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
list(APPEND rocksdb_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
list(APPEND rocksdb_CMAKE_ARGS -DFAIL_ON_WARNINGS=OFF)
list(APPEND rocksdb_CMAKE_ARGS -DPORTABLE=ON)
list(APPEND rocksdb_CMAKE_ARGS -DUSE_RTTI=1)

list(APPEND rocksdb_CMAKE_ARGS -DWITH_TOOLS=OFF)
list(APPEND rocksdb_CMAKE_ARGS -DWITH_TESTS=OFF)


list(APPEND rocksdb_CMAKE_ARGS -DWITH_GFLAGS=OFF)
list(APPEND rocksdb_CMAKE_ARGS -DWITH_JEMALLOC=OFF)
list(APPEND rocksdb_CMAKE_ARGS -DWITH_TBB=${TBB_FOUND})

list(APPEND rocksdb_CMAKE_ARGS -DWITH_SNAPPY=${SNAPPY_FOUND})
list(APPEND rocksdb_CMAKE_ARGS -DWITH_LZ4=${LZ4_FOUND})
list(APPEND rocksdb_CMAKE_ARGS -DWITH_ZLIB=${ZLIB_FOUND})
list(APPEND rocksdb_CMAKE_ARGS -DWITH_BZ2=${BZIP2_FOUND})
list(APPEND rocksdb_CMAKE_ARGS -DWITH_ZSTD=${ZSTD_FOUND})

if(MSVC)
    list(APPEND rocksdb_CMAKE_ARGS -DWITHOUT_THIRDPARTY_INC=ON)
endif()

if(WITH_BUNDLE_SNAPPY)
    list(APPEND rocksdb_CMAKE_ARGS -Dsnappy_ROOT_DIR=${SNAPPY_ROOT_DIR})
endif()

if(WITH_BUNDLE_LZ4)
    list(APPEND rocksdb_CMAKE_ARGS -Dlz4_ROOT_DIR=${LZ4_ROOT_DIR})
endif()

message(STATUS "cmake args ${rocksdb_CMAKE_ARGS}")

include(ExternalProject)
if(MSVC)
   set(BUILD_CMD ${CMAKE_COMMAND}  --build . --config Release --target rocksdb -- -m)
else()
   set(BUILD_CMD $(MAKE) rocksdb)
endif()

ExternalProject_Add(rocksdb
    SOURCE_DIR "${ROCKSDB_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS ${rocksdb_CMAKE_ARGS}
    BUILD_COMMAND ${BUILD_CMD}
    INSTALL_COMMAND ""
    )

ExternalProject_Get_Property(rocksdb BINARY_DIR)

add_dependencies(${ErlangRocksDBNIF_TARGET} rocksdb)

if(WITH_BUNDLE_SNAPPY)
    ExternalProject_Add_StepDependencies(rocksdb build snappy)
endif()

if(WITH_BUNDLE_LZ4)
    ExternalProject_Add_StepDependencies(rocksdb build lz4)
endif()

if(MSVC)
    set(ROCKSDB_LIBRARIES
        ${BINARY_DIR}/Release/rocksdb${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(ROCKSDB_LIBRARIES
        ${BINARY_DIR}/librocksdb${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

set(ROCKSDB_FOUND TRUE)

set(ROCKSDB_INCLUDE_DIRS
    ${ROCKSDB_ROOT_DIR}/include)
message(STATUS "Found RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Found RocksDB includes: ${ROCKSDB_INCLUDE_DIRS}")

mark_as_advanced(
    ROCKSDB_ROOT_DIR
    ROCKSDB_LIBRARIES
    ROCKSDB_INCLUDE_DIRS
)
