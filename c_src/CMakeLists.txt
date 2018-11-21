cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(ErlangRocksDB)

set(ErlangRocksDB_TARGET erocksdb)

set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})

#set(CMAKE_BUILD_TYPE Release)
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/Modules")

# Use lz4 provided by host system
option(ERLANG_ROCKSDB_USE_SYSTEM_LZ4 "Use lz4 from host system" ON)

# Use snappy provided by host system
option(ERLANG_ROCKSDB_USE_SYSTEM_SNAPPY "Use snappy from host system" ON)

# Use RocksDB provided by host system
# (for users with extra requirements)
option(ERLANG_ROCKSDB_USE_SYSTEM_ROCKSDB "Use RocksDB from host system" ON)

# Build and distribute RocksDB tools (e.g. sst_dump, etc)
option(ERLANG_ROCKSDB_ADD_ROCKSDB_TOOLS "Build and add rocksdb tools" OFF)

# Build RocksDB and link it statically with dependencies
option(ERLANG_ROCKSDB_BUILD_STATIC "Link libraries statically" ON)

if(ERLANG_ROCKSDB_BUILD_STATIC)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a .lib)
endif(ERLANG_ROCKSDB_BUILD_STATIC)
unset(ERLANG_ROCKSDB_BUILD_STATIC CACHE)

#--------------------------------------------------------------------------------
include(FindErlang)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${ERLANG_EI_LIBRARY_PATH})

find_library(ERLANG_EI_LIB ei)
find_library(ERLANG_ERL_INTERFACE_LIB erl_interface)
include_directories(${ERLANG_EI_INCLUDE_PATH})
include_directories(${ERLANG_ERTS_INCLUDE_PATH})
#--------------------------------------------------------------------------------

if(ERLANG_ROCKSDB_USE_SYSTEM_ROCKSDB)
    find_package(RocksDB)
else()
    # Build rocksdb from deps
endif(ERLANG_ROCKSDB_USE_SYSTEM_ROCKSDB)
unset(ERLANG_ROCKSDB_USE_SYSTEM_ROCKSDB CACHE)

#--------------------------------------------------------------------------------

if(ERLANG_ROCKSDB_USE_SYSTEM_LZ4)
    find_package(LZ4)
else()
    # Build lz4 from deps
endif(ERLANG_ROCKSDB_USE_SYSTEM_LZ4)
unset(ERLANG_ROCKSDB_USE_SYSTEM_LZ4 CACHE)

#--------------------------------------------------------------------------------

if(ERLANG_ROCKSDB_USE_SYSTEM_SNAPPY)
    find_package(Snappy)
else()
    # Build snappy from deps
endif(ERLANG_ROCKSDB_USE_SYSTEM_SNAPPY)
unset(ERLANG_ROCKSDB_USE_SYSTEM_SNAPPY CACHE)

#--------------------------------------------------------------------------------

add_library(${ErlangRocksDB_TARGET} MODULE "")

set_property(TARGET ${ErlangRocksDB_TARGET} PROPERTY CXX_STANDARD 14)
set_property(TARGET ${ErlangRocksDB_TARGET} PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET ${ErlangRocksDB_TARGET} PROPERTY CXX_EXTENSIONS OFF)

target_sources(${ErlangRocksDB_TARGET}
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb.cc
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/backup.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/counter_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_column_family.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rate_limiter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/batch.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/env.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_db.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/refobjects.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/bitset_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erlang_merge.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_iter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/transactions.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cache.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_snapshot.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/util.cc
)

set_source_files_properties(
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb.cc
    PROPERTIES COMPILE_OPTIONS "-Wno-missing-field-initializers;-Wno-unused-parameter")

set_source_files_properties(
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_column_family.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_db.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/backup.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/counter_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/env.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/batch.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_db.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rate_limiter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_snapshot.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/transactions.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cache.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erlang_merge.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/refobjects.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/bitset_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_iter.cc
    PROPERTIES COMPILE_OPTIONS "-Wno-unused-parameter")

target_include_directories(${ErlangRocksDB_TARGET}
    PRIVATE
    ${DEFAULT_INCLUDE_DIRECTORIES}
    ${PROJECT_BINARY_DIR}
)

target_compile_options(${ErlangRocksDB_TARGET}
    PUBLIC
    -Wall
    -Wextra
#    -pedantic-errors
)

set(TARGET_LINKER_OPTIONS)
if(APPLE)
	set(TARGET_LINKER_OPTIONS "-flat_namespace -undefined suppress")
endif(APPLE)

target_link_libraries(${ErlangRocksDB_TARGET}
  PUBLIC
  ${TARGET_LINKER_OPTIONS}
  ${ERLANG_ERL_INTERFACE_LIB}
  ${ERLANG_EI_LIB}
  ${ROCKSDB_LIBRARIES}
  ${LZ4_LIBRARY}
  ${SNAPPY_LIBRARY}
)