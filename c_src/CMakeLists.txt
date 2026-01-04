cmake_minimum_required(VERSION 3.5.0 FATAL_ERROR)

project(ErlangRocksDBNIF)


if(POLICY CMP0028)
  cmake_policy(SET CMP0028 NEW)
endif()
if(POLICY CMP0046)
  cmake_policy(SET CMP0046 NEW)
endif()
if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()
if(POLICY CMP0056)
  cmake_policy(SET CMP0056 NEW)
endif()
if(POLICY CMP0065)
  cmake_policy(SET CMP0065 NEW)
endif()
if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()
if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()


list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Add standard package manager paths for macOS (MacPorts and Homebrew)
if(APPLE)
    # MacPorts
    if(EXISTS "/opt/local")
        list(APPEND CMAKE_PREFIX_PATH "/opt/local")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/local/lib")
    endif()
    # Homebrew on Apple Silicon
    if(EXISTS "/opt/homebrew")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/homebrew/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/homebrew/lib")
    endif()
    # Homebrew on Intel (default location)
    if(EXISTS "/usr/local/include")
        list(APPEND CMAKE_PREFIX_PATH "/usr/local")
        list(APPEND CMAKE_INCLUDE_PATH "/usr/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib")
    endif()
endif()

# includes
# set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})
if(MSVC)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
endif()

#set( _erocksdb_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
#if(WIN32)
#    list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 .lib .a .dll .so)
#else()
#    set(CMAKE_FIND_LIBRARY_SUFFIXES .a .so .dylib)
#endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Check for C++20 support (required by RocksDB 10.x)
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++20" COMPILER_SUPPORTS_CXX20)
if(NOT COMPILER_SUPPORTS_CXX20)
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} does not support C++20. "
                        "RocksDB 10.x requires C++20. Please use GCC >= 11 or Clang >= 10.")
endif()

if (NOT WIN32)
  # On platforms like FreeBSD where gmake is not the default, we need
  # to look for gmake explicitly to make sure LevelDB can build.
  # TODO(dforsyth): Remove this once LevelDB is upgraded to a version
  # that uses CMake. See MESOS-8757.
  find_program(
    MAKE_PROGRAM
    NAMES gmake make
    DOCS "External 'make' executable.")

  if (NOT MAKE_PROGRAM)
    message(FATAL_ERROR "Could not find an external 'make' executable.")
  endif ()
endif ()



set(ErlangRocksDBNIF_TARGET erocksdb)
add_library(${ErlangRocksDBNIF_TARGET} MODULE "")

if(MSVC)
  set_target_properties(${ErlangRocksDBNIF_TARGET} PROPERTIES OUTPUT_NAME "liberocksdb")
endif()

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_THREAD_PREFER_PTHREAD ON)

option(WITH_CCACHE "Build with ccache." OFF)
if(WITH_CCACHE)
  find_program(CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Building with ccache: ${CCACHE_FOUND}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
  else(CCACHE_FOUND)
    message(FATAL_ERROR "Can't find ccache. Is it installed?")
  endif(CCACHE_FOUND)
endif(WITH_CCACHE)

# Compression library options
# LZ4 and Snappy: auto-detect by default, unless WITH_BUNDLE_* is set
# Other compression libs: only enabled if explicitly requested
option(WITH_BUNDLE_LZ4 "Use bundled lz4 instead of system" OFF)
option(WITH_BUNDLE_SNAPPY "Use bundled snappy instead of system" OFF)
option(WITH_ZSTD "build with zstd" OFF)
option(WITH_ZLIB "build with zlib" OFF)
option(WITH_BZ2 "build with bzip2" OFF)

# LZ4: use bundled if requested, otherwise auto-detect
if(WITH_BUNDLE_LZ4)
    set(WITH_LZ4 OFF)
else()
    option(WITH_LZ4 "build with lz4" ON)
endif()

# Snappy: use bundled if requested, otherwise auto-detect
if(WITH_BUNDLE_SNAPPY)
    set(WITH_SNAPPY OFF)
else()
    option(WITH_SNAPPY "build with SNAPPY" ON)
endif()

if(WITH_ZLIB)
    find_package(zlib REQUIRED)
    add_definitions(-DZLIB)
    if(ZLIB_INCLUDE_DIRS)
        # CMake 3
        include_directories(${ZLIB_INCLUDE_DIRS})
    else()
        # CMake 2
        include_directories(${ZLIB_INCLUDE_DIR})
    endif()
    list(APPEND THIRDPARTY_LIBS ${ZLIB_LIBRARIES})
endif()

if(WITH_BZ2)
    find_package(BZip2 REQUIRED)
endif()

# LZ4: use bundled or find system library
if(WITH_BUNDLE_LZ4)
    include(build-lz4)
elseif(WITH_LZ4)
    find_package(LZ4)
    if(NOT LZ4_FOUND)
        message(STATUS "LZ4 not found, disabling LZ4 support")
        set(WITH_LZ4 OFF)
    endif()
endif()

# Snappy: use bundled or find system library
if(WITH_BUNDLE_SNAPPY)
    include(build-snappy)
elseif(WITH_SNAPPY)
    find_package(Snappy)
    if(NOT SNAPPY_FOUND AND NOT Snappy_FOUND)
        message(STATUS "Snappy not found, disabling Snappy support")
        set(WITH_SNAPPY OFF)
    endif()
endif()

# Zstd: find system library
if(WITH_ZSTD)
    find_package(Zstd)
    if(NOT ZSTD_FOUND AND NOT Zstd_FOUND)
        message(STATUS "Zstd not found, disabling Zstd support")
        set(WITH_ZSTD OFF)
    endif()
endif()


option(WITH_TBB "build with Threading Building Blocks (TBB)" OFF)
if(WITH_TBB)
  find_package(TBB REQUIRED)
endif()

# Use RocksDB provided by host system
# (for users with extra requirements)
option(WITH_SYSTEM_ROCKSDB "require and build with system rocksdb" OFF)
if (WITH_SYSTEM_ROCKSDB)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
  find_package(RocksDB 6.13.3 REQUIRED)
else()
  include(build-rocksdb)
endif()


#--------------------------------------------------------------------------------
include(FindErlang)

set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${ERLANG_EI_LIBRARY_PATH})

find_library(ERLANG_EI_LIB ei)
find_library(ERLANG_ERL_INTERFACE_LIB erl_interface)
include_directories(${ERLANG_EI_INCLUDE_PATH})
include_directories(${ERLANG_ERTS_INCLUDE_PATH})
#--------------------------------------------------------------------------------

include_directories(${ZSTD_INCLUDE_DIR})
include_directories(${LZ4_INCLUDE_DIR})
include_directories(${BZIP2_INCLUDE_DIR})
include_directories(${ZLIB_INCLUDE_DIR})
include_directories(${SNAPPY_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIRS})
#include_directories(${LIBGFLAGS_INCLUDE_DIR})

#--------------------------------------------------------------------------------

set_property(TARGET ${ErlangRocksDBNIF_TARGET} PROPERTY CXX_STANDARD 20)
set_property(TARGET ${ErlangRocksDBNIF_TARGET} PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET ${ErlangRocksDBNIF_TARGET} PROPERTY CXX_EXTENSIONS OFF)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /W4 /wd4127 /wd4800 /wd4996 /wd4351 /wd4100 /wd4204 /wd4324")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-strict-aliasing")
  if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

target_sources(${ErlangRocksDBNIF_TARGET}
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb.cc
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/backup.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/batch.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/pessimistic_transaction.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/bitset_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cache.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/counter_merge_operator.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/env.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erlang_merge.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/compaction_filter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_column_family.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_db.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_iter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/erocksdb_snapshot.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rate_limiter.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/refobjects.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/sst_file_manager.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/sst_file_writer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/sst_file_reader.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/transaction_log.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/util.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/write_buffer_manager.cc
)

target_include_directories(${ErlangRocksDBNIF_TARGET}
    PRIVATE
    ${DEFAULT_INCLUDE_DIRECTORIES}
    ${ROCKSDB_INCLUDE_DIR}
    ${ZSTD_INCLUDE_DIR}
    ${LZ4_INCLUDE_DIR}
    ${SNAPPY_INCLUDE_DIR}
    ${PROJECT_BINARY_DIR}
)

#target_compile_options(${ErlangRocksDBNIF_TARGET}
#    PUBLIC
#    -Wall
#    -Wextra
#    -pedantic-errors
#)

set(TARGET_LINKER_OPTIONS)
if(APPLE)
	set(TARGET_LINKER_OPTIONS "-flat_namespace -undefined suppress")
endif(APPLE)

# On Linux with bundled snappy, we need --whole-archive to include all symbols
# including RTTI symbols that are needed at runtime but not directly referenced
if(WITH_BUNDLE_SNAPPY AND NOT APPLE AND NOT MSVC)
    # Handle snappy separately with whole-archive wrapper
    set(SNAPPY_LINK_LIBS "")
else()
    set(SNAPPY_LINK_LIBS ${SNAPPY_LIBRARY})
endif()

target_link_libraries(
    ${ErlangRocksDBNIF_TARGET}
    PRIVATE
    ${TARGET_LINKER_OPTIONS}
    ${ROCKSDB_LIBRARIES}
    ${ZSTD_LIBRARY}
    ${LZ4_LIBRARY}
    ${SNAPPY_LINK_LIBS}
    ${BZIP2_LIBRARIES}
    ${ZLIB_LIBRARY}
    ${TBB_LIBRARIES}
)

# Link bundled snappy with --whole-archive to include RTTI symbols
if(WITH_BUNDLE_SNAPPY AND NOT APPLE AND NOT MSVC)
    target_link_libraries(
        ${ErlangRocksDBNIF_TARGET}
        PRIVATE
        "-Wl,--whole-archive"
        "${SNAPPY_LIBRARY}"
        "-Wl,--no-whole-archive"
    )
endif()

if(MSVC)
	target_link_libraries(
		${ErlangRocksDBNIF_TARGET}
		PRIVATE
		Shlwapi
		Rpcrt4
	)
endif(MSVC)
