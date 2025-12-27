add_dependencies (${ErlangRocksDBNIF_TARGET} snappy)
include(GNUInstallDirs)

set(SNAPPY_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../deps/snappy")
set(SNAPPY_ROOT_DIR "${CMAKE_CURRENT_BINARY_DIR}/snappy")
set(SNAPPY_INCLUDE_DIR "${SNAPPY_ROOT_DIR}/include")
set(SNAPPY_STATIC_LIB "${SNAPPY_ROOT_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(SNAPPY_LIBRARY ${SNAPPY_STATIC_LIB})

include(ExternalProject)

# Build snappy with RTTI enabled to match RocksDB (which uses USE_RTTI=1)
# Snappy's CMakeLists.txt strips -frtti and adds -fno-rtti to CMAKE_CXX_FLAGS.
# We pass -frtti via CMAKE_CXX_FLAGS_RELEASE which gets appended AFTER base flags,
# so the final order is "... -fno-rtti ... -frtti" and last flag wins with gcc/clang.
if(MSVC)
    set(SNAPPY_RTTI_FLAG "/GR")
else()
    set(SNAPPY_RTTI_FLAG "-frtti")
endif()

ExternalProject_Add(snappy
    SOURCE_DIR "${SNAPPY_SOURCE_DIR}"
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${SNAPPY_ROOT_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5
        -DCMAKE_BUILD_TYPE=Release
        -DBUILD_SHARED_LIBS=OFF
        -DSNAPPY_BUILD_TESTS=OFF
        -DSNAPPY_BUILD_BENCHMARKS=OFF
        -DCMAKE_CXX_FLAGS_RELEASE=${SNAPPY_RTTI_FLAG}
    BINARY_DIR ${SNAPPY_ROOT_DIR}
    BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}"
    )

set(SNAPPY_FOUND TRUE)

message(STATUS "Snappy library: ${SNAPPY_LIBRARY}")
message(STATUS "Snappy includes: ${SNAPPY_INCLUDE_DIR}")

mark_as_advanced(
    SNAPPY_ROOT_DIR
    SNAPPY_LIBRARY
    SNAPPY_INCLUDE_DIR
)

