auto_sources(hfiles "*.h" RECURSE "${ERLANG_ROCKSDB_SRC_DIR}")
auto_sources(files "*.cpp" RECURSE "${ERLANG_ROCKSDB_SRC_DIR}")

include_directories(
  SYSTEM
  ${Erlang_EI_INCLUDE_DIR}
  ${Erlang_EI_DIR}/src
)


link_directories(
  ${Erlang_EI_LIBRARY_DIR}
)

add_executable(../priv/rocksdb.so
    ${hfiles}
    ${files}
)

target_link_libraries(../priv/rocksdb.so ${Erlang_EI_LIBRARIES})
target_link_libraries(../priv/rocksdb.so ${ERLANG_ROCKSDB_EXTERNAL_DEPS}