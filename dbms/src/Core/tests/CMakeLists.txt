add_executable (exception EXCLUDE_FROM_ALL exception.cpp)
target_link_libraries (exception clickhouse_common_io)

add_executable (string_pool EXCLUDE_FROM_ALL string_pool.cpp)
target_link_libraries (string_pool clickhouse_common_io)
target_include_directories (string_pool BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

add_executable (field EXCLUDE_FROM_ALL field.cpp)
target_link_libraries (field dbms)

add_executable (move_field EXCLUDE_FROM_ALL move_field.cpp)
target_link_libraries (move_field clickhouse_common_io)

add_executable (rvo_test EXCLUDE_FROM_ALL rvo_test.cpp)
target_link_libraries (rvo_test Threads::Threads)

add_executable (string_ref_hash EXCLUDE_FROM_ALL string_ref_hash.cpp)
target_link_libraries (string_ref_hash clickhouse_common_io)
