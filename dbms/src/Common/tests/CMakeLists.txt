add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test dbms)

add_executable (sip_hash sip_hash.cpp)
target_link_libraries (sip_hash tiflash_common_io)

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf tiflash_common_io)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array tiflash_common_io)

add_executable (lru_cache lru_cache.cpp)
target_link_libraries (lru_cache tiflash_common_io)

add_executable (hash_table hash_table.cpp)
target_link_libraries (hash_table tiflash_common_io)

add_executable (small_table small_table.cpp)
target_link_libraries (small_table tiflash_common_io)

add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation tiflash_common_io)

add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 tiflash_common_io)

add_executable (int_hashes_perf int_hashes_perf.cpp AvalancheTest.cpp Random.cpp)
target_link_libraries (int_hashes_perf tiflash_common_io)

add_executable (simple_cache simple_cache.cpp)
target_include_directories (simple_cache PRIVATE ${DBMS_INCLUDE_DIR})
target_link_libraries (simple_cache common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array tiflash_common_io ${Boost_FILESYSTEM_LIBRARY})

add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort tiflash_common_io)

add_executable (shell_command_test shell_command_test.cpp)
target_link_libraries (shell_command_test tiflash_common_io)

add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists tiflash_common_io)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array tiflash_common_io)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency tiflash_common_io)

add_executable (thread_pool thread_pool.cpp)
target_link_libraries (thread_pool tiflash_common_io)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache tiflash_common_io)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving tiflash_common_io)

add_executable (integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_include_directories (integer_hash_tables_and_hashes BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (integer_hash_tables_and_hashes tiflash_common_io)

add_executable (allocator allocator.cpp)
target_link_libraries (allocator tiflash_common_io)

add_executable (cow_columns cow_columns.cpp)
target_link_libraries (cow_columns tiflash_common_io)

add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch tiflash_common_io)

add_executable (persisted_container persisted_container.cpp)
target_link_libraries (persisted_container tiflash_common_io)

add_executable(decimal_test_decimal_type gtest_decimal_type.cpp)
target_link_libraries(decimal_test_decimal_type tiflash_common_io gtest_main)

add_executable(mytime_test gtest_mytime.cpp)
target_link_libraries(mytime_test tiflash_common_io tiflash_functions gtest_main)

add_executable(rw_lock_test gtest_rw_lock.cpp)
target_link_libraries(rw_lock_test tiflash_common_io tiflash_functions gtest_main)

add_executable (uint128_perf uint128_perf.cpp)
target_link_libraries (uint128_perf tiflash_common_io)
