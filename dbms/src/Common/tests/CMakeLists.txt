# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable (hashes_test hashes_test.cpp)
target_link_libraries (hashes_test dbms)

add_executable (sip_hash sip_hash.cpp)
target_link_libraries (sip_hash clickhouse_common_io)

add_executable (sip_hash_perf sip_hash_perf.cpp)
target_link_libraries (sip_hash_perf clickhouse_common_io)

add_executable (auto_array auto_array.cpp)
target_link_libraries (auto_array clickhouse_common_io)

add_executable (hash_table hash_table.cpp)
target_link_libraries (hash_table clickhouse_common_io)

add_executable (small_table small_table.cpp)
target_link_libraries (small_table clickhouse_common_io)

add_executable (parallel_aggregation parallel_aggregation.cpp)
target_link_libraries (parallel_aggregation clickhouse_common_io)

add_executable (parallel_aggregation2 parallel_aggregation2.cpp)
target_link_libraries (parallel_aggregation2 clickhouse_common_io)

add_executable (int_hashes_perf int_hashes_perf.cpp AvalancheTest.cpp Random.cpp)
target_link_libraries (int_hashes_perf clickhouse_common_io)

add_executable (simple_cache simple_cache.cpp)
target_include_directories (simple_cache PRIVATE ${DBMS_INCLUDE_DIR})
target_link_libraries (simple_cache common)

add_executable (compact_array compact_array.cpp)
target_link_libraries (compact_array clickhouse_common_io ${Boost_FILESYSTEM_LIBRARY})

add_executable (radix_sort radix_sort.cpp)
target_link_libraries (radix_sort clickhouse_common_io)

add_executable (shell_command_test shell_command_test.cpp)
target_link_libraries (shell_command_test clickhouse_common_io)

add_executable (arena_with_free_lists arena_with_free_lists.cpp)
target_link_libraries (arena_with_free_lists clickhouse_common_io)

add_executable (pod_array pod_array.cpp)
target_link_libraries (pod_array clickhouse_common_io)

add_executable (thread_creation_latency thread_creation_latency.cpp)
target_link_libraries (thread_creation_latency clickhouse_common_io)

add_executable (thread_pool thread_pool.cpp)
target_link_libraries (thread_pool clickhouse_common_io)

add_executable (array_cache array_cache.cpp)
target_link_libraries (array_cache clickhouse_common_io)

add_executable (space_saving space_saving.cpp)
target_link_libraries (space_saving clickhouse_common_io)

add_executable (integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_include_directories (integer_hash_tables_and_hashes BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (integer_hash_tables_and_hashes clickhouse_common_io)

add_executable (cow_columns cow_columns.cpp)
target_link_libraries (cow_columns clickhouse_common_io)

add_executable (stopwatch stopwatch.cpp)
target_link_libraries (stopwatch clickhouse_common_io)

add_executable (persisted_container persisted_container.cpp)
target_link_libraries (persisted_container clickhouse_common_io)

# performance tests

add_executable (uint128_perf uint128_perf.cpp)
target_link_libraries (uint128_perf clickhouse_common_io)

add_executable (mpmc_queue_perftest mpmc_queue_perftest.cpp)
target_link_libraries (mpmc_queue_perftest clickhouse_common_io)

add_executable (allocator_perf allocator_perf.cpp)
target_link_libraries (allocator_perf clickhouse_common_io)

add_executable(bench_common
    bench_logger.cpp
)
target_link_libraries(bench_common benchmark::benchmark_main clickhouse_common_io)
