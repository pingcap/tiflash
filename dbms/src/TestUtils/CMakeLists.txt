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

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(test_util .)
list(REMOVE_ITEM test_util_sources "bench_dbms_main.cpp" "gtests_dbms_main.cpp")

add_library(test_util_gtest_main ${test_util_headers} ${test_util_sources} gtests_dbms_main.cpp)
target_link_libraries(test_util_gtest_main dbms gtest_main tiflash_aggregate_functions server_for_test)
target_compile_definitions(test_util_gtest_main PRIVATE DBMS_PUBLIC_GTEST)

add_library(test_util_bench_main ${test_util_headers} ${test_util_sources} bench_dbms_main.cpp)
target_link_libraries(test_util_bench_main dbms gtest_main benchmark)
target_include_directories(test_util_bench_main BEFORE PUBLIC ${benchmark_SOURCE_DIR}/include)
target_compile_definitions(test_util_bench_main PRIVATE DBMS_PUBLIC_GTEST)
