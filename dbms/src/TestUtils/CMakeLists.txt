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
list(REMOVE_ITEM test_util_sources "bench_dbms_main.cpp" "gtests_dbms_main.cpp" "TimeZoneTestData.cpp")

# Embed the pinned TZif files so installed test binaries do not need the source tree or host tzdata.
set(timezone_test_data_dir "${TiFlash_SOURCE_DIR}/contrib/cctz/testdata")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${timezone_test_data_dir}/version")
file(STRINGS "${timezone_test_data_dir}/version" timezone_test_data_version LIMIT_COUNT 1)
set(timezone_test_data "static constexpr std::string_view timezone_test_data_version = \"${timezone_test_data_version}\";\n")
string(APPEND timezone_test_data "static constexpr std::pair<std::string_view, std::string_view> timezone_test_data[] = {\n")
foreach(zone
    Africa/Monrovia
    America/Chicago
    America/Goose_Bay
    America/New_York
    America/Santiago
    America/St_Johns
    Asia/Shanghai
    Asia/Singapore
    Australia/Lord_Howe
    EST
    Europe/Amsterdam
    Europe/Riga
    Pacific/Apia
    Pacific/Kwajalein
    UTC)
    set(zone_file "${timezone_test_data_dir}/zoneinfo/${zone}")
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${zone_file}")
    file(READ "${zone_file}" zone_hex HEX)
    file(SIZE "${zone_file}" zone_size)
    string(REGEX REPLACE "(..)" "\\\\x\\1" zone_bytes "${zone_hex}")
    string(APPEND timezone_test_data "    {\"${zone}\", {\"${zone_bytes}\", ${zone_size}}},\n")
endforeach()
string(APPEND timezone_test_data "};\n")
file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/TimeZoneTestData.inc" CONTENT "${timezone_test_data}")

add_library(test_util_gtest_main ${test_util_headers} ${test_util_sources} gtests_dbms_main.cpp TimeZoneTestData.cpp)
target_link_libraries(test_util_gtest_main dbms gtest_main tiflash_aggregate_functions grpc_server_holder)
target_include_directories(test_util_gtest_main PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_compile_definitions(test_util_gtest_main PRIVATE DBMS_PUBLIC_GTEST)

add_library(test_util_bench_main ${test_util_headers} ${test_util_sources} bench_dbms_main.cpp)
target_link_libraries(test_util_bench_main dbms gtest_main benchmark)
target_include_directories(test_util_bench_main BEFORE PUBLIC ${benchmark_SOURCE_DIR}/include)
target_compile_definitions(test_util_bench_main PRIVATE DBMS_PUBLIC_GTEST)
