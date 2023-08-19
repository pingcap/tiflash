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

include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (exchange_perftest
    exchange_perftest.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Functions/FunctionsConversion.cpp)
target_link_libraries (exchange_perftest
    gtest_main
    dbms
    clickhouse_functions
    clickhouse_aggregate_functions
    tiflash-dttool-lib)
