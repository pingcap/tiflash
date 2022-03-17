# Copyright 2022 PingCAP, Ltd.
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

add_executable (exception exception.cpp gtest_decimal_comparison.cpp)
target_link_libraries (exception clickhouse_common_io)

add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool clickhouse_common_io)
target_include_directories (string_pool BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})

add_executable (field field.cpp)
target_link_libraries (field dbms)

add_executable (move_field move_field.cpp)
target_link_libraries (move_field clickhouse_common_io)

add_executable (rvo_test rvo_test.cpp)
target_link_libraries (rvo_test Threads::Threads)

add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash clickhouse_common_io)
