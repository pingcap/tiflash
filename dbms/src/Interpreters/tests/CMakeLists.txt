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

add_executable (expression expression.cpp)
target_link_libraries (expression dbms)

add_executable (create_query create_query.cpp)
target_link_libraries (create_query dbms)

add_executable (select_query select_query.cpp)
target_link_libraries (select_query clickhouse_storages_system dbms)

add_executable (aggregate aggregate.cpp)
target_link_libraries (aggregate dbms)

add_executable (hash_map hash_map.cpp)
target_include_directories (hash_map BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map dbms)

add_executable (hash_map3 hash_map3.cpp)
target_link_libraries (hash_map3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})

add_executable (hash_map_string hash_map_string.cpp)
target_include_directories (hash_map_string BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string dbms)

add_executable (hash_map_string_2 hash_map_string_2.cpp)
target_link_libraries (hash_map_string_2 dbms)

add_executable (hash_map_string_3 hash_map_string_3.cpp)
target_link_libraries (hash_map_string_3 dbms ${FARMHASH_LIBRARIES} ${METROHASH_LIBRARIES})
target_include_directories (hash_map_string_3 BEFORE PRIVATE ${TiFlash_SOURCE_DIR}/contrib/libfarmhash)
target_include_directories (hash_map_string_3 BEFORE PRIVATE ${TiFlash_SOURCE_DIR}/contrib/libmetrohash/src)

add_executable (hash_map_string_small hash_map_string_small.cpp)
target_include_directories (hash_map_string_small BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (hash_map_string_small dbms)

add_executable (two_level_hash_map two_level_hash_map.cpp)
target_include_directories (two_level_hash_map BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (two_level_hash_map dbms)

add_executable (logical_expressions_optimizer logical_expressions_optimizer.cpp)
target_link_libraries (logical_expressions_optimizer dbms)

add_executable (users users.cpp)
target_link_libraries (users dbms ${Boost_FILESYSTEM_LIBRARY} dbms)
