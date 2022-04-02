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

include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (tab_separated_streams tab_separated_streams.cpp ${SRCS})
target_link_libraries (tab_separated_streams dbms clickhouse_functions)

add_executable (block_row_transforms block_row_transforms.cpp ${SRCS})
target_link_libraries (block_row_transforms dbms clickhouse_functions)

add_executable (expression_stream expression_stream.cpp ${SRCS})
target_link_libraries (expression_stream dbms clickhouse_storages_system clickhouse_functions)

add_executable (native_streams native_streams.cpp ${SRCS})
target_link_libraries (native_streams dbms clickhouse_functions)

add_executable (filter_stream filter_stream.cpp ${SRCS})
target_link_libraries (filter_stream dbms clickhouse_storages_system clickhouse_functions)

add_executable (filter_stream_hitlog filter_stream_hitlog.cpp ${SRCS})
target_link_libraries (filter_stream_hitlog dbms clickhouse_functions)

add_executable (sorting_stream sorting_stream.cpp ${SRCS})
target_link_libraries (sorting_stream dbms clickhouse_functions)

add_executable (union_stream2 union_stream2.cpp ${SRCS})
target_link_libraries (union_stream2 dbms clickhouse_functions)
