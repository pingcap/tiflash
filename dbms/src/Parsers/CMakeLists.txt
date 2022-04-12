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

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_parsers .)
add_library(clickhouse_parsers ${SPLIT_SHARED} ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
target_link_libraries (clickhouse_parsers clickhouse_common_io)
target_include_directories (clickhouse_parsers PUBLIC ${DBMS_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
endif ()
