# Copyright 2024 PingCAP, Inc.
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

add_subdirectory(dtpb)

include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_subdirectory(./Remote/Proto)

add_headers_and_sources(delta_merge .)
add_headers_and_sources(delta_merge ./BitmapFilter)
add_headers_and_sources(delta_merge ./Index)
add_headers_and_sources(delta_merge ./Index/VectorIndexHNSW)
add_headers_and_sources(delta_merge ./Filter)
add_headers_and_sources(delta_merge ./FilterParser)
add_headers_and_sources(delta_merge ./File)
add_headers_and_sources(delta_merge ./ColumnFile)
add_headers_and_sources(delta_merge ./Delta)
add_headers_and_sources(delta_merge ./ReadThread)
add_headers_and_sources(delta_merge ./Remote)
add_headers_and_sources(delta_merge ./Remote/DataStore)
add_headers_and_sources(delta_merge ./Decode)
add_headers_and_sources(delta_merge ./StoragePool)

add_library(delta_merge ${delta_merge_headers} ${delta_merge_sources})
target_link_libraries(delta_merge PRIVATE dbms page)

add_subdirectory(workload)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()
