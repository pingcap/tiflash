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

add_headers_and_sources(kvstore .)
add_headers_and_sources(kvstore ./FFI)
add_headers_and_sources(kvstore ./MultiRaft)
add_headers_and_sources(kvstore ./MultiRaft/Disagg)
add_headers_and_sources(kvstore ./Utils)
add_headers_and_sources(kvstore ./TiKVHelpers)
add_headers_and_sources(kvstore ./Decode)
add_headers_and_sources(kvstore ./Read)

add_subdirectory (./MultiRaft/Disagg)

add_library(kvstore ${kvstore_headers} ${kvstore_sources})
target_link_libraries(kvstore PRIVATE dbms page KVStoreFastAddPeerProto)

if (ENABLE_TESTS)
    add_headers_and_sources(kvstore tests)
endif ()
