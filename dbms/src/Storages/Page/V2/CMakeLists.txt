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

add_headers_and_sources(page_storage_v2 ./)
add_headers_and_sources(page_storage_v2 ./VersionSet)
add_headers_and_sources(page_storage_v2 ./gc)
## Add IO basic files excluding DataKeyManager. We don't want to add dependency for libtiflashproxy
add_headers_and_sources(io_base ${TiFlash_SOURCE_DIR}/dbms/src/Encryption)
list(FILTER io_base_headers EXCLUDE REGEX "DataKeyManager.*")
list(FILTER io_base_sources EXCLUDE REGEX "DataKeyManager.*")
list(APPEND page_storage_v2_headers
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/PathPool.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/Page.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/PageDefines.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/PageStorage.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/WriteBatch.h
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.h
)
list(APPEND page_storage_v2_sources 
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/PathPool.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${TiFlash_SOURCE_DIR}/dbms/src/Interpreters/SettingsCommon.cpp
)
add_library(page_storage_v2 EXCLUDE_FROM_ALL
        ${page_storage_v2_headers} ${page_storage_v2_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_storage_v2 PUBLIC ${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage_v2 clickhouse_common_io cpptoml
        kv_client tipb) # TODO: remove dependency on these libs. Now we need them for DB::Context

### Build a control binary for PageStorage
## For `page_ctl`, we need to define `PAGE_STORAGE_UTIL_DEBUGGGING`
add_executable(page_ctl EXCLUDE_FROM_ALL
        tests/page_storage_ctl.cpp
        ${page_storage_v2_headers} ${page_storage_v2_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_ctl PUBLIC ${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_ctl clickhouse_common_io cpptoml)
target_compile_options(page_ctl  PRIVATE -Wno-format)
target_compile_definitions(page_ctl PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING DBMS_PUBLIC_GTEST)
