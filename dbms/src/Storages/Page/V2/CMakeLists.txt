add_headers_and_sources(page_storage_v2 ./)
add_headers_and_sources(page_storage_v2 ./mvcc)
add_headers_and_sources(page_storage_v2 ./VersionSet)
add_headers_and_sources(page_storage_v2 ./gc)
## Add IO basic files excluding DataKeyManager. We don't want to add dependency for libtiflashproxy
add_headers_and_sources(io_base ${ClickHouse_SOURCE_DIR}/dbms/src/Encryption)
list(FILTER io_base_headers EXCLUDE REGEX "DataKeyManager.*")
list(FILTER io_base_sources EXCLUDE REGEX "DataKeyManager.*")
list(APPEND page_storage_v2_headers
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/Page.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/PageDefines.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/PageStorage.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/WriteBatch.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.h
)
list(APPEND page_storage_v2_sources 
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Interpreters/SettingsCommon.cpp
)
add_library(page_storage_v2 EXCLUDE_FROM_ALL
        ${page_storage_v2_headers} ${page_storage_v2_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_storage_v2 PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage_v2 clickhouse_common_io cpptoml
        kv_client tipb) # TODO: remove dependency on these libs. Now we need them for DB::Context

### Build a control binary for PageStorage
## For `page_ctl`, we need to define `PAGE_STORAGE_UTIL_DEBUGGGING`
add_executable(page_ctl EXCLUDE_FROM_ALL
        tests/page_storage_ctl.cpp
        ${page_storage_v2_headers} ${page_storage_v2_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_ctl PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_ctl clickhouse_common_io cpptoml)
target_compile_options(page_ctl  PRIVATE -Wno-format)
target_compile_definitions(page_ctl PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING DBMS_PUBLIC_GTEST)
