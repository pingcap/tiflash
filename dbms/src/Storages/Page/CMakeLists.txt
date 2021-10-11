add_headers_and_sources(page_storage ./)
add_headers_and_sources(page_storage ./mvcc)
add_headers_and_sources(page_storage ./VersionSet)
add_headers_and_sources(page_storage ./gc)

add_headers_and_sources(new_page_storage ./NP)
add_headers_and_sources(new_page_storage ./NP/pagemap)
add_headers_and_sources(new_page_storage ./mvcc)
add_headers_and_sources(new_page_storage ./VersionSet)

## Add IO basic files excluding DataKeyManager. We don't want to add dependency for libtiflashproxy
add_headers_and_sources(io_base ${ClickHouse_SOURCE_DIR}/dbms/src/Encryption)
list(FILTER io_base_headers EXCLUDE REGEX "DataKeyManager.*")
list(FILTER io_base_sources EXCLUDE REGEX "DataKeyManager.*")
list(APPEND page_storage_headers
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.h
)
list(APPEND page_storage_sources 
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Interpreters/SettingsCommon.cpp
)

add_library(page_storage EXCLUDE_FROM_ALL
        ${page_storage_headers} ${page_storage_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_storage PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage clickhouse_common_io cpptoml
        kv_client tipb) # TODO: remove dependency on these libs. Now we need them for DB::Context


list(APPEND new_page_storage_sources 
        ${ClickHouse_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Encryption/RateLimiter.cpp
)

add_library(new_page_storage EXCLUDE_FROM_ALL
        ${new_page_storage_headers} ${new_page_storage_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(new_page_storage PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(new_page_storage clickhouse_common_io cpptoml kv_client tipb) 

### Build a control binary for PageStorage
## For `page_ctl`, we need to define `PAGE_STORAGE_UTIL_DEBUGGGING`
add_executable(page_ctl EXCLUDE_FROM_ALL
        tests/page_storage_ctl.cpp
        ${page_storage_headers} ${page_storage_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_ctl PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_ctl clickhouse_common_io cpptoml)
target_compile_options(page_ctl  PRIVATE -Wno-format)
target_compile_definitions(page_ctl PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING DBMS_PUBLIC_GTEST)
