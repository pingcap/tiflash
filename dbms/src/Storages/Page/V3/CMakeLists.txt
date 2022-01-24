add_headers_and_sources(page_storage_v3 ./)
add_headers_and_sources(page_storage_v3 ./spacemap)


list(APPEND page_storage_v3_sources
    ${TiFlash_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Storages/Page/PageUtil.cpp
    ${TiFlash_SOURCE_DIR}/dbms/src/Encryption/RateLimiter.cpp
)

add_library(page_storage_v3 EXCLUDE_FROM_ALL
    ${page_storage_v3_headers} ${page_storage_v3_sources}
    ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_storage_v3 PUBLIC ${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage_v3 clickhouse_common_io cpptoml kv_client tipb) 