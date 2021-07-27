add_headers_and_sources(page_storage ./)
add_headers_and_sources(page_storage ./mvcc)
add_headers_and_sources(page_storage ./VersionSet)
add_headers_and_sources(page_storage ./gc)
## Add IO basic files excluding DataKeyManager. We don't want to add dependency for libtiflashproxy
add_headers_and_sources(io_base ${TIFLASH_SOURCE_DIR}/dbms/src/Encryption)
list(FILTER io_base_headers EXCLUDE REGEX "DataKeyManager.*")
list(FILTER io_base_sources EXCLUDE REGEX "DataKeyManager.*")
list(APPEND page_storage_headers
        ${TIFLASH_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.h
        ${TIFLASH_SOURCE_DIR}/dbms/src/Storages/PathPool.h
        ${TIFLASH_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.h
)
list(APPEND page_storage_sources 
        ${TIFLASH_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.cpp
        ${TIFLASH_SOURCE_DIR}/dbms/src/Storages/PathPool.cpp
        ${TIFLASH_SOURCE_DIR}/dbms/src/Server/StorageConfigParser.cpp
)
add_library(page_storage EXCLUDE_FROM_ALL
        ${page_storage_headers} ${page_storage_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_storage PUBLIC ${TIFLASH_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage tiflash_common_io
        kv_client tipb) # TODO: remove dependency on these libs. Now we need them for DB::Context

### Build a control binary for PageStorage
## For `page_ctl`, we need to define `PAGE_STORAGE_UTIL_DEBUGGGING`
add_executable(page_ctl EXCLUDE_FROM_ALL
        tests/page_storage_ctl.cpp
        ${page_storage_headers} ${page_storage_sources}
        ${io_base_headers} ${io_base_sources}
)
target_include_directories(page_ctl PUBLIC ${TIFLASH_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_ctl tiflash_common_io)
target_compile_options(page_ctl  PRIVATE -Wno-format)
target_compile_definitions(page_ctl PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING DBMS_PUBLIC_GTEST)
