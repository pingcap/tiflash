add_headers_and_sources(page_storage ./)
add_headers_and_sources(page_storage ./mvcc)
add_headers_and_sources(page_storage ./VersionSet)
add_headers_and_sources(page_storage ./gc)
add_headers_and_sources(page_storage ${ClickHouse_SOURCE_DIR}/dbms/src/Encryption)
add_library(page_storage EXCLUDE_FROM_ALL
        ${page_storage_headers} ${page_storage_sources}
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathCapacityMetrics.cpp
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.h
        ${ClickHouse_SOURCE_DIR}/dbms/src/Storages/PathPool.cpp
        )
target_include_directories(page_storage PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src)
target_link_libraries(page_storage clickhouse_common_io kv_client tipb)
target_compile_definitions(page_storage PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING)

add_executable(page_ctl EXCLUDE_FROM_ALL tests/page_storage_ctl.cpp)
target_link_libraries(page_ctl page_storage)
target_compile_options(page_ctl  PRIVATE -Wno-format)
target_compile_definitions(page_ctl PRIVATE PAGE_STORAGE_UTIL_DEBUGGGING)
