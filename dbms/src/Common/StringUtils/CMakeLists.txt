include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_stringutils .)

add_library(string_utils ${SPLIT_SHARED} ${clickhouse_common_stringutils_headers} ${clickhouse_common_stringutils_sources})
target_include_directories (string_utils PRIVATE ${DBMS_INCLUDE_DIR})
