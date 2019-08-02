add_headers_and_sources(page_storage ../)
add_headers_and_sources(page_storage ../mvcc)
add_headers_and_sources(page_storage ../VersionSet)
add_library(page_storage EXCLUDE_FROM_ALL
        ${page_storage_headers} ${page_storage_sources})
target_link_libraries(page_storage clickhouse_common_io)

# glob all unit tests of dm into unit_tests_page_storage
macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

# attach all dm gtest sources
grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/tests ps_gtest_sources)

add_executable(unit_tests_page_storage EXCLUDE_FROM_ALL ${ps_gtest_sources})
target_link_libraries(unit_tests_page_storage gtest_main page_storage)
target_compile_options(unit_tests_page_storage PRIVATE -Wno-unknown-pragmas)
add_check(unit_tests_page_storage)

# non googletest
add_executable(page_stress_page_storage stress_page_stroage.cpp)
target_link_libraries(page_stress_page_storage page_storage)
target_compile_options(page_stress_page_storage PRIVATE -Wno-format) # turn off printf format check

add_executable(page_utils_get_valid_pages utils_get_valid_pages.cpp)
target_link_libraries(page_utils_get_valid_pages page_storage)
target_compile_options(page_utils_get_valid_pages PRIVATE -Wno-format)

add_executable(test_page_storage_write_disk_full test_page_storage_write_disk_full.cpp)
target_link_libraries(test_page_storage_write_disk_full dbms)
target_compile_options(test_page_storage_write_disk_full PRIVATE -Wno-format)

add_executable(mem_usage_test mem_usage_test.cpp)
