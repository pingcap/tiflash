
# glob all unit tests of PageStorage into gtests_page_storage
macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

# attach all dm gtest sources
grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms/src/Storages/Page/V3/tests ps_v3_gtest_sources)

add_executable(gtests_page_storage_v3 ${ps_v3_gtest_sources} ${ClickHouse_SOURCE_DIR}/dbms/src/TestUtils/gtests_dbms_main.cpp)
target_link_libraries(gtests_page_storage_v3 gtest_main page_storage_v3)
target_compile_options(gtests_page_storage_v3 PRIVATE -Wno-unknown-pragmas)
target_compile_definitions(gtests_page_storage_v3 PRIVATE DBMS_PUBLIC_GTEST)
add_check(gtests_page_storage_v3)

# non-google test
# add_headers_and_sources(page_storage_v3_stress ./test)
# add_executable(page_storage_v3_stress EXCLUDE_FROM_ALL ${page_storage_v3_stress_sources})
# target_link_libraries(page_storage_v3_stress page_storage_v3)
# target_compile_options(page_storage_v3_stress PRIVATE -Wno-format -lc++) # turn off printf format check
