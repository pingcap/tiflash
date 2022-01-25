include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (region_test region.cpp)
target_link_libraries (region_test dbms)

# glob all unit tests of tmt into gtests_tmt
macro (grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file (GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro ()

# attach all tmt gtest sources
grep_gtest_sources (${TiFlash_SOURCE_DIR}/dbms/src/Storages/Transaction/tests gtests_tmt_sources)

add_executable (gtests_tmt EXCLUDE_FROM_ALL ${gtests_tmt_sources})
target_include_directories (gtests_tmt BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
target_link_libraries (gtests_tmt gtest_main dbms clickhouse_functions)
target_compile_options (gtests_tmt PRIVATE -Wno-unknown-pragmas)
add_check (gtests_tmt)
