include_directories (${CMAKE_CURRENT_BINARY_DIR})

# glob all unit tests of dm into unit_tests_dm
macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()
# attach all dm gtest sources
grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms/src/Storages/DeltaMerge/tests dm_gtest_sources)
add_executable(unit_tests_dm ${dm_gtest_sources} dm_basic_include.h)
target_link_libraries(unit_tests_dm gtest_main dbms clickhouse_functions)
add_check(unit_tests_dm)

# add unit test by hand
add_executable(dm_test_storage_delta_merge gtest_dm_storage_delta_merge.cpp)
target_include_directories(dm_test_storage_delta_merge BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(dm_test_storage_delta_merge
        dbms
        gtest_main
        cityhash
        ${Poco_Net_LIBRARY}
        ${Poco_Data_LIBRARY}
        common
        clickhouse_common_io
        clickhouse_functions
        )

add_executable(dm_test_delta_merge_store gtest_dm_delta_merge_store.cpp)
target_link_libraries(dm_test_delta_merge_store dbms gtest_main clickhouse_functions)

add_executable(dm_test_segment gtest_dm_segment.cpp)
target_link_libraries(dm_test_segment dbms gtest_main clickhouse_functions)

add_executable(dm_test_disk_value_space gtest_dm_disk_value_space.cpp)
target_link_libraries(dm_test_disk_value_space dbms gtest_main clickhouse_functions)

add_executable(dm_test_merge_block_stream gtest_dm_delta_merge_block_stream.cpp)
target_link_libraries(dm_test_merge_block_stream dbms gtest_main clickhouse_functions)

add_executable(dm_test_delta_tree gtest_dm_delta_tree.cpp)
target_link_libraries(dm_test_delta_tree dbms gtest_main)

add_executable(dm_test_chunk gtest_dm_chunk.cpp)
target_link_libraries(dm_test_chunk dbms gtest_main)
