include_directories (${CMAKE_CURRENT_BINARY_DIR})

# glob all unit tests of dm into unit_tests_dm
macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()
# attach all dm gtest sources
grep_gtest_sources(${TiFlash_SOURCE_DIR}/dbms/src/Storages/DeltaMerge/tests dm_gtest_sources)
add_executable(gtests_dm EXCLUDE_FROM_ALL ${dm_gtest_sources} dm_basic_include.h)
target_link_libraries(gtests_dm gtest_main dbms clickhouse_functions)
add_check(gtests_dm)

# add unit test manually
add_executable(dm_test_storage_delta_merge EXCLUDE_FROM_ALL gtest_dm_storage_delta_merge.cpp)
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

add_executable(dm_test_delta_merge_store EXCLUDE_FROM_ALL gtest_dm_delta_merge_store.cpp)
target_link_libraries(dm_test_delta_merge_store dbms gtest_main clickhouse_functions)

add_executable(dm_test_segment EXCLUDE_FROM_ALL gtest_dm_segment.cpp)
target_link_libraries(dm_test_segment dbms gtest_main clickhouse_functions)

add_executable(dm_test_delta_tree EXCLUDE_FROM_ALL gtest_dm_delta_tree.cpp)
target_link_libraries(dm_test_delta_tree dbms gtest_main)

add_executable(dm_test_dmfile EXCLUDE_FROM_ALL gtest_dm_file.cpp)
target_link_libraries(dm_test_dmfile dbms gtest_main clickhouse_functions)

add_executable(dm_test_region EXCLUDE_FROM_ALL gtest_dm_region.cpp)
target_link_libraries(dm_test_region dbms gtest_main clickhouse_functions)

add_executable(dm_test_minmax_index EXCLUDE_FROM_ALL gtest_dm_minmax_index.cpp)
target_link_libraries(dm_test_minmax_index dbms gtest_main clickhouse_functions)

add_executable(dm_test_delta_index_manager EXCLUDE_FROM_ALL gtest_dm_delta_index_manager.cpp)
target_link_libraries(dm_test_delta_index_manager dbms gtest_main clickhouse_functions)

add_subdirectory (bank EXCLUDE_FROM_ALL)
