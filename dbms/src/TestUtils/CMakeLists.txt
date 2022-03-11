include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(test_util .)
list(REMOVE_ITEM ${test_util_sources} bench_dbms_main.cpp gtests_dbms_main.cpp)

add_library(test_util_gtest_main ${test_util_headers} ${test_util_sources} gtests_dbms_main.cpp)
target_link_libraries(test_util_gtest_main dbms gtest_main)
add_library(test_util_bench_main ${test_util_headers} ${test_util_sources} bench_dbms_main.cpp)
target_link_libraries(test_util_bench_main dbms gtest_main)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
