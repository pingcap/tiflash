include(${TiFlash_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(test_utils .)

add_library(test_utils ${test_utils_headers} ${test_utils_sources})
target_link_libraries(test_utils dbms gtest_main)
if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
