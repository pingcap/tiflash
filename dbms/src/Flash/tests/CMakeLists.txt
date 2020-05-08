include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (gtest_diagnostics gtest_diagnostics.cpp)
target_link_libraries (gtest_diagnostics gtest_main flash_service)

add_executable (gtest_log_search gtest_log_search.cpp)
target_link_libraries (gtest_log_search gtest_main flash_service)
