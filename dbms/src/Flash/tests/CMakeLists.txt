include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (diagnostics_test diagnostics_test.cpp)
target_link_libraries (diagnostics_test gtest_main flash_service)