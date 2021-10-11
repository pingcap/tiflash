
add_headers_and_sources(new_page_stress_testing ./test)
add_executable(new_page_stress_testing EXCLUDE_FROM_ALL ${new_page_stress_testing_sources})
target_link_libraries(new_page_stress_testing new_page_storage)
target_compile_options(new_page_stress_testing PRIVATE -Wno-format -lc++) # turn off printf format check
