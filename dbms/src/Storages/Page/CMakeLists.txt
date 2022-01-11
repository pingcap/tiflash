add_subdirectory(V2)

# PageStorage stress test
add_headers_and_sources(page_stress_testing stress)
add_headers_and_sources(page_stress_testing stress/workload)
add_executable(page_stress_testing EXCLUDE_FROM_ALL ${page_stress_testing_sources})
target_link_libraries(page_stress_testing dbms page_storage_v3)
target_include_directories(page_stress_testing PRIVATE stress)
target_compile_options(page_stress_testing PRIVATE -Wno-format -lc++) # turn off printf format check
