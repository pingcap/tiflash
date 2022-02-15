add_headers_and_sources(page_converter .)
add_executable(page_converter EXCLUDE_FROM_ALL ${page_converter_sources})
target_link_libraries(page_converter dbms page_storage_v2 page_storage_v3)
target_include_directories(page_converter PRIVATE .)
target_compile_options(page_converter PRIVATE -Wno-format -lc++) # turn off printf format check