
set(page_converter_source PageConverter.cpp 
    PageConverter.h 
    PageConverterOptions.h)

# Used to convert V2 to V3
add_executable(page_converter EXCLUDE_FROM_ALL 
    page_converter_offline.cpp 
    ${page_converter_source})
target_link_libraries(page_converter dbms page_storage_v2 page_storage_v3)
target_include_directories(page_converter PRIVATE .)
target_compile_options(page_converter PRIVATE -Wno-format -lc++)

# Used to restore the backup from `page_converter`
add_executable(page_converter_restore EXCLUDE_FROM_ALL 
    page_converter_restore.cpp 
    ${page_converter_source})
target_link_libraries(page_converter_restore dbms page_storage_v2 page_storage_v3)
target_include_directories(page_converter_restore PRIVATE .)
target_compile_options(page_converter_restore PRIVATE -Wno-format -lc++)