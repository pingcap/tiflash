include(${TIFLASH_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(tiflash_table_functions .)

list(REMOVE_ITEM tiflash_table_functions_sources ITableFunction.cpp TableFunctionFactory.cpp)
list(REMOVE_ITEM tiflash_table_functions_headers ITableFunction.h TableFunctionFactory.h)

add_library(tiflash_table_functions ${tiflash_table_functions_sources})
target_link_libraries(tiflash_table_functions tiflash_storages_system dbms ${Poco_Foundation_LIBRARY})
