set(TICI_PROJECT_DIR "${CMAKE_SOURCE_DIR}/contrib/tici/components/searchlib")
set(TICI_LIB_NAME "tici_search_lib") 
set(TICI_LIB "${CMAKE_CURRENT_BINARY_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}${TICI_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

file(GLOB LIB_SOURCE_FILES "${TICI_PROJECT_DIR}/src/*")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge)

add_custom_command(
    OUTPUT ${TICI_LIB} 
    COMMAND cargo build -p tici-search-lib --release --target-dir ${CMAKE_CURRENT_BINARY_DIR} --manifest-path ${TICI_PROJECT_DIR}/Cargo.toml
    WORKING_DIRECTORY ${TICI_PROJECT_DIR}
    DEPENDS ${LIB_SOURCE_FILES}
    COMMENT "Build Rust lib"${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(rustbuild ALL DEPENDS ${TICI_LIB})
add_library(tici_search_lib_static STATIC IMPORTED GLOBAL)
set_target_properties(tici_search_lib_static PROPERTIES
    IMPORTED_LOCATION ${TICI_LIB}
)

add_dependencies(tici_search_lib_static rustbuild)
target_include_directories(tici_search_lib_static  INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge)

add_library(tici_search_lib SHARED "${TiFlash_SOURCE_DIR}/libs/libclara-cmake/dummy.cpp")
target_link_libraries(tici_search_lib PRIVATE "$<LINK_LIBRARY:WHOLE_ARCHIVE,tici_search_lib_static>")
target_include_directories(tici_search_lib  INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge)
# On Ubuntu/Debian:
# sudo apt-get install liblzma-dev libbz2-dev
# On CentOS/RHEL:
# sudo yum install xz-devel bzip2-devel
target_link_libraries(tici_search_lib PRIVATE lzma bz2)
