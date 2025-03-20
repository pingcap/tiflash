set(TICI_PROJECT_DIR "${CMAKE_SOURCE_DIR}/contrib/tici")
set(TICI_LIB_NAME "tici_search_lib") 
set(TICI_LIB "${CMAKE_CURRENT_BINARY_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}${TICI_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

file(GLOB LIB_SOURCE_FILES "${TICI_PROJECT_DIR}/components/searchlib/src/*")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge)

add_custom_command(
    OUTPUT ${TICI_LIB} 
    COMMAND cargo build -p tici-search-lib --release --target-dir ${CMAKE_CURRENT_BINARY_DIR} --manifest-path ${TICI_PROJECT_DIR}/Cargo.toml
    WORKING_DIRECTORY ${TICI_PROJECT_DIR}
    DEPENDS ${LIB_SOURCE_FILES}
    COMMENT "Build Rust lib"${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(rustbuild ALL DEPENDS ${TICI_LIB})
add_library(tici_search_lib STATIC IMPORTED GLOBAL)
set_target_properties(tici_search_lib PROPERTIES
    IMPORTED_LOCATION ${TICI_LIB}
)

add_dependencies(tici_search_lib rustbuild)
target_include_directories(tici_search_lib  INTERFACE
    ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge)

