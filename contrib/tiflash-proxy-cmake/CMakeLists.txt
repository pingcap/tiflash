set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
set(_TIFLASH_PROXY_LIBRARY "${_TIFLASH_PROXY_SOURCE_DIR}/target/release/${CMAKE_SHARED_LIBRARY_PREFIX}tiflash_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _TIFLASH_PROXY_SRCS "${_TIFLASH_PROXY_SOURCE_DIR}/*.rs")

add_custom_command(OUTPUT ${_TIFLASH_PROXY_LIBRARY}
    COMMENT "Building tiflash proxy"
    COMMAND make release
    VERBATIM
    WORKING_DIRECTORY ${_TIFLASH_PROXY_SOURCE_DIR}
    DEPENDS "${_TIFLASH_PROXY_SRCS}")

add_custom_target(tiflash_proxy ALL
    DEPENDS ${_TIFLASH_PROXY_LIBRARY})

add_library(libtiflash_proxy SHARED IMPORTED GLOBAL)
set_target_properties(libtiflash_proxy PROPERTIES IMPORTED_LOCATION ${_TIFLASH_PROXY_LIBRARY})
add_dependencies(libtiflash_proxy tiflash_proxy)
