set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
set(_TIFLASH_PROXY_LIBRARY "${_TIFLASH_PROXY_SOURCE_DIR}/target/release/${CMAKE_SHARED_LIBRARY_PREFIX}tiflash_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}")

add_custom_command(OUTPUT ${_TIFLASH_PROXY_LIBRARY}
    COMMENT "Build tiflash proxy"
    COMMAND make release
    WORKING_DIRECTORY ${_TIFLASH_PROXY_SOURCE_DIR}
    DEPENDS "${_TIFLASH_PROXY_SOURCE_DIR}/raftstore-proxy/ffi/src/RaftStoreProxyFFI/@version")

add_custom_target(tiflash_proxy_build ALL
    DEPENDS ${_TIFLASH_PROXY_LIBRARY})

add_library(tiflash_proxy SHARED IMPORTED GLOBAL)
set_target_properties(tiflash_proxy PROPERTIES IMPORTED_LOCATION ${_TIFLASH_PROXY_LIBRARY})
add_dependencies(tiflash_proxy tiflash_proxy_build)
