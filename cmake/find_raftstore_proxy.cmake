set (TIFLASH_PROXY_LIB_PATH "${TiFlash_SOURCE_DIR}/libs/libtiflash-proxy")
set (TIFLASH_PROXY_MODULE_PATH "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
set (TIFLASH_PROXY_FFI_SRC "${TIFLASH_PROXY_MODULE_PATH}/raftstore-proxy/ffi/src")

set (TIFLASH_PROXY_LIBRARY "TIFLASH_PROXY_LIBRARY-NOTFOUND")
find_library (TIFLASH_PROXY_LIBRARY NAMES tiflash_proxy HINTS ${TIFLASH_PROXY_LIB_PATH})

if (NOT EXISTS "${TIFLASH_PROXY_FFI_SRC}")
    message (FATAL_ERROR "${TIFLASH_PROXY_FFI_SRC} is not found, please update RaftStore Proxy")
endif ()

if (TIFLASH_PROXY_LIBRARY STREQUAL "TIFLASH_PROXY_LIBRARY-NOTFOUND")
    message (FATAL_ERROR "RaftStore Proxy is not found in ${TIFLASH_PROXY_LIB_PATH}, try to run ` mkdir -p ${TIFLASH_PROXY_LIB_PATH} && cd ${TIFLASH_PROXY_MODULE_PATH} && make && ln -s ${TIFLASH_PROXY_MODULE_PATH}/target/release/libtiflash_proxy.* ${TIFLASH_PROXY_LIB_PATH} `")
endif ()

message (STATUS "Using RaftStore Proxy: ${TIFLASH_PROXY_LIBRARY}, FFI src: ${TIFLASH_PROXY_FFI_SRC}")
