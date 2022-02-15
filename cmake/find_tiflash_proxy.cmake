option(USE_INTERNAL_TIFLASH_PROXY "Set to FALSE to use external tiflash proxy instead of bundled. (Experimental. Set to FALSE on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/Makefile")
    if(USE_INTERNAL_TIFLASH_PROXY)
        message(WARNING "submodule contrib/tiflash-proxy is missing. to fix try run: \n git submodule update --init")
        message(WARNING "Can't use internal tiflash proxy")
        set(USE_INTERNAL_TIFLASH_PROXY 0)
    endif()
    set(MISSING_INTERNAL_TIFLASH_PROXY 1)
endif()

if(NOT USE_INTERNAL_TIFLASH_PROXY)
    find_path(TIFLASH_PROXY_INCLUDE_DIR NAMES RaftStoreProxyFFI/ProxyFFI.h PATH_SUFFIXES raftstore-proxy/ffi/src)
    find_library(TIFLASH_PROXY_LIBRARY NAMES tiflash_proxy PATH_SUFFIXES PATH_SUFFIXES target/release)
    if(NOT TIFLASH_PROXY_INCLUDE_DIR)
        message(WARNING "Can't find external tiflash proxy include dir")
        set(EXTERNAL_TIFLASH_PROXY_FOUND 0)
    elseif(NOT TIFLASH_PROXY_LIBRARY)
        message(WARNING "Can't find external tiflash proxy library")
        set(EXTERNAL_TIFLASH_PROXY_FOUND 0)
    else()
        set(EXTERNAL_TIFLASH_PROXY_FOUND 1)
    endif()
endif()

if(NOT EXTERNAL_TIFLASH_PROXY_FOUND)
    if(NOT MISSING_INTERNAL_TIFLASH_PROXY)
        set(TIFLASH_PROXY_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src")
        set(TIFLASH_PROXY_LIBRARY libtiflash_proxy)
        set(USE_INTERNAL_TIFLASH_PROXY 1)
    else()
        message(FATAL_ERROR "Can't find tiflash proxy")
    endif()
endif()

if (APPLE AND USE_INTERNAL_TIFLASH_PROXY)
    execute_process(
        COMMAND brew --prefix openssl@1.1
        RESULT_VARIABLE BREW_OPENSSL
        OUTPUT_VARIABLE BREW_OPENSSL_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if (BREW_OPENSSL EQUAL 0 AND EXISTS "${BREW_OPENSSL_PREFIX}")
        message(STATUS "Found openssl installed by Homebrew at ${BREW_OPENSSL_PREFIX}")
    else()
        if(NOT DEFINED ENV{OPENSSL_ROOT_DIR})
            message(FATAL_ERROR "Not found openssl installed by Homebrew or env `OPENSSL_ROOT_DIR`, please install openssl by `brew install openssl@1.1`")
        else()
            message(STATUS "Found openssl by `OPENSSL_ROOT_DIR` at $ENV{OPENSSL_ROOT_DIR}")
        endif()
    endif()
endif ()

set(TIFLASH_PROXY_FOUND TRUE)

message(STATUS "Using tiflash proxy: ${USE_INTERNAL_TIFLASH_PROXY} : ${TIFLASH_PROXY_INCLUDE_DIR}, ${TIFLASH_PROXY_LIBRARY}")
