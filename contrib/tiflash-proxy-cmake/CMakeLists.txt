set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
set(_TIFLASH_PROXY_LIBRARY "${_TIFLASH_PROXY_SOURCE_DIR}/target/release/${CMAKE_SHARED_LIBRARY_PREFIX}tiflash_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _TIFLASH_PROXY_SRCS "${_TIFLASH_PROXY_SOURCE_DIR}/*.rs")
list(FILTER _TIFLASH_PROXY_SRCS EXCLUDE REGEX ${_TIFLASH_PROXY_SOURCE_DIR}/target/.*)

# use `CFLAGS=-w CXXFLAGS=-w` to inhibit warning messages.
set(TIFLASH_RUST_ENV CMAKE=${CMAKE_COMMAND} CFLAGS=-w CXXFLAGS=-w)

if(TIFLASH_LLVM_TOOLCHAIN AND USE_LIBCXX)
    set(TIFLASH_RUST_LINKER ${CMAKE_CURRENT_BINARY_DIR}/tiflash-linker)
    set(TIFLASH_RUST_LINKER_TMP ${CMAKE_CURRENT_BINARY_DIR}/tmp/tiflash-linker)
    file(WRITE ${TIFLASH_RUST_LINKER_TMP}
            "#!/usr/bin/env sh\n${CMAKE_CXX_COMPILER} -l:libc++.so -l:libc++abi.so $@")
    file(COPY ${TIFLASH_RUST_LINKER_TMP}
            DESTINATION ${CMAKE_CURRENT_BINARY_DIR}
            FILE_PERMISSIONS OWNER_WRITE OWNER_READ OWNER_EXECUTE)
    set(TIFLASH_RUSTFLAGS "-C rpath=yes -C linker=${TIFLASH_RUST_LINKER}")
    if(ARCH_AARCH64)
        set(TIFLASH_RUSTFLAGS "-C link-arg=-Wl,-Bsymbolic ${TIFLASH_RUSTFLAGS}")
    endif()
    if(LINKER_NAME)
        set(TIFLASH_RUSTFLAGS "-C link-arg=-fuse-ld=${LINKER_NAME} ${TIFLASH_RUSTFLAGS}")
    endif()
    set(TIFLASH_RUST_ENV CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CXXSTDLIB=c++ ${TIFLASH_RUST_ENV} RUSTFLAGS=${TIFLASH_RUSTFLAGS} PROTOC=${Protobuf_PROTOC_EXECUTABLE} PROTOC_INCLUDE=${Protobuf_INCLUDE_DIR})
    message(STATUS "Enforce LLVM toolchain for rust")
endif()

message(STATUS "Using rust env for tiflash-proxy: ${TIFLASH_RUST_ENV}")

add_custom_command(OUTPUT ${_TIFLASH_PROXY_LIBRARY}
    COMMENT "Building tiflash proxy"
    # `ENGINE_LABEL_VALUE` is used in proxy for copying `libraftstore_proxy.xx` to `lib${ENGINE_LABEL_VALUE}_proxy.xx`
    COMMAND ${CMAKE_COMMAND} -E env ${TIFLASH_RUST_ENV} ENGINE_LABEL_VALUE=tiflash make release
    VERBATIM
    WORKING_DIRECTORY ${_TIFLASH_PROXY_SOURCE_DIR}
    DEPENDS "${_TIFLASH_PROXY_SRCS}" "${_TIFLASH_PROXY_SOURCE_DIR}/Cargo.lock" "${_TIFLASH_PROXY_SOURCE_DIR}/rust-toolchain")

add_custom_target(tiflash_proxy ALL
    DEPENDS ${_TIFLASH_PROXY_LIBRARY})

add_library(libtiflash_proxy SHARED IMPORTED GLOBAL)
set_target_properties(libtiflash_proxy PROPERTIES IMPORTED_LOCATION ${_TIFLASH_PROXY_LIBRARY} IMPORTED_NO_SONAME ON)
add_dependencies(libtiflash_proxy tiflash_proxy)
