# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG" OR SAN_DEBUG)
    set(_TIFLASH_PROXY_BUILD_PROFILE "debug")
    if (ENABLE_JEMALLOC)
        if (APPLE)
            message(STATUS "proxy's jemalloc is disabled (AppleOS)")
            set(_TIFLASH_PROXY_MAKE_COMMAND make debug)
        else()
            message(STATUS "proxy's jemalloc is enabled")
            set(_TIFLASH_PROXY_MAKE_COMMAND ENABLE_FEATURES="external-jemalloc" make debug)
        endif()
    else()
        message(STATUS "proxy's jemalloc is disabled")
        set(_TIFLASH_PROXY_MAKE_COMMAND make debug)
    endif()
else()
    set(_TIFLASH_PROXY_BUILD_PROFILE "release")
    if (ENABLE_JEMALLOC)
        if (APPLE)
            message(STATUS "proxy's jemalloc is disabled (AppleOS)")
            set(_TIFLASH_PROXY_MAKE_COMMAND make release)
        else()
            message(STATUS "proxy's jemalloc is enabled")
            set(_TIFLASH_PROXY_MAKE_COMMAND ENABLE_FEATURES="external-jemalloc" make release)
        endif()
    else()
        message(STATUS "proxy's jemalloc is disabled")
        set(_TIFLASH_PROXY_MAKE_COMMAND make release)
    endif()
endif()

set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
set(_TIFLASH_PROXY_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/${_TIFLASH_PROXY_BUILD_PROFILE}/${CMAKE_SHARED_LIBRARY_PREFIX}tiflash_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _TIFLASH_PROXY_SRCS "${_TIFLASH_PROXY_SOURCE_DIR}/*.rs")

# Build in the build directory instead of the default source directory
set(TIFLASH_RUST_ENV "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}" ${TIFLASH_RUST_ENV})

# use `CFLAGS=-w CXXFLAGS=-w` to inhibit warning messages.
if (TIFLASH_LLVM_TOOLCHAIN)
    set(TIFLASH_RUST_ENV CMAKE=${CMAKE_COMMAND} "CFLAGS=-w -fuse-ld=lld" "CXXFLAGS=-w -fuse-ld=lld -stdlib=libc++" ${TIFLASH_RUST_ENV})
else()
    set(TIFLASH_RUST_ENV CMAKE=${CMAKE_COMMAND} CFLAGS=-w CXXFLAGS=-w ${TIFLASH_RUST_ENV})
endif()

set(TIFLASH_RUST_ENV PROTOC=${Protobuf_PROTOC_EXECUTABLE} PROTOC_INCLUDE=${Protobuf_INCLUDE_DIR} ${TIFLASH_RUST_ENV})

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
    set(TIFLASH_RUST_ENV CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} CXXSTDLIB=c++ RUSTFLAGS=${TIFLASH_RUSTFLAGS} ${TIFLASH_RUST_ENV})
    message(STATUS "Enforce LLVM toolchain for rust")
endif()

message(STATUS "Using rust env for tiflash-proxy: ${TIFLASH_RUST_ENV}")

add_custom_command(OUTPUT ${_TIFLASH_PROXY_LIBRARY}
    COMMENT "Building TiFlash Proxy using ${_TIFLASH_PROXY_BUILD_PROFILE} profile"
    COMMAND ${CMAKE_COMMAND} -E env ${TIFLASH_RUST_ENV} ${_TIFLASH_PROXY_MAKE_COMMAND}
    VERBATIM
    USES_TERMINAL
    WORKING_DIRECTORY ${_TIFLASH_PROXY_SOURCE_DIR}
    DEPENDS "${_TIFLASH_PROXY_SRCS}"
            "${_TIFLASH_PROXY_SOURCE_DIR}/Cargo.lock"
            "${TiFlash_SOURCE_DIR}/rust-toolchain")
            find_program(OBJCOPY_EXECUTABLE "objcopy")

add_custom_target(tiflash_proxy ALL DEPENDS ${_TIFLASH_PROXY_LIBRARY})
add_library(libtiflash_proxy SHARED IMPORTED GLOBAL)
set_target_properties(libtiflash_proxy PROPERTIES IMPORTED_LOCATION ${_TIFLASH_PROXY_LIBRARY} IMPORTED_NO_SONAME ON)
add_dependencies(libtiflash_proxy tiflash_proxy)
