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

# Helper: extract openssl version from a Cargo.lock file.
# Cargo.lock stores each dependency as:
#   [[package]]
#   name = "openssl"
#   version = "0.10.72"
# This function scans name/version lines and returns the version
# that immediately follows the openssl name entry.
function(_extract_openssl_version CARGO_LOCK_PATH OUT_VAR)
    file(STRINGS "${CARGO_LOCK_PATH}" _LINES REGEX "^(name|version) = \"")
    set(_FOUND FALSE)
    foreach(_LINE IN LISTS _LINES)
        if(_LINE MATCHES "^name = \"openssl\"$")
            set(_FOUND TRUE)
        elseif(_FOUND AND _LINE MATCHES "^version = \"([0-9.]+)\"$")
            set(${OUT_VAR} "${CMAKE_MATCH_1}" PARENT_SCOPE)
            return()
        endif()
    endforeach()
    # openssl not found — leave OUT_VAR empty so caller can decide how to handle
    set(${OUT_VAR} "" PARENT_SCOPE)
endfunction()

set(_TIFLASH_PROXY_ENABLE_FEATURES "")
if (ENABLE_JEMALLOC)
    if (APPLE)
        message(STATUS "proxy's jemalloc is disabled (AppleOS)")
    else()
        message(STATUS "proxy's jemalloc is enabled")
        if (ENABLE_NEXT_GEN_COLUMNAR)
            set(_TIFLASH_PROXY_ENABLE_FEATURES ENABLE_FEATURES="external-jemalloc")
        elseif (ENABLE_NEXT_GEN)
            set(_TIFLASH_PROXY_ENABLE_FEATURES ENABLE_FEATURES="raftstore-proxy/external-jemalloc")
        else()
            set(_TIFLASH_PROXY_ENABLE_FEATURES ENABLE_FEATURES="external-jemalloc")
        endif()
    endif()
else()
    message(STATUS "proxy's jemalloc is disabled")
endif()
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG" OR SAN_DEBUG)
    set(_TIFLASH_PROXY_BUILD_PROFILE "debug")
else()
    set(_TIFLASH_PROXY_BUILD_PROFILE "release")
endif()
set(_TIFLASH_PROXY_MAKE_COMMAND "${_TIFLASH_PROXY_ENABLE_FEATURES}" make "${_TIFLASH_PROXY_BUILD_PROFILE}")

# use different tiflash-proxy for classic, next-gen, or columnar
if (ENABLE_NEXT_GEN_COLUMNAR)
    set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-columnar-hub")
elseif (ENABLE_NEXT_GEN)
    set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy-next-gen")
else()
    set(_TIFLASH_PROXY_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy")
endif()

# When ENABLE_NEXT_GEN_COLUMNAR is on, tiflash-columnar-hub and
# cloud-storage-engine must use the same openssl crate version.
# Mismatched openssl versions may cause features such as CMEK
# be broken.
if (ENABLE_NEXT_GEN_COLUMNAR)
    set(_COLUMNAR_HUB_CARGO_LOCK "${TiFlash_SOURCE_DIR}/contrib/tiflash-columnar-hub/Cargo.lock")
    set(_CLOUD_STORAGE_CARGO_LOCK "${TiFlash_SOURCE_DIR}/contrib/cloud-storage-engine/Cargo.lock")

    if (EXISTS "${_COLUMNAR_HUB_CARGO_LOCK}" AND EXISTS "${_CLOUD_STORAGE_CARGO_LOCK}")
        _extract_openssl_version("${_COLUMNAR_HUB_CARGO_LOCK}" _HUB_OPENSSL_VERSION)
        _extract_openssl_version("${_CLOUD_STORAGE_CARGO_LOCK}" _CS_OPENSSL_VERSION)

        if(NOT _HUB_OPENSSL_VERSION)
            message(FATAL_ERROR
                "Could not find openssl version in ${_COLUMNAR_HUB_CARGO_LOCK}")
        endif()
        if(NOT _CS_OPENSSL_VERSION)
            message(FATAL_ERROR
                "Could not find openssl version in ${_CLOUD_STORAGE_CARGO_LOCK}")
        endif()

        if(NOT _HUB_OPENSSL_VERSION STREQUAL _CS_OPENSSL_VERSION)
            message(FATAL_ERROR
                "openssl version mismatch: "
                "tiflash-columnar-hub uses ${_HUB_OPENSSL_VERSION}, "
                "cloud-storage-engine uses ${_CS_OPENSSL_VERSION}. "
                "Both must use the same openssl crate version when ENABLE_NEXT_GEN_COLUMNAR=ON, "
                "otherwise features such as CMEK may be broken.")
        else()
            message(STATUS
                "openssl version check passed: both use ${_HUB_OPENSSL_VERSION} "
                "(tiflash-columnar-hub == cloud-storage-engine)")
        endif()
    else()
        if(NOT EXISTS "${_COLUMNAR_HUB_CARGO_LOCK}")
            message(WARNING "${_COLUMNAR_HUB_CARGO_LOCK} not found, skipping openssl version check")
        endif()
        if(NOT EXISTS "${_CLOUD_STORAGE_CARGO_LOCK}")
            message(WARNING "${_CLOUD_STORAGE_CARGO_LOCK} not found, skipping openssl version check")
        endif()
    endif()
endif()

message(STATUS "Using TiFlash Rust FFI source dir: ${_TIFLASH_PROXY_SOURCE_DIR}, make command: ${_TIFLASH_PROXY_MAKE_COMMAND}")

set(_TIFLASH_PROXY_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/${_TIFLASH_PROXY_BUILD_PROFILE}/${CMAKE_SHARED_LIBRARY_PREFIX}tiflash_proxy${CMAKE_SHARED_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _TIFLASH_PROXY_SRCS "${_TIFLASH_PROXY_SOURCE_DIR}/*.rs")
list(FILTER _TIFLASH_PROXY_SRCS EXCLUDE REGEX "/target/")
set(_TIFLASH_PROXY_MAKEFILE "${_TIFLASH_PROXY_SOURCE_DIR}/Makefile")
set(_TIFLASH_PROXY_CARGO_MANIFEST "${_TIFLASH_PROXY_SOURCE_DIR}/Cargo.toml")
set(_TIFLASH_PROXY_CARGO_LOCK "${_TIFLASH_PROXY_SOURCE_DIR}/Cargo.lock")
set(_TIFLASH_PROXY_CARGO_CONFIG "${_TIFLASH_PROXY_SOURCE_DIR}/.cargo/config.toml")
set(_TIFLASH_PROXY_RUST_TOOLCHAIN "${TiFlash_SOURCE_DIR}/rust-toolchain.toml")
if (EXISTS "${_TIFLASH_PROXY_SOURCE_DIR}/rust-toolchain.toml")
    set(_TIFLASH_PROXY_RUST_TOOLCHAIN "${_TIFLASH_PROXY_SOURCE_DIR}/rust-toolchain.toml")
elseif (EXISTS "${_TIFLASH_PROXY_SOURCE_DIR}/rust-toolchain")
    set(_TIFLASH_PROXY_RUST_TOOLCHAIN "${_TIFLASH_PROXY_SOURCE_DIR}/rust-toolchain")
endif()

# Build in the build directory instead of the default source directory
set(TIFLASH_RUST_ENV "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}" ${TIFLASH_RUST_ENV})

if (ENABLE_NEXT_GEN_COLUMNAR)
    set(TIFLASH_RUST_ENV "PROMETHEUS_METRIC_NAME_PREFIX=tiflash_proxy_" ${TIFLASH_RUST_ENV})
endif()

# Set CMAKE_POLICY_VERSION_MINIMUM to support CMake 4.0+ with older crate CMakeLists.txt files.
set(TIFLASH_RUST_ENV "CMAKE_POLICY_VERSION_MINIMUM=3.5" ${TIFLASH_RUST_ENV})

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

set(_TIFLASH_PROXY_CUSTOM_DEPENDS
    "${_TIFLASH_PROXY_SRCS}"
    "${_TIFLASH_PROXY_MAKEFILE}"
    "${_TIFLASH_PROXY_CARGO_MANIFEST}"
    "${_TIFLASH_PROXY_RUST_TOOLCHAIN}")
if (EXISTS "${_TIFLASH_PROXY_CARGO_LOCK}")
    list(APPEND _TIFLASH_PROXY_CUSTOM_DEPENDS "${_TIFLASH_PROXY_CARGO_LOCK}")
endif()
if (EXISTS "${_TIFLASH_PROXY_CARGO_CONFIG}")
    list(APPEND _TIFLASH_PROXY_CUSTOM_DEPENDS "${_TIFLASH_PROXY_CARGO_CONFIG}")
endif()

add_custom_command(OUTPUT ${_TIFLASH_PROXY_LIBRARY}
    COMMENT "Building TiFlash Proxy using ${_TIFLASH_PROXY_BUILD_PROFILE} profile"
    COMMAND ${CMAKE_COMMAND} -E env ${TIFLASH_RUST_ENV} ${_TIFLASH_PROXY_MAKE_COMMAND}
    VERBATIM
    USES_TERMINAL
    WORKING_DIRECTORY ${_TIFLASH_PROXY_SOURCE_DIR}
    DEPENDS ${_TIFLASH_PROXY_CUSTOM_DEPENDS})

add_custom_target(tiflash_proxy ALL DEPENDS ${_TIFLASH_PROXY_LIBRARY})
add_library(libtiflash_proxy SHARED IMPORTED GLOBAL)
set_target_properties(libtiflash_proxy PROPERTIES IMPORTED_LOCATION ${_TIFLASH_PROXY_LIBRARY} IMPORTED_NO_SONAME ON)
add_dependencies(libtiflash_proxy tiflash_proxy)
