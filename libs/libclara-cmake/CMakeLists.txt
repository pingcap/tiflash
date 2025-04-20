# Copyright 2025 PingCAP, Inc.
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

set(_CLARA_SOURCE_DIR "${TiFlash_SOURCE_DIR}/libs/libclara")
set(_CLARA_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge")
set(_CLARA_INCLUDE_DIR "${_CLARA_GEN_DIR}")

set(_CLARA_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}clara${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(_CLARA_GEN_SRC_FILES
        # From `libclara/clare_fts/build.rs`.
        "${_CLARA_GEN_DIR}/clara_fts/src/tokenizer/mod.rs.cc"
        "${_CLARA_GEN_DIR}/clara_fts/src/index_reader.rs.cc"
        "${_CLARA_GEN_DIR}/clara_fts/src/index_writer.rs.cc"
        "${_CLARA_GEN_DIR}/clara_fts/src/brute_searcher.rs.cc"
)

file(MAKE_DIRECTORY ${_CLARA_INCLUDE_DIR})
file(GLOB_RECURSE _CLARA_RUST_SRCS "${_CLARA_SOURCE_DIR}/*.rs")

if (TIFLASH_LLVM_TOOLCHAIN)
    set(TIFLASH_RUST_ENV CMAKE=${CMAKE_COMMAND} "CFLAGS=-fuse-ld=lld" "CXXFLAGS=-fuse-ld=lld -stdlib=libc++" ${TIFLASH_RUST_ENV})
else()
    set(TIFLASH_RUST_ENV CMAKE=${CMAKE_COMMAND} ${TIFLASH_RUST_ENV})
endif()

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
    message(STATUS "Enforce LLVM toolchain for libclara")
endif()

message(STATUS "Using rust env for libclara: ${TIFLASH_RUST_ENV}")

# This will build all workspace crates.
add_custom_command(OUTPUT ${_CLARA_LIBRARY} ${_CLARA_GEN_SRC_FILES}
        COMMENT "Building libclara"
        COMMAND ${CMAKE_COMMAND} -E env ${TIFLASH_RUST_ENV} cargo build --release --target-dir ${CMAKE_CURRENT_BINARY_DIR}
        VERBATIM
        USES_TERMINAL
        WORKING_DIRECTORY ${_CLARA_SOURCE_DIR}
        DEPENDS "${_CLARA_RUST_SRCS}"
                "${_CLARA_SOURCE_DIR}/Cargo.lock"
                "${_CLARA_SOURCE_DIR}/Cargo.toml"
                "${_CLARA_SOURCE_DIR}/rust-toolchain.toml")

add_library(clara ${_CLARA_GEN_SRC_FILES})
target_compile_options(clara PRIVATE -fPIC)
target_link_libraries(clara PUBLIC ${_CLARA_LIBRARY})
target_include_directories(clara PUBLIC ${_CLARA_INCLUDE_DIR})

# Rust cannot generate a dynamic library with correct visibility, so we generate a static library first and then link it to a shared library.
# The reason we use dynamic library is to avoid symbol conflicts.

add_library(clara_shared SHARED "${TiFlash_SOURCE_DIR}/libs/libclara-cmake/dummy.cpp")
target_link_libraries(clara_shared PUBLIC "$<LINK_LIBRARY:WHOLE_ARCHIVE,clara>")
target_include_directories(clara_shared PUBLIC ${_CLARA_INCLUDE_DIR})
