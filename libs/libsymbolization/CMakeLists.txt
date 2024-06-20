# Copyright 2023 PingCAP, Inc.
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

# Debug-mode symbolization is slow. Let's always build a release. The building is fast anyway.
set(_SYMBOLIZATION_BUILD_PROFILE "release")
set(_SYMBOLIZATION_BUILD_ARGS --release)

set(_SYMBOLIZATION_SOURCE_DIR "${TiFlash_SOURCE_DIR}/libs/libsymbolization")
set(_SYMBOLIZATION_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/${_SYMBOLIZATION_BUILD_PROFILE}/${CMAKE_STATIC_LIBRARY_PREFIX}symbolization${CMAKE_STATIC_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _SYMBOLIZATION_SRCS "${_SYMBOLIZATION_SOURCE_DIR}/src/*.rs")
file(GLOB_RECURSE _SYMBOLIZATION_HEADERS "${_SYMBOLIZATION_SOURCE_DIR}/include/*.h")

add_custom_command(OUTPUT ${_SYMBOLIZATION_LIBRARY}
        COMMENT "Building symbolization using ${_SYMBOLIZATION_BUILD_PROFILE} profile"
        COMMAND cargo build ${_SYMBOLIZATION_BUILD_ARGS} --target-dir ${CMAKE_CURRENT_BINARY_DIR}
        VERBATIM
        USES_TERMINAL
        WORKING_DIRECTORY ${_SYMBOLIZATION_SOURCE_DIR}
        DEPENDS "${_SYMBOLIZATION_SRCS}"
                "${_SYMBOLIZATION_HEADERS}"
                "${_SYMBOLIZATION_SOURCE_DIR}/Cargo.lock"
                "${_SYMBOLIZATION_SOURCE_DIR}/Cargo.toml"
                "${TiFlash_SOURCE_DIR}/rust-toolchain.toml")

add_custom_target(symbolization ALL DEPENDS ${_SYMBOLIZATION_LIBRARY})
add_library(libsymbolization STATIC IMPORTED GLOBAL)
set_target_properties(libsymbolization PROPERTIES IMPORTED_LOCATION ${_SYMBOLIZATION_LIBRARY})
add_dependencies(libsymbolization symbolization)
target_include_directories(libsymbolization INTERFACE ${_SYMBOLIZATION_SOURCE_DIR}/include)
