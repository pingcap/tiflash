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

set(_PROCESS_METRICS_SOURCE_DIR "${TiFlash_SOURCE_DIR}/libs/libprocess_metrics")
set(_PROCESS_METRICS_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}process_metrics${CMAKE_STATIC_LIBRARY_SUFFIX}")
file(GLOB_RECURSE _PROCESS_METRICS_SRCS "${_PROCESS_METRICS_SOURCE_DIR}/src/*.rs")
file(GLOB_RECURSE _PROCESS_METRICS_HEADERS "${_PROCESS_METRICS_SOURCE_DIR}/include/*.h")
add_custom_command(OUTPUT ${_PROCESS_METRICS_LIBRARY}
        COMMENT "Building process_metrics"
        COMMAND cargo build --release --target-dir ${CMAKE_CURRENT_BINARY_DIR}
        VERBATIM
        USES_TERMINAL
        WORKING_DIRECTORY ${_PROCESS_METRICS_SOURCE_DIR}
        DEPENDS "${_PROCESS_METRICS_SRCS}"
        "${_PROCESS_METRICS_HEADERS}"
                "${_PROCESS_METRICS_SOURCE_DIR}/Cargo.lock"
                "${_PROCESS_METRICS_SOURCE_DIR}/Cargo.toml"
                "${TiFlash_SOURCE_DIR}/rust-toolchain")

add_custom_target(_process_metrics ALL DEPENDS ${_PROCESS_METRICS_LIBRARY} JOB_POOL rust_job_pool)
add_library(process_metrics STATIC IMPORTED GLOBAL)
set_target_properties(process_metrics PROPERTIES IMPORTED_LOCATION ${_PROCESS_METRICS_LIBRARY})
add_dependencies(process_metrics _process_metrics)
target_include_directories(process_metrics INTERFACE ${_PROCESS_METRICS_SOURCE_DIR}/include)
