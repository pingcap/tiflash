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

option (USE_INTERNAL_LIBCLARA "Set to FALSE to use a pre-built libclara (Only used in CI. Set to FALSE on your own risk)" 1)

if (NOT USE_INTERNAL_LIBCLARA)
    if (NOT EXISTS "${LIBCLARA_CXXBRIDGE_DIR}/clara_fts/src/index_reader.rs.cc")
        message (FATAL_ERROR "Cannot find clara_fts/src/index_reader.rs.cc in LIBCLARA_CXXBRIDGE_DIR (${LIBCLARA_CXXBRIDGE_DIR})")
    endif ()
    if (NOT EXISTS "${LIBCLARA_LIBRARY}")
        message (FATAL_ERROR "Cannot find libclara specified by LIBCLARA_LIBRARY (${LIBCLARA_LIBRARY})")
    endif ()

    message(STATUS "Using pre-built libclara: ${LIBCLARA_CXXBRIDGE_DIR}, ${LIBCLARA_LIBRARY}")

    add_library(clara_shared SHARED IMPORTED)
    set_target_properties(clara_shared PROPERTIES
        IMPORTED_LOCATION "${LIBCLARA_LIBRARY}")
    set_target_properties(clara_shared PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${LIBCLARA_CXXBRIDGE_DIR}")
endif()
