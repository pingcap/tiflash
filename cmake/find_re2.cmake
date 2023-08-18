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

option (USE_INTERNAL_RE2_LIBRARY "Set to FALSE to use system re2 library instead of bundled [slower]" ON)

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/re2/re2")
    if(USE_INTERNAL_RE2_LIBRARY)
        message(WARNING "submodule contrib/re2 is missing. to fix try run: \n git submodule update --init")
        message (WARNING "Can't find internal re2 library")
    endif()
    set(USE_INTERNAL_RE2_LIBRARY 0)
    set(MISSING_INTERNAL_RE2_LIBRARY 1)
endif()

if (NOT USE_INTERNAL_RE2_LIBRARY)
    find_library (RE2_LIBRARY re2)
    find_path (RE2_INCLUDE_DIR NAMES re2/re2.h PATHS ${RE2_INCLUDE_PATHS})
    if (NOT RE2_LIBRARY OR NOT RE2_INCLUDE_DIR)
        message (WARNING "Can't find system re2 library")
    endif ()
endif ()

string(FIND ${CMAKE_CURRENT_BINARY_DIR} " " _have_space)
if(_have_space GREATER 0)
    message(WARNING "Using spaces in build path [${CMAKE_CURRENT_BINARY_DIR}] highly not recommended. Library re2st will be disabled.")
    set (MISSING_INTERNAL_RE2_ST_LIBRARY 1)
endif()

if (RE2_LIBRARY AND RE2_INCLUDE_DIR)
    set (RE2_ST_LIBRARY ${RE2_LIBRARY})
elseif (NOT MISSING_INTERNAL_RE2_LIBRARY)
    set (USE_INTERNAL_RE2_LIBRARY 1)
    set (RE2_LIBRARY re2)
    set (RE2_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/re2")
    if (NOT MISSING_INTERNAL_RE2_ST_LIBRARY)
        set (RE2_ST_LIBRARY re2_st)
        set (USE_RE2_ST 1)
    else ()
        set (RE2_ST_LIBRARY ${RE2_LIBRARY})
        message (WARNING "Using internal re2 library instead of re2_st")
    endif ()
endif ()

message (STATUS "Using re2: ${RE2_INCLUDE_DIR} : ${RE2_LIBRARY}; ${RE2_ST_INCLUDE_DIR} : ${RE2_ST_LIBRARY}")
