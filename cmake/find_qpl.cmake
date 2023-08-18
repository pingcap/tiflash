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

option (USE_INTERNAL_QPL_LIBRARY "Set to FALSE to use system qpl library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_QPL_LIBRARY)
    if (EXISTS "${TiFlash_SOURCE_DIR}/contrib/qpl/include/qpl/qpl.h")
        set (QPL_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/qpl/)
        set (QPL_LIBRARY qpl)
    else ()
        message (FATAL_ERROR "Submodule contrib/qpl is missing. Try run: \n git submodule update --init --recursive \n to fix it or set -DUSE_INTERNAL_QPL_LIBRARY=OFF to use system qpl library.")
    endif ()
else ()
    find_library (QPL_LIBRARY qpl)
    find_path (QPL_INCLUDE_DIR NAMES qpl.h PATHS ${QPL_INCLUDE_PATHS} PATH_SUFFIXES qpl)
    if (QPL_LIBRARY AND QPL_INCLUDE_DIR)
    else ()
        message (FATAL_ERROR "System qpl library not found. Try to install system qpl library or set -DUSE_INTERNAL_QPL_LIBRARY=ON to use bundled qpl library.")
    endif ()
endif ()

if (QPL_LIBRARY AND QPL_INCLUDE_DIR)
    SET (USE_QPL 1)
endif ()
    
message (STATUS "Using QPL: ${QPL_INCLUDE_DIR} : ${QPL_LIBRARY}")
