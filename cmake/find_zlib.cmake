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

option (USE_INTERNAL_ZLIB_LIBRARY "Set to FALSE to use system zlib library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_ZLIB_LIBRARY)
    find_package (ZLIB)
endif ()

if (NOT ZLIB_FOUND)
    if (NOT MSVC)
        set (INTERNAL_ZLIB_NAME "zlib-ng")
    else ()
        set (INTERNAL_ZLIB_NAME "zlib")
        if (NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
            message (WARNING "Will use standard zlib, please clone manually:\n git clone https://github.com/madler/zlib.git ${TiFlash_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
        endif ()
    endif ()

    set (USE_INTERNAL_ZLIB_LIBRARY 1)
    set (ZLIB_COMPAT 1)
    set (ZLIB_ENABLE_TESTS 0)
    set (WITH_NATIVE_INSTRUCTIONS ${ARCHNATIVE})

    set (ZLIB_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}" "${TiFlash_BINARY_DIR}/contrib/${INTERNAL_ZLIB_NAME}") # generated zconf.h
    set (ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR}) # for poco
    set (ZLIB_FOUND 1) # for poco
    if (USE_STATIC_LIBRARIES)
        set (ZLIB_LIBRARIES zlibstatic)
    else ()
        set (ZLIB_LIBRARIES zlib)
    endif ()
endif ()

message (STATUS "Using zlib: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
