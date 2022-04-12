# Copyright 2022 PingCAP, Ltd.
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

option (USE_INTERNAL_ZSTD_LIBRARY "Set to FALSE to use system zstd library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_ZSTD_LIBRARY AND NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/zstd/lib/zstd.h")
   message (WARNING "submodule contrib/zstd is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_ZSTD_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    find_library (ZSTD_LIBRARY zstd)
    find_path (ZSTD_INCLUDE_DIR NAMES zstd.h PATHS ${ZSTD_INCLUDE_PATHS})
endif ()

if (ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_ZSTD_LIBRARY 1)
    set (ZSTD_LIBRARY zstd)
endif ()

message (STATUS "Using zstd: ${ZSTD_INCLUDE_DIR} : ${ZSTD_LIBRARY}")
