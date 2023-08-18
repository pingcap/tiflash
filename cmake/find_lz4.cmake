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

option (USE_INTERNAL_LZ4_LIBRARY "Set to FALSE to use system lz4 library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_LZ4_LIBRARY AND NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/lz4/lib/lz4.h")
   message (WARNING "submodule contrib/lz4 is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_LZ4_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    find_library (LZ4_LIBRARY lz4)
    find_path (LZ4_INCLUDE_DIR NAMES lz4.h PATHS ${LZ4_INCLUDE_PATHS})
endif ()

if (LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
else ()
    set (LZ4_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/lz4/lib)
    set (USE_INTERNAL_LZ4_LIBRARY 1)
    set (LZ4_LIBRARY lz4)
endif ()

message (STATUS "Using lz4: ${LZ4_INCLUDE_DIR} : ${LZ4_LIBRARY}")
