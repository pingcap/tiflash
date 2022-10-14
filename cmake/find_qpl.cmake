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

option (USE_INTERNAL_QPL_LIBRARY "Set to FALSE to use system qpl library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_QPL_LIBRARY AND NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/qpl/include/qpl/qpl.h")
   message (WARNING "submodule contrib/qpl is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_QPL_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_QPL_LIBRARY)
    find_library (QPL_LIBRARY qpl)
    find_path (QPL_INCLUDE_DIR NAMES qpl.h PATHS ${QPL_INCLUDE_PATHS})
endif ()

if (QPL_LIBRARY AND QPL_INCLUDE_DIR)
else ()
    set (QPL_INCLUDE_DIR ${TiFlash_SOURCE_DIR}/contrib/qpl/include)
    set (USE_INTERNAL_QPL_LIBRARY 1)
    set (QPL_LIBRARY qpl)
endif ()

message (STATUS "Using qpl: ${QPL_INCLUDE_DIR} : ${QPL_LIBRARY}")
