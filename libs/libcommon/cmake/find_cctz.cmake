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

option (USE_INTERNAL_CCTZ_LIBRARY "Set to FALSE to use system cctz library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_CCTZ_LIBRARY AND NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/cctz/include/cctz/time_zone.h")
   message (WARNING "submodule contrib/cctz is missing. to fix try run: \n git submodule update --init --recursive")
   set (MISSING_INTERNAL_CCTZ_LIBRARY 1)
   set (USE_INTERNAL_CCTZ_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_CCTZ_LIBRARY)
    find_library (CCTZ_LIBRARY cctz)
    find_path (CCTZ_INCLUDE_DIR NAMES cctz/civil_time.h civil_time.h PATHS ${CCTZ_INCLUDE_PATHS})
endif ()

if (CCTZ_LIBRARY AND CCTZ_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_CCTZ_LIBRARY)
    set (USE_INTERNAL_CCTZ_LIBRARY 1)
    set (CCTZ_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/cctz/include")
    set (CCTZ_LIBRARY cctz)
endif ()

message (STATUS "Using cctz: ${CCTZ_INCLUDE_DIR} : ${CCTZ_LIBRARY}")
