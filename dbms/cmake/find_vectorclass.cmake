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


option (ENABLE_VECTORCLASS "Faster math functions with vectorclass lib" OFF)

if (ENABLE_VECTORCLASS)

    set (VECTORCLASS_INCLUDE_PATHS "${TiFlash_SOURCE_DIR}/contrib/vectorclass" CACHE STRING "Path of vectorclass library")
    find_path (VECTORCLASS_INCLUDE_DIR NAMES vectorf128.h PATHS ${VECTORCLASS_INCLUDE_PATHS})

    if (VECTORCLASS_INCLUDE_DIR)
        set (USE_VECTORCLASS 1)
    endif ()

    message (STATUS "Using vectorclass=${USE_VECTORCLASS}: ${VECTORCLASS_INCLUDE_DIR}")

endif ()
