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

option (ENABLE_ICU "Enable ICU" OFF)

if (ENABLE_ICU)
    set (ICU_PATHS "/usr/local/opt/icu4c/lib")
    set (ICU_INCLUDE_PATHS "/usr/local/opt/icu4c/include")
    find_library (ICUI18N icui18n PATHS ${ICU_PATHS})
    find_library (ICUUC icuuc PATHS ${ICU_PATHS})
    find_library (ICUDATA icudata PATHS ${ICU_PATHS})
    set (ICU_LIBS ${ICUI18N} ${ICUUC} ${ICUDATA})

    find_path (ICU_INCLUDE_DIR NAMES unicode/unistr.h PATHS ${ICU_INCLUDE_PATHS})
    if (ICU_INCLUDE_DIR AND ICU_LIBS)
        set(USE_ICU 1)
    endif ()
endif ()

if (USE_ICU)
    message (STATUS "Using icu=${USE_ICU}: ${ICU_INCLUDE_DIR} : ${ICU_LIBS}")
else ()
    message (STATUS "Build without ICU (support for collations and charset conversion functions will be disabled)")
endif ()
