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

macro(find_contrib_lib LIB_NAME)

    string(TOLOWER ${LIB_NAME} LIB_NAME_LC)
    string(TOUPPER ${LIB_NAME} LIB_NAME_UC)
    string(REPLACE "-" "_" LIB_NAME_UC ${LIB_NAME_UC})

    option (USE_INTERNAL_${LIB_NAME_UC}_LIBRARY "Use bundled library ${LIB_NAME} instead of system" ${NOT_UNBUNDLED})

    if (NOT USE_INTERNAL_${LIB_NAME_UC}_LIBRARY)
        find_package ("${LIB_NAME}")
    endif ()

    if (NOT ${LIB_NAME_UC}_FOUND)
        set (USE_INTERNAL_${LIB_NAME_UC}_LIBRARY 1)
        set (${LIB_NAME_UC}_LIBRARIES ${LIB_NAME_LC})
        set (${LIB_NAME_UC}_INCLUDE_DIR ${${LIB_NAME_UC}_CONTRIB_INCLUDE_DIR})
    endif ()

    message (STATUS "Using ${LIB_NAME}: ${${LIB_NAME_UC}_INCLUDE_DIR} : ${${LIB_NAME_UC}_LIBRARIES}")

endmacro()
