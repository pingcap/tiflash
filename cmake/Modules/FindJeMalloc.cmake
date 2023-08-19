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

# https://github.com/bro/cmake/blob/master/FindJeMalloc.cmake
#
# - Try to find jemalloc headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(JeMalloc)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  JEMALLOC_ROOT_DIR Set this variable to the root installation of
#                    jemalloc if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  JEMALLOC_FOUND             System has jemalloc libs/headers
#  JEMALLOC_LIBRARIES         The jemalloc library/libraries
#  JEMALLOC_INCLUDE_DIR       The location of jemalloc headers

find_path(JEMALLOC_ROOT_DIR
    NAMES include/jemalloc/jemalloc.h
)

find_library(JEMALLOC_LIBRARIES
    NAMES jemalloc
    HINTS ${JEMALLOC_ROOT_DIR}/lib
)

find_path(JEMALLOC_INCLUDE_DIR
    NAMES jemalloc/jemalloc.h
    HINTS ${JEMALLOC_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JeMalloc DEFAULT_MSG
    JEMALLOC_LIBRARIES
    JEMALLOC_INCLUDE_DIR
)

mark_as_advanced(
    JEMALLOC_ROOT_DIR
    JEMALLOC_LIBRARIES
    JEMALLOC_INCLUDE_DIR
)
