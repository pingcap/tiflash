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

# - Try to find cityhash headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(cityhash)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  CITYHASH_ROOT_DIR Set this variable to the root installation of
#                    cityhash if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  CITYHASH_FOUND             System has cityhash libs/headers
#  CITYHASH_LIBRARIES         The cityhash library/libraries
#  CITYHASH_INCLUDE_DIR       The location of cityhash headers

find_path(CITYHASH_ROOT_DIR
    NAMES include/city.h
)

find_library(CITYHASH_LIBRARIES
    NAMES cityhash
    PATHS ${CITYHASH_ROOT_DIR}/lib ${CITYHASH_LIBRARIES_PATHS}
)

find_path(CITYHASH_INCLUDE_DIR
    NAMES city.h
    PATHS ${CITYHASH_ROOT_DIR}/include ${CITYHASH_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cityhash DEFAULT_MSG
    CITYHASH_LIBRARIES
    CITYHASH_INCLUDE_DIR
)

mark_as_advanced(
    CITYHASH_ROOT_DIR
    CITYHASH_LIBRARIES
    CITYHASH_INCLUDE_DIR
)
