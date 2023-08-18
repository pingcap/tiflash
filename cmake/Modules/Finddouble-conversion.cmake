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

# - Try to find double-conversion headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(double-conversion)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  DOUBLE_CONVERSION_ROOT_DIR Set this variable to the root installation of
#                    double-conversion if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  DOUBLE_CONVERSION_FOUND             System has double-conversion libs/headers
#  DOUBLE_CONVERSION_LIBRARIES         The double-conversion library/libraries
#  DOUBLE_CONVERSION_INCLUDE_DIR       The location of double-conversion headers

find_path(DOUBLE_CONVERSION_ROOT_DIR
    NAMES include/double-conversion/double-conversion.h
)

find_library(DOUBLE_CONVERSION_LIBRARIES
    NAMES double-conversion
    PATHS ${DOUBLE_CONVERSION_ROOT_DIR}/lib ${BTRIE_CITYHASH_PATHS}
)

find_path(DOUBLE_CONVERSION_INCLUDE_DIR
    NAMES double-conversion/double-conversion.h
    PATHS ${DOUBLE_CONVERSION_ROOT_DIR}/include ${DOUBLE_CONVERSION_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(double_conversion DEFAULT_MSG
    DOUBLE_CONVERSION_LIBRARIES
    DOUBLE_CONVERSION_INCLUDE_DIR
)

mark_as_advanced(
    DOUBLE_CONVERSION_ROOT_DIR
    DOUBLE_CONVERSION_LIBRARIES
    DOUBLE_CONVERSION_INCLUDE_DIR
)
