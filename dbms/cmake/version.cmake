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

# This strings autochanged from release_lib.sh:
set(VERSION_DESCRIBE v1.1.54381-testing)
set(VERSION_REVISION 54381)
set(VERSION_GITHASH af82c78a45b6a6f136e10bb2e7ca9b936d09a46c)
# end of autochange

set (VERSION_MAJOR 1)
set (VERSION_MINOR 1)
set (VERSION_PATCH ${VERSION_REVISION})
set (VERSION_EXTRA "")
set (VERSION_TWEAK "")

set (VERSION_STRING "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
if (VERSION_TWEAK)
    set(VERSION_STRING "${VERSION_STRING}.${VERSION_TWEAK}")
endif ()
if (VERSION_EXTRA)
    set(VERSION_STRING "${VERSION_STRING}${VERSION_EXTRA}")
endif ()

set (VERSION_FULL "${PROJECT_NAME} ${VERSION_STRING}")

if (OS_DARWIN)
    # dirty hack: ld: malformed 64-bit a.b.c.d.e version number: 1.1.54160
    math (EXPR VERSION_SO1 "${VERSION_REVISION}/255")
    math (EXPR VERSION_SO2 "${VERSION_REVISION}%255")
    set (VERSION_SO "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_SO1}.${VERSION_SO2}")
else ()
    set (VERSION_SO "${VERSION_STRING}")
endif ()

set (TIFLASH_NAME "TiFlash")

# Semantic version.
set (TIFLASH_VERSION_MAJOR 4)
set (TIFLASH_VERSION_MINOR 1)
set (TIFLASH_VERSION_REVISION 0)
set (TIFLASH_VERSION "${TIFLASH_VERSION_MAJOR}.${TIFLASH_VERSION_MINOR}.${TIFLASH_VERSION_REVISION}")

# Release version that follows PD/TiKV/TiDB convention.
# Variables bellow are important, use `COMMAND_ERROR_IS_FATAL ANY`(since cmake 3.19) to confirm that there is output.

execute_process(
  # Do not execute with --dirty, because checking dirty state is extremely slow.
  COMMAND git describe --tags --always
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE TIFLASH_RELEASE_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY
  )

set (TIFLASH_EDITION $ENV{TIFLASH_EDITION})
if (NOT TIFLASH_EDITION)
    set (TIFLASH_EDITION Community)
endif ()

execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE TIFLASH_GIT_HASH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY
  )

execute_process(
  COMMAND git rev-parse --abbrev-ref HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE TIFLASH_GIT_BRANCH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY
  )

execute_process(
  COMMAND date -u "+%Y-%m-%d %H:%M:%S"
  OUTPUT_VARIABLE TIFLASH_UTC_BUILD_TIME
  OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY
)

set (TIFLASH_PROFILE ${CMAKE_BUILD_TYPE})
