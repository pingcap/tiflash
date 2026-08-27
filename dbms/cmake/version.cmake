# Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/cmake/version.cmake
#
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

set (TIFLASH_NAME "TiFlash")

# Release version that follows PD/TiKV/TiDB convention.
# Variables bellow are important, use `COMMAND_ERROR_IS_FATAL ANY`(since cmake 3.19) to confirm that there is output.

if (ENABLE_NEXT_GEN)
  set(TIFLASH_NEXT_GEN_DEFAULT_VERSION "v27.0.0-pre" CACHE STRING
      "Fallback release version for next-gen builds")

  # Prefer a next-gen release tag when it is reachable from the current commit.
  # Development branches may not contain a next-gen tag, so use the default
  # next-gen version when Git returns a commit hash or an unrelated tag.
  execute_process(
    COMMAND git describe --tags --always
            --match "v2[0-9].*"
            --match "v3[0-9].*"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE TIFLASH_RELEASE_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
    )

  if (NOT "${TIFLASH_RELEASE_VERSION}" MATCHES "^v(2[0-9]|3[0-9])\\.[0-9]+\\.[0-9]+([-.].*)?$")
    set(TIFLASH_RELEASE_VERSION "${TIFLASH_NEXT_GEN_DEFAULT_VERSION}")
  endif ()
else ()
  execute_process(
    # Do not execute with --dirty, because checking dirty state is extremely slow.
    COMMAND git describe --tags --always
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE TIFLASH_RELEASE_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
    )
endif ()

# Extract the major version number
string(REGEX REPLACE "^v([0-9]+).*" "\\1" TIFLASH_VERSION_MAJOR ${TIFLASH_RELEASE_VERSION})
# Extract the minor version number
string(REGEX REPLACE "^v[0-9]+\\.([0-9]+).*" "\\1" TIFLASH_VERSION_MINOR ${TIFLASH_RELEASE_VERSION})
# Extract the patch version number
string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" TIFLASH_VERSION_PATCH ${TIFLASH_RELEASE_VERSION})
# Extract the extra information if it exists
set(TIFLASH_VERSION_EXTRA "")
if ("${TIFLASH_RELEASE_VERSION}" MATCHES "-.*")
  string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.[0-9]+-(.*)$" "\\1" TIFLASH_VERSION_EXTRA ${TIFLASH_RELEASE_VERSION})
  set(TIFLASH_VERSION "${TIFLASH_VERSION_MAJOR}.${TIFLASH_VERSION_MINOR}.${TIFLASH_VERSION_PATCH}-${TIFLASH_VERSION_EXTRA}")
else ()
  set(TIFLASH_VERSION "${TIFLASH_VERSION_MAJOR}.${TIFLASH_VERSION_MINOR}.${TIFLASH_VERSION_PATCH}")
endif ()

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
