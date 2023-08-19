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

option(USE_INTERNAL_CURL_LIBRARY "Set to FALSE to use external curl instead of bundled. (Experimental. Set to FALSE on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/curl/CMakeLists.txt")
    if(USE_INTERNAL_CURL_LIBRARY )
        message(WARNING "submodule contrib/curl is missing. to fix try run: \n git submodule update --init")
        message(WARNING "Can't use internal curl")
        set(USE_INTERNAL_CURL_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_CURL_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_CURL_LIBRARY)
    find_package(curl)
    if(NOT CURL_INCLUDE_DIRS)
        message(WARNING "Can't find external curl include dir")
        set(EXTERNAL_CURL_LIBRARY_FOUND 0)
    elseif(NOT CURL_LIBRARIES)
        message(WARNING "Can't find external curl library")
        set(EXTERNAL_CURL_LIBRARY_FOUND 0)
    else()
        set(EXTERNAL_CURL_LIBRARY_FOUND 1)
    endif()
endif()

if(NOT EXTERNAL_CURL_LIBRARY_FOUND)
    if(NOT MISSING_INTERNAL_CURL_LIBRARY)
        set(CURL_INCLUDE_DIRS "${TiFlash_SOURCE_DIR}/contrib/curl/include")
        set(CURL_LIBRARIES libcurl)
        set(CURL_FOUND TRUE)
        set(USE_INTERNAL_CURL_LIBRARY 1)
    else()
        message(FATAL_ERROR "Can't find curl")
    endif()
endif()

message(STATUS "Using curl: ${USE_INTERNAL_CURL_LIBRARY} : ${CURL_INCLUDE_DIRS}, ${CURL_LIBRARIES}")
