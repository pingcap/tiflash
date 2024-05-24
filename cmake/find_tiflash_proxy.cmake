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

option(USE_INTERNAL_TIFLASH_PROXY "Set to FALSE to use external tiflash proxy instead of bundled. (Only used in CI. Set to FALSE on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/Makefile")
    if(USE_INTERNAL_TIFLASH_PROXY)
        message(WARNING "submodule contrib/tiflash-proxy is missing. to fix try run: \n git submodule update --init")
        message(WARNING "Can't use internal tiflash proxy")
        set(USE_INTERNAL_TIFLASH_PROXY 0)
    endif()
    set(MISSING_INTERNAL_TIFLASH_PROXY 1)
endif()

if(NOT USE_INTERNAL_TIFLASH_PROXY)
    find_path(TIFLASH_PROXY_INCLUDE_DIR NAMES RaftStoreProxyFFI/ProxyFFI.h PATH_SUFFIXES raftstore-proxy/ffi/src)
    find_library(TIFLASH_PROXY_LIBRARY NAMES tiflash_proxy PATH_SUFFIXES PATH_SUFFIXES target/release)
    if(NOT TIFLASH_PROXY_INCLUDE_DIR)
        message(WARNING "Can't find external tiflash proxy include dir")
        set(EXTERNAL_TIFLASH_PROXY_FOUND 0)
    elseif(NOT TIFLASH_PROXY_LIBRARY)
        message(WARNING "Can't find external tiflash proxy library")
        set(EXTERNAL_TIFLASH_PROXY_FOUND 0)
    else()
        set(EXTERNAL_TIFLASH_PROXY_FOUND 1)
    endif()
endif()

if(NOT EXTERNAL_TIFLASH_PROXY_FOUND)
    if(NOT MISSING_INTERNAL_TIFLASH_PROXY)
        set(TIFLASH_PROXY_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src")
        set(TIFLASH_PROXY_LIBRARY libtiflash_proxy)
        set(USE_INTERNAL_TIFLASH_PROXY 1)
    else()
        message(FATAL_ERROR "Can't find tiflash proxy")
    endif()
endif()

set(TIFLASH_PROXY_FOUND TRUE)
# SERVERLESS_PROXY=0 if using normal proxy.
# SERVERLESS_PROXY=1 if using serverless proxy.
if (EXISTS "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/proxy_components/proxy_ffi/src/cloud_helper.rs")
    add_definitions(-DSERVERLESS_PROXY=1)
else()
    add_definitions(-DSERVERLESS_PROXY=0)
endif()

message(STATUS "Using tiflash proxy: ${USE_INTERNAL_TIFLASH_PROXY} : ${TIFLASH_PROXY_INCLUDE_DIR}, ${TIFLASH_PROXY_LIBRARY}")

if (NOT USE_INTERNAL_TIFLASH_PROXY)
    add_custom_target(tiflash_proxy ALL DEPENDS ${TIFLASH_PROXY_LIBRARY})
endif()
