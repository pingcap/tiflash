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

option(USE_INTERNAL_TIFLASH_PROXY "Set to FALSE to use external tiflash-proxy instead of bundled. (Only used in CI. Set to FALSE on your own risk)" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/Makefile")
    if(USE_INTERNAL_TIFLASH_PROXY)
        message(WARNING "submodule contrib/tiflash-proxy is missing. to fix try run: \n git submodule update --init")
        message(WARNING "Can't use internal tiflash-proxy")
        set(USE_INTERNAL_TIFLASH_PROXY FALSE)
    endif()
    set(MISSING_INTERNAL_TIFLASH_PROXY 1)
endif()

if(NOT USE_INTERNAL_TIFLASH_PROXY)
    find_path(TIFLASH_PROXY_INCLUDE_DIR NAMES RaftStoreProxyFFI/ProxyFFI.h PATH_SUFFIXES raftstore-proxy/ffi/src)
    find_library(TIFLASH_PROXY_LIBRARY NAMES tiflash_proxy PATH_SUFFIXES PATH_SUFFIXES target/release)
    if(NOT TIFLASH_PROXY_INCLUDE_DIR)
        message(WARNING "Can't find external tiflash-proxy include dir")
        set(EXTERNAL_TIFLASH_PROXY_FOUND FALSE)
    elseif(NOT TIFLASH_PROXY_LIBRARY)
        message(WARNING "Can't find external tiflash-proxy library")
        set(EXTERNAL_TIFLASH_PROXY_FOUND FALSE)
    else()
        # Reuse the prebuilt tiflash-proxy library (classic or next-gen)
        message(STATUS "Found pre-built tiflash-proxy library")
        set(EXTERNAL_TIFLASH_PROXY_FOUND TRUE)
    endif()
endif()

if(NOT EXTERNAL_TIFLASH_PROXY_FOUND)
    if(NOT MISSING_INTERNAL_TIFLASH_PROXY)
        if (ENABLE_NEXT_GEN)
            set(TIFLASH_PROXY_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy-next-gen/raftstore-proxy/ffi/src")
        else()
            set(TIFLASH_PROXY_INCLUDE_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/raftstore-proxy/ffi/src")
        endif()
        set(TIFLASH_PROXY_LIBRARY libtiflash_proxy)
        set(USE_INTERNAL_TIFLASH_PROXY TRUE)
    else()
        message(FATAL_ERROR "Can't find tiflash-proxy")
    endif()
endif()

set(TIFLASH_PROXY_FOUND TRUE)
if (ENABLE_NEXT_GEN AND NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy-next-gen/proxy_components/proxy_ffi/src/cloud_helper.rs")
    message(FATAL "Can't find next-gen tiflash-proxy")
endif()

message(STATUS "Using tiflash-proxy: USE_INTERNAL_TIFLASH_PROXY:${USE_INTERNAL_TIFLASH_PROXY}, headers:${TIFLASH_PROXY_INCLUDE_DIR}, lib:${TIFLASH_PROXY_LIBRARY}, ENABLE_NEXT_GEN:${ENABLE_NEXT_GEN}")

if (NOT USE_INTERNAL_TIFLASH_PROXY)
    add_custom_target(tiflash_proxy ALL DEPENDS ${TIFLASH_PROXY_LIBRARY})
endif()
