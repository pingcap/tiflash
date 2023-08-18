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

# Default value on mac is ON to make development easy.
# On other platform it is OFF because we should release tiflash binary built with openssl.
option(USE_INTERNAL_SSL_LIBRARY "Set to FALSE to use system *ssl library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/boringssl/README.md")
    if(USE_INTERNAL_SSL_LIBRARY)
        message(WARNING "submodule contrib/boringssl is missing. to fix try run: \n git submodule update --init")
        message (WARNING "Can't find internal ssl library")
    endif()
    set(USE_INTERNAL_SSL_LIBRARY 0)
    set(MISSING_INTERNAL_SSL_LIBRARY 1)
endif()

if (NOT OS_DARWIN)
    option(USE_GM_SSL "Set to FALSE to disable GmSSL" ${USE_INTERNAL_SSL_LIBRARY})
else()
    # Avoid link to GmSSL when compile on macos because GmSSL only supports dynamic link which complicate the binary package
    option(USE_GM_SSL "Set to FALSE to disable GmSSL" 0)
endif()

set (OPENSSL_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})

if (NOT USE_INTERNAL_SSL_LIBRARY)
    if (OS_DARWIN)
        set (OPENSSL_ROOT_DIR "/usr/local/opt/openssl@1.1" CACHE INTERNAL "")
        # https://rt.openssl.org/Ticket/Display.html?user=guest&pass=guest&id=2232
        if (USE_STATIC_LIBRARIES)
            message(WARNING "Disable USE_STATIC_LIBRARIES if you have linking problems with OpenSSL on MacOS")
        endif ()
    endif ()
    find_package (OpenSSL)

    if (NOT OPENSSL_FOUND)
        # Try to find manually.
        set (OPENSSL_INCLUDE_PATHS "/usr/local/opt/openssl/include")
        set (OPENSSL_PATHS "/usr/local/opt/openssl/lib")
        find_path (OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS ${OPENSSL_INCLUDE_PATHS})
        find_library (OPENSSL_SSL_LIBRARY ssl PATHS ${OPENSSL_PATHS})
        find_library (OPENSSL_CRYPTO_LIBRARY crypto PATHS ${OPENSSL_PATHS})
        if (OPENSSL_SSL_LIBRARY AND OPENSSL_CRYPTO_LIBRARY AND OPENSSL_INCLUDE_DIR)
            set (OPENSSL_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
            set (OPENSSL_FOUND 1)
        endif ()
    endif ()

    if (NOT OPENSSL_FOUND)
        message (WARNING "Can't find system ssl")
    endif()
endif ()

if (NOT OPENSSL_FOUND AND NOT MISSING_INTERNAL_SSL_LIBRARY)
    set (USE_INTERNAL_SSL_LIBRARY 1)
    set (OPENSSL_ROOT_DIR "${TiFlash_SOURCE_DIR}/contrib/boringssl")
    set (OPENSSL_INCLUDE_DIR "${OPENSSL_ROOT_DIR}/include")
    set (OPENSSL_CRYPTO_LIBRARY crypto)
    set (OPENSSL_SSL_LIBRARY ssl)
    set (OPENSSL_FOUND 1)
    set (OPENSSL_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
endif ()

if(OPENSSL_FOUND)
    # we need keep OPENSSL_FOUND for many libs in contrib
    set(USE_SSL 1)
endif()

# used by new poco
# part from /usr/share/cmake-*/Modules/FindOpenSSL.cmake, with removed all "EXISTS "
if(OPENSSL_FOUND AND NOT USE_INTERNAL_SSL_LIBRARY)
    if(NOT TARGET OpenSSL::Crypto AND
    (OPENSSL_CRYPTO_LIBRARY OR
        LIB_EAY_LIBRARY_DEBUG OR
        LIB_EAY_LIBRARY_RELEASE)
        )
        add_library(OpenSSL::Crypto UNKNOWN IMPORTED)
        set_target_properties(OpenSSL::Crypto PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}")
        if(OPENSSL_CRYPTO_LIBRARY)
            set_target_properties(OpenSSL::Crypto PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                IMPORTED_LOCATION "${OPENSSL_CRYPTO_LIBRARY}")
        endif()
        if(LIB_EAY_LIBRARY_RELEASE)
            set_property(TARGET OpenSSL::Crypto APPEND PROPERTY
                IMPORTED_CONFIGURATIONS RELEASE)
            set_target_properties(OpenSSL::Crypto PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
                IMPORTED_LOCATION_RELEASE "${LIB_EAY_LIBRARY_RELEASE}")
        endif()
        if(LIB_EAY_LIBRARY_DEBUG)
            set_property(TARGET OpenSSL::Crypto APPEND PROPERTY
                IMPORTED_CONFIGURATIONS DEBUG)
            set_target_properties(OpenSSL::Crypto PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
                IMPORTED_LOCATION_DEBUG "${LIB_EAY_LIBRARY_DEBUG}")
        endif()
    endif()
    if(NOT TARGET OpenSSL::SSL AND
    (OPENSSL_SSL_LIBRARY OR
        SSL_EAY_LIBRARY_DEBUG OR
        SSL_EAY_LIBRARY_RELEASE)
        )
        add_library(OpenSSL::SSL UNKNOWN IMPORTED)
        set_target_properties(OpenSSL::SSL PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}")
        if(OPENSSL_SSL_LIBRARY)
            set_target_properties(OpenSSL::SSL PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                IMPORTED_LOCATION "${OPENSSL_SSL_LIBRARY}")
        endif()
        if(SSL_EAY_LIBRARY_RELEASE)
            set_property(TARGET OpenSSL::SSL APPEND PROPERTY
                IMPORTED_CONFIGURATIONS RELEASE)
            set_target_properties(OpenSSL::SSL PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
                IMPORTED_LOCATION_RELEASE "${SSL_EAY_LIBRARY_RELEASE}")
        endif()
        if(SSL_EAY_LIBRARY_DEBUG)
            set_property(TARGET OpenSSL::SSL APPEND PROPERTY
                IMPORTED_CONFIGURATIONS DEBUG)
            set_target_properties(OpenSSL::SSL PROPERTIES
                IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
                IMPORTED_LOCATION_DEBUG "${SSL_EAY_LIBRARY_DEBUG}")
        endif()
        if(TARGET OpenSSL::Crypto)
            set_target_properties(OpenSSL::SSL PROPERTIES
                INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)
        endif()
    endif()
endif()

message (STATUS "Using ssl=${USE_SSL} gmssl=${USE_GM_SSL}: ${OPENSSL_INCLUDE_DIR} : ${OPENSSL_LIBRARIES}")
