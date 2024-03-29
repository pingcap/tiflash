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

# 'tiflash' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.


option(ENABLE_TIFLASH_ALL "Enable all tools" ON)
option(ENABLE_TIFLASH_SERVER "Enable server" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_CLIENT "Enable client" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_DTTOOL "Enable dttool: tools to manage dmfile" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_DTWORKLOAD "Enable dtworkload: tools to test and stress DeltaTree" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_PAGEWORKLOAD "Enable pageworkload: tools to test and stress PageStorage" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_PAGECTL "Enable pagectl: tools to debug page storage" ${ENABLE_TIFLASH_ALL})
option(ENABLE_TIFLASH_CHECKPOINTTOOL "Enable checkpoint tool: tools to debug checkpoint of PageStorage" ${ENABLE_TIFLASH_ALL})

configure_file (config_tools.h.in ${CMAKE_CURRENT_BINARY_DIR}/config_tools.h)

add_library(server_for_test
    FlashGrpcServerHolder.cpp
)

target_link_libraries (server_for_test PUBLIC tiflash_common_io tiflash_storages_system)
target_include_directories (server_for_test PRIVATE ${TiFlash_SOURCE_DIR}/contrib/grpc/)

add_library (tiflash-server-lib
    CertificateReloader.cpp
    MetricsTransmitter.cpp
    MetricsPrometheus.cpp
    ServerInfo.cpp
    Server.cpp
    StatusFile.cpp
    TCPHandler.cpp
    BgStorageInit.cpp
    Bootstrap.cpp
    StorageConfigParser.cpp
    UserConfigParser.cpp
    RaftConfigParser.cpp)

target_link_libraries(tiflash-server-lib PRIVATE ${TIFLASH_PROXY_LIBRARY})

target_link_libraries (tiflash-server-lib PUBLIC server_for_test tiflash_common_io daemon tiflash_storages_system tiflash_functions tiflash_aggregate_functions tiflash_table_functions)
target_include_directories (tiflash-server-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)
target_include_directories (tiflash-server-lib PRIVATE ${TiFlash_SOURCE_DIR}/contrib/GmSSL/include)

add_library (tiflash-client-lib Client.cpp)
target_link_libraries (tiflash-client-lib tiflash_functions tiflash_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (tiflash-client-lib PRIVATE ${READLINE_INCLUDE_DIR})

add_library(tiflash-dttool-lib ${SPLIT_SHARED} DTTool/DTToolBench.cpp DTTool/DTToolMigrate.cpp DTTool/DTToolInspect.cpp)
target_link_libraries(tiflash-dttool-lib PUBLIC dbms daemon tiflash_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories(tiflash-dttool-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_library(tiflash-dttool-entry-object STATIC DTTool/DTTool.cpp)
target_link_libraries(tiflash-dttool-entry-object tiflash-dttool-lib ${TIFLASH_PROXY_LIBRARY})
target_include_directories(tiflash-dttool-entry-object PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_executable (tiflash main.cpp)

if (USE_INTERNAL_TIFLASH_PROXY)
    get_target_property(TIFLASH_PROXY_LIB_PATH ${TIFLASH_PROXY_LIBRARY} IMPORTED_LOCATION)
    cmake_path(GET TIFLASH_PROXY_LIB_PATH PARENT_PATH TIFLASH_PROXY_LIB_DIR)
else()
    set(TIFLASH_PROXY_LIB_DIR "${PREBUILT_LIBS_ROOT}")
endif()

cmake_path(
    RELATIVE_PATH TIFLASH_PROXY_LIB_DIR
    BASE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    OUTPUT_VARIABLE TIFLASH_PROXY_LIB_RPATH)

if (OS_LINUX)
    if (ARCH_AARCH64)
        set(TIFLASH_ARCH_NAME "aarch64")
    elseif(ARCH_AMD64)
        set(TIFLASH_ARCH_NAME "x86_64")
    endif()

    if (CMAKE_PREFIX_PATH)
        set(TIFLASH_DEPENDENCY_DIR_HINT
                "${CMAKE_PREFIX_PATH}/lib;${CMAKE_PREFIX_PATH}/lib/${TIFLASH_ARCH_NAME}-unknown-linux-gnu/;")
        set(TIFLASH_DEPENDENCY_DIRECTORIES
                "${CMAKE_PREFIX_PATH}/lib"
                "${CMAKE_PREFIX_PATH}/lib/${TIFLASH_ARCH_NAME}-unknown-linux-gnu/")
    else()
        set(TIFLASH_DEPENDENCY_DIR_HINT "")
        set(TIFLASH_DEPENDENCY_DIRECTORIES "")
    endif()

    # set build rpaths, so that executables can be directly called in build tree (easy to debug)
    set_target_properties(tiflash PROPERTIES BUILD_RPATH "$ORIGIN/;$ORIGIN/${TIFLASH_PROXY_LIB_RPATH}/;${TIFLASH_DEPENDENCY_DIR_HINT}")
    set_target_properties(tiflash PROPERTIES INSTALL_RPATH "$ORIGIN/")
    install (TARGETS tiflash
            COMPONENT tiflash-release
            DESTINATION "."
            RUNTIME_DEPENDENCY_SET tiflash-release-dependency)
    install (RUNTIME_DEPENDENCY_SET tiflash-release-dependency
            COMPONENT tiflash-release
            DESTINATION "."
            PRE_EXCLUDE_REGEXES
                "libdl.*"
                "libc-.*"
                "libc\\..*"
                "libgcc_s.*"
                "librt.*"
                "libm.*"
                "ld-linux-x86-64.*"
                "ld-linux-aarch64.*"
                "libpthread.*"# exclude libc dependencies
            DIRECTORIES ${TIFLASH_DEPENDENCY_DIRECTORIES})
    if (USE_GM_SSL)
        install (TARGETS gmssl
                COMPONENT tiflash-release
                DESTINATION ".")
    endif()

    if (NOT (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG" OR SAN_DEBUG))
        install (SCRIPT ${TiFlash_SOURCE_DIR}/cmake/tiflash_linux_post_install.cmake COMPONENT tiflash-release)
    endif()
elseif(APPLE)
    # set build rpaths, so that executables can be directly called in build tree (easy to debug)
    set_target_properties(tiflash PROPERTIES BUILD_RPATH "@executable_path/;@executable_path/${TIFLASH_PROXY_LIB_RPATH}/")
    set_target_properties(tiflash PROPERTIES INSTALL_RPATH "@executable_path/")
    install (TARGETS tiflash
            COMPONENT tiflash-release
            DESTINATION "."
            RUNTIME_DEPENDENCY_SET tiflash-release-dependency)
    install (RUNTIME_DEPENDENCY_SET tiflash-release-dependency
            COMPONENT tiflash-release
            DESTINATION "."
            PRE_INCLUDE_REGEXES ".*proxy.*")
endif ()

include(CMakePrintHelpers)
cmake_print_properties(TARGETS tiflash PROPERTIES BUILD_RPATH INSTALL_RPATH)

target_link_libraries (tiflash tiflash_common_io)
target_include_directories (tiflash BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
target_include_directories (tiflash PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

if (ENABLE_TIFLASH_SERVER)
    target_link_libraries (tiflash tiflash-server-lib)
endif ()
if (ENABLE_TIFLASH_CLIENT)
    target_link_libraries (tiflash tiflash-client-lib)
endif ()
if (ENABLE_TIFLASH_DTTOOL)
    target_link_libraries(tiflash tiflash-dttool-entry-object)
endif ()
if (ENABLE_TIFLASH_DTWORKLOAD)
    target_link_libraries(tiflash dt-workload-lib)
endif ()
if (ENABLE_TIFLASH_PAGEWORKLOAD)
    target_link_libraries(tiflash page-workload-lib)
endif()
if (ENABLE_TIFLASH_PAGECTL)
    target_link_libraries(tiflash page-ctl-lib)
endif ()
if (ENABLE_TIFLASH_CHECKPOINTTOOL)
    target_link_libraries(tiflash checkpoint-tool-lib)
endif ()

# install always because depian package want this files:
install (TARGETS tiflash RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

set (GLIBC_MAX_REQUIRED 2.4)
add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/tiflash | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort -r | perl -lnE 'exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'")
