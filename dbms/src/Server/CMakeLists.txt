# Copyright 2022 PingCAP, Ltd.
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

# 'clickhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.


option(ENABLE_CLICKHOUSE_ALL "Enable all tools" ON)
option(ENABLE_CLICKHOUSE_SERVER "Enable server" ${ENABLE_CLICKHOUSE_ALL})
option(ENABLE_CLICKHOUSE_CLIENT "Enable client" ${ENABLE_CLICKHOUSE_ALL})
option(ENABLE_TIFLASH_DTTOOL "Enable dttool: tools to manage dmfile" ${ENABLE_CLICKHOUSE_ALL})
option(ENABLE_TIFLASH_DTWORKLOAD "Enable dtworkload: tools to test and stress DeltaTree" ${ENABLE_CLICKHOUSE_ALL})

configure_file (config_tools.h.in ${CMAKE_CURRENT_BINARY_DIR}/config_tools.h)

add_library (clickhouse-server-lib
    HTTPHandler.cpp
    MetricsTransmitter.cpp
    MetricsPrometheus.cpp
    NotFoundHandler.cpp
    PingRequestHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    StatusFile.cpp
    TCPHandler.cpp
    StorageConfigParser.cpp
    UserConfigParser.cpp
    RaftConfigParser.cpp)

target_link_libraries(clickhouse-server-lib PRIVATE ${TIFLASH_PROXY_LIBRARY})

target_link_libraries (clickhouse-server-lib PUBLIC clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (clickhouse-server-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_library (clickhouse-client-lib Client.cpp)
target_link_libraries (clickhouse-client-lib clickhouse_functions clickhouse_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-client-lib PRIVATE ${READLINE_INCLUDE_DIR})

add_library(tiflash-dttool-lib ${SPLIT_SHARED} DTTool/DTToolBench.cpp DTTool/DTToolMigrate.cpp DTTool/DTToolInspect.cpp)
target_link_libraries(tiflash-dttool-lib PUBLIC dbms daemon clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories(tiflash-dttool-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_library(tiflash-dttool-entry-object STATIC DTTool/DTTool.cpp)
target_link_libraries(tiflash-dttool-entry-object tiflash-dttool-lib ${TIFLASH_PROXY_LIBRARY})
target_include_directories(tiflash-dttool-entry-object PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

if (USE_EMBEDDED_COMPILER)
    link_directories (${LLVM_LIBRARY_DIRS})
    add_subdirectory ("Compiler-${LLVM_VERSION}")
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-server clickhouse-server.cpp)
    target_link_libraries (clickhouse-server clickhouse-server-lib)
    add_executable (clickhouse-client clickhouse-client.cpp)
    target_link_libraries (clickhouse-client clickhouse-client-lib)
    # Also in utils
    add_executable (tiflash-dttool DTTool/tiflash-dttool.cpp)
    target_link_libraries (tiflash-dttool tiflash-dttool-entry-object)

    set (CLICKHOUSE_ALL_TARGETS clickhouse-server clickhouse-client clickhouse-extract-from-config clickhouse-format)

    if (USE_EMBEDDED_COMPILER)
        add_executable (clickhouse-clang clickhouse-clang.cpp)
        target_link_libraries (clickhouse-clang clickhouse-compiler-lib)
        add_executable (clickhouse-lld clickhouse-lld.cpp)
        target_link_libraries (clickhouse-lld clickhouse-compiler-lib)
        install (TARGETS clickhouse-clang clickhouse-lld RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list (APPEND CLICKHOUSE_ALL_TARGETS clickhouse-clang clickhouse-lld)
    endif ()

    install (TARGETS clickhouse-server ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_ALL_TARGETS})
    add_custom_target (tiflash ALL DEPENDS clickhouse-bundle)
else ()
    add_executable (tiflash main.cpp)

    if (USE_INTERNAL_TIFLASH_PROXY)
        set(PROXY_LIBRARY_DIR "${TiFlash_SOURCE_DIR}/contrib/tiflash-proxy/target/release")
    else()
        set(PROXY_LIBRARY_DIR "${PREBUILT_LIBS_ROOT}")
    endif()

    if (ARCH_LINUX)
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

        # set build rpaths, so that executables are be directly called in build tree (easy to debug)
        set_target_properties(tiflash PROPERTIES BUILD_RPATH "${PROXY_LIBRARY_DIR};${TIFLASH_DEPENDENCY_DIR_HINT}")
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

        install (SCRIPT ${TiFlash_SOURCE_DIR}/cmake/tiflash_linux_post_install.cmake COMPONENT tiflash-release)
    elseif(APPLE)
        # set build rpaths, so that executables are be directly called in build tree (easy to debug)
        set_target_properties(tiflash PROPERTIES BUILD_RPATH "${PROXY_LIBRARY_DIR}")
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

    target_link_libraries (tiflash clickhouse_common_io)
    target_include_directories (tiflash BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
    target_include_directories (tiflash PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

    if (USE_EMBEDDED_COMPILER)
        target_link_libraries (tiflash clickhouse-compiler-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_SERVER)
        target_link_libraries (tiflash clickhouse-server-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_CLIENT)
        target_link_libraries (tiflash clickhouse-client-lib)
    endif ()
    if (ENABLE_TIFLASH_DTTOOL)
        target_link_libraries(tiflash tiflash-dttool-entry-object)
    endif ()
    if (ENABLE_TIFLASH_DTWORKLOAD)
        target_link_libraries(tiflash dt-workload-lib)
    endif ()

    set (CLICKHOUSE_BUNDLE)
    if (ENABLE_CLICKHOUSE_SERVER)
        add_custom_target (clickhouse-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-server DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-server)
    endif ()
    if (ENABLE_CLICKHOUSE_CLIENT)
        add_custom_target (clickhouse-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-client DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-client DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-client)
    endif ()
    if (ENABLE_TIFLASH_DTTOOL)
        add_custom_target (tiflash-dttool ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-dttool DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-dttool DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE tiflash-dttool)
    endif ()
    # install always because depian package want this files:
    add_custom_target (clickhouse-clang ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-clang DEPENDS tiflash)
    add_custom_target (clickhouse-lld ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-lld DEPENDS tiflash)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-clang clickhouse-lld)
    install (TARGETS tiflash RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    install (FILES
       ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-clang
       ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-lld
       DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_BUNDLE})
endif ()

set (GLIBC_MAX_REQUIRED 2.4)
add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort -r | perl -lnE 'exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'")
