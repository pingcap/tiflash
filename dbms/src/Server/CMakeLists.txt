# 'tiflash' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.


option (ENABLE_TIFLASH_ALL "Enable all tools" ON)
option (ENABLE_TIFLASH_SERVER "Enable server" ${ENABLE_TIFLASH_ALL})
option (ENABLE_TIFLASH_CLIENT "Enable client" ${ENABLE_TIFLASH_ALL})
option (ENABLE_TIFLASH_LOCAL "Enable local" OFF)
option (ENABLE_TIFLASH_BENCHMARK "Enable benchmark" OFF)
option (ENABLE_TIFLASH_PERFORMANCE "Enable performance" OFF)
option (ENABLE_TIFLASH_TOOLS "Enable tools: compressor format extract-from-config-lib" OFF)

configure_file (config_tools.h.in ${CMAKE_CURRENT_BINARY_DIR}/config_tools.h)

add_library (tiflash-server-lib
    HTTPHandler.cpp
    InterserverIOHTTPHandler.cpp
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
    RaftConfigParser.cpp
    ClusterManagerService.cpp)

target_link_libraries(tiflash-server-lib PRIVATE ${TIFLASH_PROXY_LIBRARY})

target_link_libraries (tiflash-server-lib tiflash_common_io daemon tiflash_storages_system tiflash_functions tiflash_aggregate_functions tiflash_table_functions)
target_include_directories (tiflash-server-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_library (tiflash-local-lib LocalServer.cpp)
target_link_libraries (tiflash-local-lib tiflash-server-lib tiflash_functions tiflash_aggregate_functions tiflash_table_functions)

add_library (tiflash-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (tiflash-extract-from-config-lib tiflash_common_config tiflash_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (tiflash-client-lib Client.cpp)
target_link_libraries (tiflash-client-lib tiflash_functions tiflash_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (tiflash-client-lib PRIVATE ${READLINE_INCLUDE_DIR})
install (FILES tiflash-client.xml DESTINATION ${TIFLASH_ETC_DIR}/tiflash-client COMPONENT tiflash-client RENAME config.xml)

add_library (tiflash-benchmark-lib ${SPLIT_SHARED} Benchmark.cpp)
target_link_libraries (tiflash-benchmark-lib tiflash-client-lib tiflash_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (tiflash-benchmark-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (tiflash-performance-test-lib ${SPLIT_SHARED} PerformanceTest.cpp)
target_link_libraries (tiflash-performance-test-lib tiflash_common_io dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (tiflash-performance-test-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (tiflash-compressor-lib ${SPLIT_SHARED} Compressor.cpp)
target_link_libraries (tiflash-compressor-lib tiflash_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (tiflash-format-lib ${SPLIT_SHARED} Format.cpp)
target_link_libraries (tiflash-format-lib dbms tiflash_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (USE_EMBEDDED_COMPILER)
    link_directories (${LLVM_LIBRARY_DIRS})
    add_subdirectory ("Compiler-${LLVM_VERSION}")
endif ()

if (TIFLASH_SPLIT_BINARY)
    add_executable (tiflash-server tiflash-server.cpp)
    target_link_libraries (tiflash-server tiflash-server-lib)
    add_executable (tiflash-client tiflash-client.cpp)
    target_link_libraries (tiflash-client tiflash-client-lib)
    add_executable (tiflash-local tiflash-local.cpp)
    target_link_libraries (tiflash-local tiflash-local-lib)
    add_executable (tiflash-benchmark tiflash-benchmark.cpp)
    target_link_libraries (tiflash-benchmark tiflash-benchmark-lib tiflash_aggregate_functions)
    add_executable (tiflash-performance-test tiflash-performance-test.cpp)
    target_link_libraries (tiflash-performance-test tiflash-performance-test-lib dbms)
    add_executable (tiflash-extract-from-config tiflash-extract-from-config.cpp)
    target_link_libraries (tiflash-extract-from-config tiflash-extract-from-config-lib)
    # Also in utils
    add_executable (tiflash-compressor tiflash-compressor.cpp)
    target_link_libraries (tiflash-compressor tiflash-compressor-lib)
    add_executable (tiflash-format tiflash-format.cpp)
    target_link_libraries (tiflash-format tiflash-format-lib)

    set (TIFLASH_ALL_TARGETS tiflash-server tiflash-client tiflash-local tiflash-benchmark tiflash-performance-test
            tiflash-extract-from-config tiflash-format)

    if (USE_EMBEDDED_COMPILER)
        add_executable (tiflash-clang tiflash-clang.cpp)
        target_link_libraries (tiflash-clang tiflash-compiler-lib)
        add_executable (tiflash-lld tiflash-lld.cpp)
        target_link_libraries (tiflash-lld tiflash-compiler-lib)
        install (TARGETS tiflash-clang tiflash-lld RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list (APPEND TIFLASH_ALL_TARGETS tiflash-clang tiflash-lld)
    endif ()

    install (TARGETS tiflash-server ${TIFLASH_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    add_custom_target (tiflash-bundle ALL DEPENDS ${TIFLASH_ALL_TARGETS})
    add_custom_target (tiflash ALL DEPENDS tiflash-bundle)
else ()
    add_executable (tiflash main.cpp)
    target_link_libraries (tiflash tiflash_common_io)
    target_include_directories (tiflash BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
    target_include_directories (tiflash PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

    if (USE_EMBEDDED_COMPILER)
        target_link_libraries (tiflash tiflash-compiler-lib)
    endif ()
    if (ENABLE_TIFLASH_SERVER)
        target_link_libraries (tiflash tiflash-server-lib)
    endif ()
    if (ENABLE_TIFLASH_CLIENT)
        target_link_libraries (tiflash tiflash-client-lib)
    endif ()
    if (ENABLE_TIFLASH_LOCAL)
        target_link_libraries (tiflash tiflash-local-lib)
    endif ()
    if (ENABLE_TIFLASH_BENCHMARK)
        target_link_libraries (tiflash tiflash-benchmark-lib)
    endif ()
    if (ENABLE_TIFLASH_PERFORMANCE)
        target_link_libraries (tiflash tiflash-performance-test-lib)
    endif ()
    if (ENABLE_TIFLASH_TOOLS)
    target_link_libraries (tiflash
        tiflash-extract-from-config-lib
        tiflash-compressor-lib
        tiflash-format-lib
    )
    endif ()

    set (TIFLASH_BUNDLE)
    if (ENABLE_TIFLASH_SERVER)
        add_custom_target (tiflash-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-server DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-server)
    endif ()
    if (ENABLE_TIFLASH_CLIENT)
        add_custom_target (tiflash-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-client DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-client DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-client)
    endif ()
    if (ENABLE_TIFLASH_LOCAL)
        add_custom_target (tiflash-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-local DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-local DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-local)
    endif ()
    if (ENABLE_TIFLASH_BENCHMARK)
        add_custom_target (tiflash-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-benchmark DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-benchmark DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-benchmark)
    endif ()
    if (ENABLE_TIFLASH_PERFORMANCE)
        add_custom_target (tiflash-performance-test ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-performance-test DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/tiflash-performance-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-performance-test)
    endif ()
    if (ENABLE_TIFLASH_TOOLS)
        add_custom_target (tiflash-extract-from-config ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-extract-from-config DEPENDS tiflash)
        add_custom_target (tiflash-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-compressor DEPENDS tiflash)
        add_custom_target (tiflash-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-format DEPENDS tiflash)
        install (FILES
           ${CMAKE_CURRENT_BINARY_DIR}/tiflash-extract-from-config
           ${CMAKE_CURRENT_BINARY_DIR}/tiflash-compressor
           ${CMAKE_CURRENT_BINARY_DIR}/tiflash-format
           DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND TIFLASH_BUNDLE tiflash-extract-from-config tiflash-compressor tiflash-format)
    endif ()
    # install always because depian package want this files:
    add_custom_target (tiflash-clang ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-clang DEPENDS tiflash)
    add_custom_target (tiflash-lld ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash tiflash-lld DEPENDS tiflash)
    list(APPEND TIFLASH_BUNDLE tiflash-clang tiflash-lld)

    install (TARGETS tiflash RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    install (FILES
       ${CMAKE_CURRENT_BINARY_DIR}/tiflash-clang
       ${CMAKE_CURRENT_BINARY_DIR}/tiflash-lld
       DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)

    add_custom_target (tiflash-bundle ALL DEPENDS ${TIFLASH_BUNDLE})
endif ()

set (GLIBC_MAX_REQUIRED 2.4)
add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/tiflash-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort -r | perl -lnE 'exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'")

install (
    FILES config.xml users.xml
    DESTINATION  ${TIFLASH_ETC_DIR}/tiflash-server
    COMPONENT tiflash)
