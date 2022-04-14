# 'clickhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.


option (ENABLE_CLICKHOUSE_ALL "Enable all tools" ON)
option (ENABLE_CLICKHOUSE_SERVER "Enable server" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_CLIENT "Enable client" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_LOCAL "Enable local" OFF)
option (ENABLE_CLICKHOUSE_BENCHMARK "Enable benchmark" OFF)
option (ENABLE_CLICKHOUSE_PERFORMANCE "Enable performance" OFF)
option (ENABLE_CLICKHOUSE_TOOLS "Enable tools: compressor format extract-from-config-lib" OFF)

configure_file (config_tools.h.in ${CMAKE_CURRENT_BINARY_DIR}/config_tools.h)

add_library (clickhouse-server-lib
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

target_link_libraries(clickhouse-server-lib PRIVATE ${TIFLASH_PROXY_LIBRARY})

target_link_libraries (clickhouse-server-lib PUBLIC clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (clickhouse-server-lib PUBLIC ${TiFlash_SOURCE_DIR}/libs/libdaemon/include)

add_library (clickhouse-local-lib LocalServer.cpp)
target_link_libraries (clickhouse-local-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)

add_library (clickhouse-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (clickhouse-extract-from-config-lib clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-client-lib Client.cpp)
target_link_libraries (clickhouse-client-lib clickhouse_functions clickhouse_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-client-lib PRIVATE ${READLINE_INCLUDE_DIR})

add_library (clickhouse-benchmark-lib ${SPLIT_SHARED} Benchmark.cpp)
target_link_libraries (clickhouse-benchmark-lib clickhouse-client-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-benchmark-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-performance-test-lib ${SPLIT_SHARED} PerformanceTest.cpp)
target_link_libraries (clickhouse-performance-test-lib clickhouse_common_io dbms ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-performance-test-lib PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

add_library (clickhouse-compressor-lib ${SPLIT_SHARED} Compressor.cpp)
target_link_libraries (clickhouse-compressor-lib clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-format-lib ${SPLIT_SHARED} Format.cpp)
target_link_libraries (clickhouse-format-lib dbms clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (USE_EMBEDDED_COMPILER)
    link_directories (${LLVM_LIBRARY_DIRS})
    add_subdirectory ("Compiler-${LLVM_VERSION}")
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-server clickhouse-server.cpp)
    target_link_libraries (clickhouse-server clickhouse-server-lib)
    add_executable (clickhouse-client clickhouse-client.cpp)
    target_link_libraries (clickhouse-client clickhouse-client-lib)
    add_executable (clickhouse-local clickhouse-local.cpp)
    target_link_libraries (clickhouse-local clickhouse-local-lib)
    add_executable (clickhouse-benchmark clickhouse-benchmark.cpp)
    target_link_libraries (clickhouse-benchmark clickhouse-benchmark-lib clickhouse_aggregate_functions)
    add_executable (clickhouse-performance-test clickhouse-performance-test.cpp)
    target_link_libraries (clickhouse-performance-test clickhouse-performance-test-lib dbms)
    add_executable (clickhouse-extract-from-config clickhouse-extract-from-config.cpp)
    target_link_libraries (clickhouse-extract-from-config clickhouse-extract-from-config-lib)
    # Also in utils
    add_executable (clickhouse-compressor clickhouse-compressor.cpp)
    target_link_libraries (clickhouse-compressor clickhouse-compressor-lib)
    add_executable (clickhouse-format clickhouse-format.cpp)
    target_link_libraries (clickhouse-format clickhouse-format-lib)

    set (CLICKHOUSE_ALL_TARGETS clickhouse-server clickhouse-client clickhouse-local clickhouse-benchmark clickhouse-performance-test
            clickhouse-extract-from-config clickhouse-format)

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
    if (ENABLE_CLICKHOUSE_LOCAL)
        target_link_libraries (tiflash clickhouse-local-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_BENCHMARK)
        target_link_libraries (tiflash clickhouse-benchmark-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_PERFORMANCE)
        target_link_libraries (tiflash clickhouse-performance-test-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_TOOLS)
    target_link_libraries (tiflash
        clickhouse-extract-from-config-lib
        clickhouse-compressor-lib
        clickhouse-format-lib
    )
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
    if (ENABLE_CLICKHOUSE_LOCAL)
        add_custom_target (clickhouse-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-local DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-local DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-local)
    endif ()
    if (ENABLE_CLICKHOUSE_BENCHMARK)
        add_custom_target (clickhouse-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-benchmark DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-benchmark DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-benchmark)
    endif ()
    if (ENABLE_CLICKHOUSE_PERFORMANCE)
        add_custom_target (clickhouse-performance-test ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-performance-test DEPENDS tiflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-performance-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-performance-test)
    endif ()
    if (ENABLE_CLICKHOUSE_TOOLS)
        add_custom_target (clickhouse-extract-from-config ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-extract-from-config DEPENDS tiflash)
        add_custom_target (clickhouse-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-compressor DEPENDS tiflash)
        add_custom_target (clickhouse-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink tiflash clickhouse-format DEPENDS tiflash)
        install (FILES
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-extract-from-config
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-compressor
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-format
           DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-extract-from-config clickhouse-compressor clickhouse-format)
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
