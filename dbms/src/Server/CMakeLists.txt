# 'clickhouse' binary is a multi purpose tool,
# that contain multiple execution modes (client, server, etc.)
# each of them is built and linked as a separate library, defined below.


option (ENABLE_CLICKHOUSE_ALL "Enable all tools" ON)
option (ENABLE_CLICKHOUSE_SERVER "Enable server" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_CLIENT "Enable client" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_LOCAL "Enable local" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_BENCHMARK "Enable benchmark" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_PERFORMANCE "Enable performance" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_TOOLS "Enable tools: compressor format extract-from-config-lib" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_COPIER "Enable copier" ${ENABLE_CLICKHOUSE_ALL})

configure_file (config_tools.h.in ${CMAKE_CURRENT_BINARY_DIR}/config_tools.h)

add_library (clickhouse-server-lib
    HTTPHandler.cpp
    InterserverIOHTTPHandler.cpp
    MetricsTransmitter.cpp
    NotFoundHandler.cpp
    PingRequestHandler.cpp
    ReplicasStatusHandler.cpp
    RootRequestHandler.cpp
    Server.cpp
    StatusFile.cpp
    TCPHandler.cpp)

target_link_libraries (clickhouse-server-lib clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)
target_include_directories (clickhouse-server-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

add_library (clickhouse-local-lib LocalServer.cpp)
target_link_libraries (clickhouse-local-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions)

add_library (clickhouse-extract-from-config-lib ${SPLIT_SHARED} ExtractFromConfig.cpp)
target_link_libraries (clickhouse-extract-from-config-lib clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})

add_library (clickhouse-client-lib Client.cpp)
target_link_libraries (clickhouse-client-lib clickhouse_functions clickhouse_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-client-lib PRIVATE ${READLINE_INCLUDE_DIR})
install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)

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

add_library (clickhouse-copier-lib ClusterCopier.cpp)
target_link_libraries (clickhouse-copier-lib clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions)

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
    add_executable (clickhouse-copier clickhouse-copier.cpp)
    target_link_libraries (clickhouse-copier clickhouse-copier-lib)

    set (CLICKHOUSE_ALL_TARGETS clickhouse-server clickhouse-client clickhouse-local clickhouse-benchmark clickhouse-performance-test
            clickhouse-extract-from-config clickhouse-format clickhouse-copier)

    if (USE_EMBEDDED_COMPILER)
        add_executable (clickhouse-clang clickhouse-clang.cpp)
        target_link_libraries (clickhouse-clang clickhouse-compiler-lib)
        add_executable (clickhouse-lld clickhouse-lld.cpp)
        target_link_libraries (clickhouse-lld clickhouse-compiler-lib)
        install (TARGETS clickhouse-clang clickhouse-lld RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list (APPEND CLICKHOUSE_ALL_TARGETS clickhouse-clang clickhouse-lld)
    endif ()

    install (TARGETS clickhouse-server ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_ALL_TARGETS})
    add_custom_target (theflash ALL DEPENDS clickhouse-bundle)
else ()
    add_executable (theflash main.cpp)
    target_link_libraries (theflash clickhouse_common_io)
    target_include_directories (theflash BEFORE PRIVATE ${COMMON_INCLUDE_DIR})
    target_include_directories (theflash PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

    if (USE_EMBEDDED_COMPILER)
        target_link_libraries (theflash clickhouse-compiler-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_SERVER)
        target_link_libraries (theflash clickhouse-server-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_CLIENT)
        target_link_libraries (theflash clickhouse-client-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_LOCAL)
        target_link_libraries (theflash clickhouse-local-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_BENCHMARK)
        target_link_libraries (theflash clickhouse-benchmark-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_PERFORMANCE)
        target_link_libraries (theflash clickhouse-performance-test-lib)
    endif ()
    if (ENABLE_CLICKHOUSE_TOOLS)
    target_link_libraries (theflash
        clickhouse-extract-from-config-lib
        clickhouse-compressor-lib
        clickhouse-format-lib
    )
    endif ()
    if (ENABLE_CLICKHOUSE_COPIER)
        target_link_libraries (theflash clickhouse-copier-lib)
    endif ()

    set (CLICKHOUSE_BUNDLE)
    if (ENABLE_CLICKHOUSE_SERVER)
        add_custom_target (clickhouse-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-server DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-server)
    endif ()
    if (ENABLE_CLICKHOUSE_CLIENT)
        add_custom_target (clickhouse-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-client DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-client DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-client)
    endif ()
    if (ENABLE_CLICKHOUSE_LOCAL)
        add_custom_target (clickhouse-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-local DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-local DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-local)
    endif ()
    if (ENABLE_CLICKHOUSE_BENCHMARK)
        add_custom_target (clickhouse-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-benchmark DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-benchmark DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-benchmark)
    endif ()
    if (ENABLE_CLICKHOUSE_PERFORMANCE)
        add_custom_target (clickhouse-performance-test ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-performance-test DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-performance-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-performance-test)
    endif ()
    if (ENABLE_CLICKHOUSE_TOOLS)
        add_custom_target (clickhouse-extract-from-config ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-extract-from-config DEPENDS theflash)
        add_custom_target (clickhouse-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-compressor DEPENDS theflash)
        add_custom_target (clickhouse-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-format DEPENDS theflash)
        install (FILES
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-extract-from-config
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-compressor
           ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-format
           DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-extract-from-config clickhouse-compressor clickhouse-format)
    endif ()
    if (ENABLE_CLICKHOUSE_COPIER)
        add_custom_target (clickhouse-copier ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-copier DEPENDS theflash)
        install (FILES ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-copier DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-copier)
    endif ()
    # install always because depian package want this files:
    add_custom_target (clickhouse-clang ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-clang DEPENDS theflash)
    add_custom_target (clickhouse-lld ALL COMMAND ${CMAKE_COMMAND} -E create_symlink theflash clickhouse-lld DEPENDS theflash)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-clang clickhouse-lld)

    install (TARGETS theflash RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)

    install (FILES
       ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-clang
       ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-lld
       DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT theflash)

    add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_BUNDLE})
endif ()

set (GLIBC_MAX_REQUIRED 2.4)
add_test(NAME GLIBC_required_version COMMAND bash -c "readelf -s ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-server | grep '@GLIBC' | grep -oP 'GLIBC_[\\d\\.]+' | sort | uniq | sort -r | perl -lnE 'exit 1 if $_ gt q{GLIBC_${GLIBC_MAX_REQUIRED}}'")

install (
    FILES config.xml users.xml
    DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server
    COMPONENT theflash)
