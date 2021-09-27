include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

add_executable (gtests_libdaemon EXCLUDE_FROM_ALL gtest_daemon_config.cpp)
target_link_libraries (gtests_libdaemon gtest_main daemon)
#add for libcctz used by BaseDaemon
if (APPLE)
    set_target_properties(gtests_libdaemon PROPERTIES LINK_FLAGS "-framework CoreFoundation")
endif ()
add_check(gtests_libdaemon)

target_include_directories (gtests_libdaemon PRIVATE ${ClickHouse_SOURCE_DIR}/libs/libpocoext/include)