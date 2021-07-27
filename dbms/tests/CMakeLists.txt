enable_testing()

# Run tests with "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${TIFLASH_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS tiflash-test tiflash-test-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT tiflash)
install (
    DIRECTORY queries performance external_dictionaries
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/tiflash-test
    USE_SOURCE_PERMISSIONS
    COMPONENT tiflash
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

install (FILES server-test.xml DESTINATION  ${TIFLASH_ETC_DIR}/tiflash-server COMPONENT tiflash)
install (FILES client-test.xml DESTINATION  ${TIFLASH_ETC_DIR}/tiflash-client COMPONENT tiflash)

add_subdirectory (external_dictionaries)

if (ENABLE_TESTS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake ${CMAKE_BINARY_DIR})

    # maybe add --no-long ?
    add_test(NAME with_server COMMAND bash -c "env BUILD_DIR=${TIFLASH_BINARY_DIR} TEST_OPT0='--skip compile' ${CMAKE_CURRENT_SOURCE_DIR}/tiflash-test-server")
endif ()
