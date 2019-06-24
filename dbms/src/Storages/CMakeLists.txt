add_subdirectory (System)

if (ENABLE_TESTS)
    add_subdirectory (tests)
    add_subdirectory (Transaction/tests)
    add_subdirectory (DeltaMerge/tests)
    add_subdirectory (Page/tests)
endif ()

