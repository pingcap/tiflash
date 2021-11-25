add_subdirectory (System)
add_subdirectory (Page)
add_subdirectory (DeltaMerge/File/dtpb)

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
    add_subdirectory (Transaction/tests EXCLUDE_FROM_ALL)
    add_subdirectory (Page/V2/tests EXCLUDE_FROM_ALL)
    add_subdirectory (DeltaMerge/tests EXCLUDE_FROM_ALL)
endif ()

