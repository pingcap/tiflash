add_subdirectory (System)
add_subdirectory (Page)
add_subdirectory (DeltaMerge/File/dtpb)

if (ENABLE_TESTS)
    add_subdirectory (tests EXCLUDE_FROM_ALL)
    add_subdirectory (Transaction/tests EXCLUDE_FROM_ALL)
    add_subdirectory (Page/V2/tests EXCLUDE_FROM_ALL)
    if (ENABLE_V3_PAGESTORAGE)
        add_subdirectory (Page/V3/tests EXCLUDE_FROM_ALL)
    endif ()
    add_subdirectory (DeltaMerge/tests EXCLUDE_FROM_ALL)
endif ()

