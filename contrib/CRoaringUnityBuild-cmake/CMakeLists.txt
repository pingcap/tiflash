SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/CRoaringUnityBuild)

add_library(roaring
        ${LIBRARY_DIR}/roaring.c
        ${LIBRARY_DIR}/roaring.h
        ${LIBRARY_DIR}/roaring.hh)

target_include_directories (roaring PUBLIC ${LIBRARY_DIR})
