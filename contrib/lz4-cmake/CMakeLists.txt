SET(LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/lz4/lib)

add_library (lz4
    ${LIBRARY_DIR}/lz4.c
    ${LIBRARY_DIR}/lz4hc.c

    ${LIBRARY_DIR}/lz4.h
    ${LIBRARY_DIR}/lz4hc.h)

target_compile_definitions(lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1)
target_compile_definitions(lz4 PUBLIC LZ4_FAST_DEC_LOOP=1)

target_include_directories(lz4 PUBLIC ${LIBRARY_DIR})

if (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
endif ()
