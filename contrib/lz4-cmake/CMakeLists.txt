# lz4 is the main compression method, cannot be disabled.

SET(LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/lz4)

set (SRCS
    "${LIBRARY_DIR}/lib/lz4.c"
    "${LIBRARY_DIR}/lib/lz4hc.c"
    "${LIBRARY_DIR}/lib/lz4frame.c"
    "${LIBRARY_DIR}/lib/xxhash.c"
)

add_library(lz4 ${SRCS})
target_compile_definitions(lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1)
target_include_directories(lz4 PUBLIC "${LIBRARY_DIR}/lib")

if (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
endif ()
