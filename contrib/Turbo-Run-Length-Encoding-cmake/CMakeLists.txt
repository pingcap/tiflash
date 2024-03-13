SET(LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/Turbo-Run-Length-Encoding/include)

add_library (trle ${LIBRARY_DIR}/trle.h)
target_include_directories(trle PUBLIC ${LIBRARY_DIR})
set_target_properties(trle PROPERTIES LINKER_LANGUAGE C)

if (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT)
    target_compile_definitions(trle PRIVATE AVX2=1)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
endif ()