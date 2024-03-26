if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(fastpforlib STATIC bitpacking.cpp)

target_include_directories(fastpforlib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(fastpforlib PROPERTIES EXPORT_NAME fastpforlib)

if (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
endif ()
