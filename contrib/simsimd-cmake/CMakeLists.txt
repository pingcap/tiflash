set(SIMSIMD_PROJECT_DIR "${TiFlash_SOURCE_DIR}/contrib/simsimd")
set(SIMSIMD_SOURCE_DIR "${SIMSIMD_PROJECT_DIR}/include")

add_library(_simsimd INTERFACE)

if (NOT EXISTS "${SIMSIMD_SOURCE_DIR}/simsimd/simsimd.h")
    message (FATAL_ERROR "submodule contrib/simsimd not found")
endif()

target_include_directories(_simsimd SYSTEM INTERFACE
    ${SIMSIMD_SOURCE_DIR})

add_library(tiflash_contrib::simsimd ALIAS _simsimd)
