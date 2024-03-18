set(USEARCH_PROJECT_DIR "${TiFlash_SOURCE_DIR}/contrib/usearch")
set(USEARCH_SOURCE_DIR "${USEARCH_PROJECT_DIR}/include")

add_library(_usearch INTERFACE)

target_include_directories(_usearch SYSTEM INTERFACE
    ${USEARCH_PROJECT_DIR}/simsimd/include
    ${USEARCH_PROJECT_DIR}/fp16/include
    ${USEARCH_SOURCE_DIR})

add_library(tiflash_contrib::usearch ALIAS _usearch)
