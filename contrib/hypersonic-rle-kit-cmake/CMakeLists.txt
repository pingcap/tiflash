SET(LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/hypersonic-rle-kit/src)

add_library (hypersonic-rle
    # ${LIBRARY_DIR}/main.c
    # ${LIBRARY_DIR}/rle_fuzz.c
    # ${LIBRARY_DIR}/rle.h
    ${LIBRARY_DIR}/bit_mmtf.c
    ${LIBRARY_DIR}/bitpack.h
    ${LIBRARY_DIR}/codec_funcs.h
    ${LIBRARY_DIR}/mmtf.c
    ${LIBRARY_DIR}/rle128_extreme_cpu.c
    ${LIBRARY_DIR}/rle128_extreme_cpu.h
    ${LIBRARY_DIR}/rle24_extreme_cpu.c
    ${LIBRARY_DIR}/rle24_extreme_cpu_decode.h
    ${LIBRARY_DIR}/rle24_extreme_cpu_encode.h
    ${LIBRARY_DIR}/rle24_extreme_cpu.h
    ${LIBRARY_DIR}/rle48_extreme_cpu.c
    ${LIBRARY_DIR}/rle48_extreme_cpu_decode.h
    ${LIBRARY_DIR}/rle48_extreme_cpu_encode.h
    ${LIBRARY_DIR}/rle48_extreme_cpu.h
    ${LIBRARY_DIR}/rle8_extreme_cpu.c
    ${LIBRARY_DIR}/rle8_extreme_cpu.h
    ${LIBRARY_DIR}/rle8_low_entropy_cpu.c
    ${LIBRARY_DIR}/rle8_low_entropy_short_cpu.c
    ${LIBRARY_DIR}/rle8_mmtf.c
    ${LIBRARY_DIR}/rle8_ocl.c
    ${LIBRARY_DIR}/rle8_ocl_kernel.h
    ${LIBRARY_DIR}/rle_sh.c
    ${LIBRARY_DIR}/rleX_extreme_common.h
    ${LIBRARY_DIR}/rleX_extreme_cpu.c
    ${LIBRARY_DIR}/rleX_extreme_cpu_decode.h
    ${LIBRARY_DIR}/rleX_extreme_cpu_encode.h
    ${LIBRARY_DIR}/rleX_extreme_cpu.h
    ${LIBRARY_DIR}/rleX_Xsl.c
    ${LIBRARY_DIR}/rleX_Xsl.h
    ${LIBRARY_DIR}/rleX_Xsl_multibyte_encoder.h
    ${LIBRARY_DIR}/rleX_Xsl_short.c
    ${LIBRARY_DIR}/rleX_Xsl_short.h
    ${LIBRARY_DIR}/rleX_Xsl_short_multibyte_encoder.h
    ${LIBRARY_DIR}/simd_platform.c
    ${LIBRARY_DIR}/simd_platform.h)

# Don't use the original rle.h, because it contains some ambiguous macro definitions
target_include_directories(hypersonic-rle PUBLIC ${TiFlash_SOURCE_DIR}/contrib/hypersonic-rle-kit-cmake/include)
set_target_properties(hypersonic-rle PROPERTIES EXPORT_NAME hypersonic-rle)

if (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}")
endif ()
