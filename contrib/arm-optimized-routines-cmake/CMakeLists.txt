ENABLE_LANGUAGE(C)
ENABLE_LANGUAGE(ASM)
set(TIFLASH_AOR_DIR ../arm-optimized-routines)

# GCC/Clang enables ASIMD for Armv8-a by default, we let us use
# ASIMD variant anyway

set(TIFLASH_AARCH64_STRING_FILES
        ${TIFLASH_AOR_DIR}/string/aarch64/memchr.S
        ${TIFLASH_AOR_DIR}/string/aarch64/memcmp.S
        ${TIFLASH_AOR_DIR}/string/aarch64/memcpy-advsimd.S
        ${TIFLASH_AOR_DIR}/string/aarch64/memrchr.S
        ${TIFLASH_AOR_DIR}/string/aarch64/memset.S
        ${TIFLASH_AOR_DIR}/string/aarch64/stpcpy.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strchr.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strchrnul.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strcmp.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strcpy.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strlen.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strcmp.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strnlen.S
        ${TIFLASH_AOR_DIR}/string/aarch64/strrchr.S
        )
add_library(tiflash-aarch64-string STATIC ${TIFLASH_AARCH64_STRING_FILES})
target_compile_definitions(tiflash-aarch64-string PRIVATE
        ARM_FNSTART=
        ARM_FNEND=
        __memcpy_aarch64=memcpy
        __memmove_aarch64=memmove
        __memset_aarch64=memset
        __memchr_aarch64=memchr
        __memrchr_aarch64=memrchr
        __memcmp_aarch64=memcmp
        __strcpy_aarch64=strcpy
        __strcmp_aarch64=strcmp
        __strchr_aarch64=strchr
        __strrchr_aarch64=strrchr
        __strchrnul_aarch64=strchrnul
        __strlen_aarch64=strlen
        __strnlen_aarch64=strnlen
        __strncmp_aarch64=strncmp
        __memcpy_aarch64_simd=memcpy
        __memmove_aarch64_simd=memmove
)

file(GLOB TIFLASH_AARCH64_MATH_FILES ${TIFLASH_AOR_DIR}/math/*.c)
add_library(tiflash-aarch64-math STATIC ${TIFLASH_AARCH64_MATH_FILES})
target_include_directories(tiflash-aarch64-math PRIVATE ${TIFLASH_AOR_DIR}/math/include)