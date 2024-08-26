# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# https://software.intel.com/sites/landingpage/IntrinsicsGuide/

include (CheckCXXSourceCompiles)
include (CMakePushCheckState)

cmake_push_check_state ()

# The variables HAVE_* determine if compiler has support for the flag to use the corresponding instruction set.
# The options ENABLE_* determine if we will tell compiler to actually use the corresponding instruction set if compiler can do it.

# All of them are unrelated to the instruction set at the host machine
# (you can compile for newer instruction set on old machines and vice versa).

option (ARCH_NATIVE "Add -march=native compiler flag. This makes your binaries non-portable but more performant code may be generated. This option overrides ENABLE_* options for specific instruction set. Highly not recommended to use." OFF)

if (ARCH_AARCH64)
    # ARM publishes almost every year a new revision of it's ISA [1]. Each version comes with new mandatory and optional features from
    # which CPU vendors can pick and choose. This creates a lot of variability ... We provide two build "profiles", one for maximum
    # compatibility intended to run on all 64-bit ARM hardware released after 2013 (e.g. Raspberry Pi 4), and one for modern ARM server
    # CPUs, (e.g. Graviton).
    #
    # [1] https://en.wikipedia.org/wiki/AArch64
    option (TIFLASH_ENABLE_ASIMD_SUPPORT "Enable Advanced SIMD support." ON)
    option (TIFLASH_ENABLE_SVE_SUPPORT "Enable Scalable Vector Extension support." OFF)
    # TODO: default ON, to be changed after CI is updated
    option (NO_ARMV81_OR_HIGHER "Disable ARMv8.1 or higher on Aarch64 for maximum compatibility with older/embedded hardware." ON)

    if (NO_ARMV81_OR_HIGHER)
        # crc32 is optional in v8.0 and mandatory in v8.1. Enable it as __crc32()* is used in lot's of places and even very old ARM CPUs
        # support it.
        set (COMPILER_FLAGS "${COMPILER_FLAGS} -march=armv8+crc")
        if (TIFLASH_ENABLE_ASIMD_SUPPORT)
            set (COMPILER_FLAGS "${COMPILER_FLAGS}+simd")
            add_definitions(-DTIFLASH_ENABLE_ASIMD_SUPPORT=1)
        endif ()
    else ()
        # ARMv8.2 is quite ancient but the lowest common denominator supported by both Graviton 2 and 3 processors [1]. In particular, it
        # includes LSE (made mandatory with ARMv8.1) which provides nice speedups without having to fall back to compat flag
        # "-moutline-atomics" for v8.0 [2, 3, 4] that requires a recent glibc with runtime dispatch helper, limiting our ability to run on
        # old OSs.
        #
        # simd:    NEON, introduced as optional in v8.0, A few extensions were added with v8.1 but it's still not mandatory. Enables the
        #          compiler to auto-vectorize.
        # sve:     Scalable Vector Extensions, introduced as optional in v8.2. Available in Graviton 3 but not in Graviton 2, and most likely
        #          also not in CI machines. Compiler support for autovectorization is rudimentary at the time of writing, see [5]. Can be
        #          enabled one-fine-day (TM) but not now.
        # ssbs:    "Speculative Store Bypass Safe". Optional in v8.0, mandatory in v8.5. Meltdown/spectre countermeasure.
        # crypto:  SHA1, SHA256, AES. Optional in v8.0. In v8.4, further algorithms were added but it's still optional, see [6].
        # dotprod: Scalar vector product (SDOT and UDOT instructions). Probably the most obscure extra flag with doubtful performance benefits
        #          but it has been activated since always, so why not enable it. It's not 100% clear in which revision this flag was
        #          introduced as optional, either in v8.2 [7] or in v8.4 [8].
        # ldapr:   Load-Acquire RCpc Register. Better support of release/acquire of atomics. Good for allocators and high contention code.
        #          Optional in v8.2, mandatory in v8.3 [9]. Supported in Graviton 2+, Azure and GCP instances. Generated from clang 15.
        #
        # [1] https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md
        # [2] https://community.arm.com/arm-community-blogs/b/tools-software-ides-blog/posts/making-the-most-of-the-arm-architecture-in-gcc-10
        # [3] https://mysqlonarm.github.io/ARM-LSE-and-MySQL/
        # [4] https://dev.to/aws-builders/large-system-extensions-for-aws-graviton-processors-3eci
        # [5] https://developer.arm.com/tools-and-software/open-source-software/developer-tools/llvm-toolchain/sve-support
        # [6] https://developer.arm.com/documentation/100067/0612/armclang-Command-line-Options/-mcpu?lang=en
        # [7] https://gcc.gnu.org/onlinedocs/gcc/ARM-Options.html
        # [8] https://developer.arm.com/documentation/102651/a/What-are-dot-product-intructions-
        # [9] https://developer.arm.com/documentation/dui0801/g/A64-Data-Transfer-Instructions/LDAPR?lang=en
        set (TEST_FLAG "-march=armv8.2-a+crypto+ssbs+dotprod")
        if (TIFLASH_ENABLE_ASIMD_SUPPORT)
            set (TEST_FLAG "${TEST_FLAG}+simd")
            add_definitions(-DTIFLASH_ENABLE_ASIMD_SUPPORT=1)
        endif ()
        if (TIFLASH_ENABLE_SVE_SUPPORT)
            set (TEST_FLAG "${TEST_FLAG}+sve")
            add_definitions(-DTIFLASH_ENABLE_SVE_SUPPORT=1)
        endif ()
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

elseif (ARCH_AMD64)
    # enable sse4.2 or lower default, check avx, avx2, avx512
    # set the flags for specific files rather than globally
    # in order to avoid SSE-AVX Transition Penalty
    # and note that the compile will succeed even if the host machine does not support the instruction set
    # so we do not set the flags to avoid core dump in old machines
    option (TIFLASH_ENABLE_AVX_SUPPORT "Use AVX/AVX2 instructions on x86_64" ON)
    option (TIFLASH_ENABLE_AVX512_SUPPORT "Use AVX512 instructions on x86_64" ON)

    # `haswell` was released since 2013 with cpu feature avx2, bmi2. It's a practical arch for optimizer
    option (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT "Use instructions based on architecture `haswell` on x86_64" ON)

    option (NO_AVX_OR_HIGHER "Disable AVX or higher on x86_64 for maximum compatibility with older/embedded hardware." OFF)
    if (NO_AVX_OR_HIGHER)
        SET(TIFLASH_ENABLE_AVX_SUPPORT OFF)
        SET(TIFLASH_ENABLE_AVX512_SUPPORT OFF)
        SET (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT OFF)
    endif()

    set (TEST_FLAG "-mssse3")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <tmmintrin.h>
        int main() {
            __m64 a = _mm_abs_pi8(__m64());
            (void)a;
            return 0;
        }
    " HAVE_SSSE3)
    if (HAVE_SSSE3)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-msse4.1")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <smmintrin.h>
        int main() {
            auto a = _mm_insert_epi8(__m128i(), 0, 0);
            (void)a;
            return 0;
        }
    " HAVE_SSE41)
    if (HAVE_SSE41)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-msse4.2")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <nmmintrin.h>
        int main() {
            auto a = _mm_crc32_u64(0, 0);
            (void)a;
            return 0;
        }
    " HAVE_SSE42)
    if (HAVE_SSE42)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mpclmul")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <wmmintrin.h>
        int main() {
            auto a = _mm_clmulepi64_si128(__m128i(), __m128i(), 0);
            (void)a;
            return 0;
        }
    " HAVE_PCLMULQDQ)
    if (HAVE_PCLMULQDQ)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TEST_FLAG "-mpopcnt")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        int main() {
            auto a = __builtin_popcountll(0);
            (void)a;
            return 0;
        }
    " HAVE_POPCNT)
    if (HAVE_POPCNT)
        set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
    endif ()

    set (TIFLASH_COMPILER_AVX2_FLAG "-mavx2")
    set (TEST_FLAG "${TIFLASH_COMPILER_AVX2_FLAG}")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm256_insert_epi8(__m256i(), 0, 0);
            (void)a;
            auto b = _mm256_add_epi16(__m256i(), __m256i());
            (void)b;
            return 0;
        }
    " HAVE_AVX2)
    if (HAVE_AVX2 AND TIFLASH_ENABLE_AVX_SUPPORT)
        # set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
        add_definitions(-DTIFLASH_ENABLE_AVX_SUPPORT=1)
    endif ()

    set (TEST_FLAG "-mavx512f -mavx512bw -mavx512vl")
    set (CMAKE_REQUIRED_FLAGS "${TEST_FLAG} -O0")
    check_cxx_source_compiles("
        #include <immintrin.h>
        int main() {
            auto a = _mm512_setzero_epi32();
            (void)a;
            auto b = _mm512_add_epi16(__m512i(), __m512i());
            (void)b;
            auto c = _mm_cmp_epi8_mask(__m128i(), __m128i(), 0);
            (void)c;
            return 0;
        }
    " HAVE_AVX512)
    message(STATUS "HAVE_AVX512: ${HAVE_AVX512}")
    if (HAVE_AVX512 AND TIFLASH_ENABLE_AVX512_SUPPORT)
        # set (COMPILER_FLAGS "${COMPILER_FLAGS} ${TEST_FLAG}")
        add_definitions(-DTIFLASH_ENABLE_AVX512_SUPPORT=1)
    endif ()

    set (TIFLASH_COMPILER_ARCH_HASWELL_FLAG "-march=haswell")
    check_cxx_compiler_flag("${TIFLASH_COMPILER_ARCH_HASWELL_FLAG}" COMPILER_SUPPORT_ARCH_HASWELL)
    if (NOT COMPILER_SUPPORT_ARCH_HASWELL)
        set (TIFLASH_ENABLE_ARCH_HASWELL_SUPPORT OFF)
    endif ()
else ()
    # ignore all other platforms
endif ()

cmake_pop_check_state ()
