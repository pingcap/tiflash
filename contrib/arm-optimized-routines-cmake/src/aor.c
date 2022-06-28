// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <stringlib.h>
#include <sys/auxv.h>

// Provide default macro definitions in case that they are not defined on current linux distro.
// For example, TiFlash compiled on older linux kernels may also be used in newer ones.
// These values should be stable for Linux: only false negative is expected when running on
// older kernels, but it is acceptable as `google/cpu_features` is also doing so.
#ifndef HWCAP2_MTE
#define HWCAP2_MTE (1 << 18)
#endif

#ifndef HWCAP_SVE
#define HWCAP_SVE (1 << 22)
#endif

#ifndef AT_HWCAP2
#define AT_HWCAP2 26
#endif

#ifndef AT_HWCAP
#define AT_HWCAP 16
#endif

/// check if MTE is supported in current environment
static inline bool mte_supported(void)
{
    return (getauxval(AT_HWCAP2) & HWCAP2_MTE) != 0;
}

/// check if SVE is supported in current environment
static inline bool sve_supported(void)
{
    return (getauxval(AT_HWCAP) & HWCAP_SVE) != 0;
}

#define STRINGIFY_IMPL(X) #X
#define STRINGIFY(X) STRINGIFY_IMPL(X)
/**
 *  \brief
 *  Symbol is defined as hidden visibility. Therefore, implementations here are only to override routines with TiFlash
 *  binary itself. This is because dependencies like `ld.so`, `libgcc_s.so`, etc will need essential routines like
 *  `memcpy` to finish the early loading procedure. Therefore, declare such symbols as visible indirect function will
 *  create cyclic dependency. It shall be good enough to override symbols within TiFlash, as most heavy computation works
 *  are happening in the main binary.
 *  \param NAME: exported symbol name
 *  \param SVE: preferred implementation when SVE is available
 *  \param MTE: preferred implementation when MTE is available
 *  \param ASIMD: preferred implementation for generic aarch64 targets (ASIMD is required by default for Armv8 and above)
 */
#define DISPATCH(NAME, SVE, MTE, ASIMD)                                                                                  \
    extern typeof(ASIMD) __tiflash_##NAME __attribute__((ifunc(STRINGIFY(__tiflash_##NAME##_resolver))));                \
    extern typeof(ASIMD) NAME __attribute__((visibility("hidden"), alias(STRINGIFY(__tiflash_##NAME))));                 \
    _Pragma("GCC diagnostic push")                                                                                       \
        _Pragma("GCC diagnostic ignored \"-Wunused-function\"") static typeof(ASIMD) * __tiflash_##NAME##_resolver(void) \
    {                                                                                                                    \
        if (sve_supported())                                                                                             \
        {                                                                                                                \
            return SVE;                                                                                                  \
        }                                                                                                                \
        if (mte_supported())                                                                                             \
        {                                                                                                                \
            return MTE;                                                                                                  \
        }                                                                                                                \
        return ASIMD;                                                                                                    \
    }                                                                                                                    \
    _Pragma("GCC diagnostic pop")
#undef memcpy
#undef memmove
#undef memset
#undef memchr
#undef memrchr
#undef memcmp
#undef strcpy
#undef stpcpy
#undef strcmp
#undef strchr
#undef strrchr
#undef strchrnul
#undef strlen
#undef strnlen
#undef strncmp

DISPATCH(memcpy, __memcpy_aarch64_sve, __memcpy_aarch64_simd, __memcpy_aarch64_simd)
DISPATCH(memmove, __memmove_aarch64_sve, __memmove_aarch64_simd, __memmove_aarch64_simd)
DISPATCH(memset, __memset_aarch64, __memset_aarch64, __memset_aarch64)
DISPATCH(memchr, __memchr_aarch64_sve, __memchr_aarch64_mte, __memchr_aarch64)
DISPATCH(memrchr, __memrchr_aarch64, __memrchr_aarch64, __memrchr_aarch64)
DISPATCH(memcmp, __memcmp_aarch64_sve, __memcmp_aarch64, __memcmp_aarch64)
DISPATCH(strcpy, __strcpy_aarch64_sve, __strcpy_aarch64, __strcpy_aarch64)
DISPATCH(stpcpy, __stpcpy_aarch64_sve, __stpcpy_aarch64, __stpcpy_aarch64)
DISPATCH(strcmp, __strcmp_aarch64_sve, __strcmp_aarch64, __strcmp_aarch64)
DISPATCH(strchr, __strchr_aarch64_sve, __strchr_aarch64_mte, __strchr_aarch64)
DISPATCH(strrchr, __strrchr_aarch64_sve, __strrchr_aarch64_mte, __strrchr_aarch64)
DISPATCH(strchrnul, __strchrnul_aarch64_sve, __strchrnul_aarch64_mte, __strchrnul_aarch64)
DISPATCH(strlen, __strlen_aarch64_sve, __strlen_aarch64_mte, __strlen_aarch64)
DISPATCH(strnlen, __strnlen_aarch64_sve, __strnlen_aarch64, __strnlen_aarch64)
DISPATCH(strncmp, __strncmp_aarch64_sve, __strncmp_aarch64, __strncmp_aarch64)