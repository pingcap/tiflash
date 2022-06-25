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

static inline bool mte_supported(void)
{
    unsigned long hwcaps = getauxval(AT_HWCAP2);
    if (hwcaps & HWCAP2_MTE)
        return true;
    return false;
}

static inline bool sve_supported(void)
{
    unsigned long hwcaps = getauxval(AT_HWCAP);
    if (hwcaps & HWCAP_SVE)
        return true;
    return false;
}

#define STRINGIFY_IMPL(X) #X
#define STRINGIFY(X) STRINGIFY_IMPL(X)
#define DISPATCH(NAME, RETURN_TYPE, ARG_LIST, SVE, MTE, ASIMD)    \
    typedef RETURN_TYPE(*__tiflash_##NAME##_t) ARG_LIST;          \
    __tiflash_##NAME##_t __tiflash_##NAME##_resolver(void) \
    {                                                             \
        if (sve_supported())                                      \
            return SVE;                                           \
        if (mte_supported())                                      \
            return MTE;                                           \
        return ASIMD;                                             \
    }                                                             \
    RETURN_TYPE NAME ARG_LIST __attribute__((ifunc(STRINGIFY(__tiflash_##NAME##_resolver))));

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

DISPATCH(memcpy, void *, (void * __restrict, const void * __restrict, size_t), __memcpy_aarch64_sve, __memcpy_aarch64_simd, __memcpy_aarch64_simd)
DISPATCH(memmove, void *, (void *, const void *, size_t), __memmove_aarch64_sve, __memmove_aarch64_simd, __memmove_aarch64_simd)
DISPATCH(memset, void *, (void *, int, size_t), __memset_aarch64, __memset_aarch64, __memset_aarch64)
DISPATCH(memchr, void *, (const void *, int, size_t), __memchr_aarch64_sve, __memchr_aarch64_mte, __memchr_aarch64)
DISPATCH(memrchr, void *, (const void *, int, size_t), __memrchr_aarch64, __memrchr_aarch64, __memrchr_aarch64)
DISPATCH(memcmp, int, (const void *, const void *, size_t), __memcmp_aarch64_sve, __memcmp_aarch64, __memcmp_aarch64)
DISPATCH(strcpy, char *, (char * __restrict, const char * __restrict), __strcpy_aarch64_sve, __strcpy_aarch64, __strcpy_aarch64)
DISPATCH(stpcpy, char *, (char * __restrict, const char * __restrict), __stpcpy_aarch64_sve, __stpcpy_aarch64, __stpcpy_aarch64)
DISPATCH(strcmp, int, (const char *, const char *), __strcmp_aarch64_sve, __strcmp_aarch64, __strcmp_aarch64)
DISPATCH(strchr, char *, (const char *, int), __strchr_aarch64_sve, __strchr_aarch64_mte, __strchr_aarch64)
DISPATCH(strrchr, char *, (const char *, int), __strrchr_aarch64_sve, __strrchr_aarch64_mte, __strrchr_aarch64)
DISPATCH(strchrnul, char *, (const char *, int), __strchrnul_aarch64_sve, __strchrnul_aarch64_mte, __strchrnul_aarch64)
DISPATCH(strlen, size_t, (const char *), __strlen_aarch64_sve, __strlen_aarch64_mte, __strlen_aarch64)
DISPATCH(strnlen, size_t, (const char *, size_t), __strnlen_aarch64_sve, __strnlen_aarch64, __strnlen_aarch64)
DISPATCH(strncmp, int, (const char *, const char *, size_t), __strncmp_aarch64_sve, __strncmp_aarch64, __strncmp_aarch64)