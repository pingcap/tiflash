// Copyright 2022 PingCAP, Inc.
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

#pragma once

extern "C" {
#include <stringlib.h>
}

#include <cstddef>
#include <cstring>

#define ALWAYS_INLINE __attribute__((__always_inline__))

ALWAYS_INLINE static inline void * inline_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __memcpy_aarch64_sve(dst_, src_, size);
#elif __ARM_NEON
    return __memcpy_aarch64_simd(dst_, src_, size);
#endif
    return __memcpy_aarch64(dst_, src_, size);
#elif __arm__
    return __memcpy_arm(dst_, src_, size);
#else
    return memcpy(dst_, src_, size);
#endif
}

ALWAYS_INLINE static inline void * inline_memmove(void * __restrict dst_, const void * src_, size_t size)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __memmove_aarch64_sve(dst_, src_, size);
#elif __ARM_NEON
    return __memmove_aarch64_simd(dst_, src_, size);
#endif
    return __memmove_aarch64(dst_, src_, size);
#elif __arm__
    return __memmove_arm(dst_, src_, size);
#else
    return memmove(dst_, src_, size);
#endif
}

ALWAYS_INLINE static inline void * inline_memset(void * dst_, int c, size_t size)
{
#if __aarch64__
    return __memset_aarch64(dst_, c, size);
#elif __arm__
    return __memset_arm(dst_, c, size);
#else
    return memset(dst_, c, size);
#endif
}

ALWAYS_INLINE static inline void * inline_memchr(const void * src_, int c, size_t size)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __memchr_aarch64_sve(src_, c, size);
#elif __ARM_FEATURE_MEMORY_TAGGING
    return __memchr_aarch64_mte(src_, c, size);
#endif
    return __memchr_aarch64(src_, c, size);
#elif __arm__
    return __memchr_arm(src_, c, size);
#else
    return memchr(src_, c, size);
#endif
}

ALWAYS_INLINE static inline void * inline_memrchr(const void * src_, int c, size_t size)
{
#if __aarch64__
    return __memrchr_aarch64(src_, c, size);
#elif __arm__
    return __memrchr_arm(src_, c, size);
#else
    return memrchr(src_, c, size);
#endif
}

ALWAYS_INLINE static inline int inline_memcmp(const void * src1_, const void * src2_, size_t size)
{
#if __aarch64__
    return __memcmp_aarch64(src1_, src2_, size);
#if __ARM_FEATURE_SVE
    return __memcmp_aarch64_sve(src1_, src2_, size);
#endif
#else
    return memcmp(src1_, src2_, size);
#endif
}

ALWAYS_INLINE static inline char * inline_strcpy(char * __restrict dst_, const char * __restrict src_)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strcpy_aarch64_sve(dst_, src_);
#endif
    return __strcpy_aarch64(dst_, src_);
#elif __arm__
    return __strcpy_arm(dst_, src_);
#else
    return strcpy(dst_, src_);
#endif
}

ALWAYS_INLINE static inline char * inline_stpcpy(char * __restrict dst_, const char * __restrict src_)
{
#if __aarch64__
    return __stpcpy_aarch64(dst_, src_);
#if __ARM_FEATURE_SVE
    return __stpcpy_aarch64_sve(dst_, src_);
#endif
#else
    return stpcpy(dst_, src_);
#endif
}

ALWAYS_INLINE static inline int inline_strcmp(const char * src1_, const char * src2_)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strcmp_aarch64_sve(src1_, src2_);
#endif
    return __strcmp_aarch64(src1_, src2_);
#elif __arm__
    return __strcmp_arm(src1_, src2_);
#else
    return strcmp(src1_, src2_);
#endif
}

ALWAYS_INLINE static inline char * inline_strchr(const char * src_, int c)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strchr_aarch64_sve(src_, c);
#elif __ARM_FEATURE_MEMORY_TAGGING
    return __strchr_aarch64_mte(src_, c);
#endif
    return __strchr_aarch64(src_, c);
#else
    return strchr(src_, c);
#endif
}

ALWAYS_INLINE static inline char * inline_strrchr(const char * src_, int c)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strrchr_aarch64_sve(src_, c);
#elif __ARM_FEATURE_MEMORY_TAGGING
    return __strrchr_aarch64_mte(src_, c);
#endif
    return __strrchr_aarch64(src_, c);
#else
    return strrchr(src_, c);
#endif
}

ALWAYS_INLINE static inline char * inline_strchrnul(const char * src_, int c)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strchrnul_aarch64_sve(src_, c);
#elif __ARM_FEATURE_MEMORY_TAGGING
    return __strchrnul_aarch64_mte(src_, c);
#endif
    return __strchrnul_aarch64(src_, c);
#else
    return strchrnul(src_, c);
#endif
}

ALWAYS_INLINE static inline size_t inline_strlen(const char * src_)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strlen_aarch64_sve(src_);
#elif __ARM_FEATURE_MEMORY_TAGGING
    return __strlen_aarch64_mte(src_);
#endif
    return __strlen_aarch64(src_);
#else
    return strlen(src_);
#endif
}

ALWAYS_INLINE static inline size_t inline_strnlen(const char * src_, size_t size)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strnlen_aarch64_sve(src_, size);
#endif
    return __strnlen_aarch64(src_, size);
#else
    return strnlen(src_, size);
#endif
}

ALWAYS_INLINE static inline int inline_strncmp(const char * src1_, const char * src2_, size_t size)
{
#if __aarch64__
#if __ARM_FEATURE_SVE
    return __strncmp_aarch64_sve(src1_, src2_, size);
#endif
    return __strncmp_aarch64(src1_, src2_, size);
#else
    return strncmp(src1_, src2_, size);
#endif
}
