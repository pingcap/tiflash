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

#pragma once
#include <common/detect_features.h>
namespace simd_option
{
#if defined(__x86_64__)

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
extern bool ENABLE_AVX;
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
extern bool ENABLE_AVX512;
#endif

#elif defined(__aarch64__)

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
extern bool ENABLE_ASIMD;
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
extern bool ENABLE_SVE;
#endif
#endif

/// @todo: notice that currently we use plain SIMD without OOP abstraction:
///     this gives several issues:
///     - there may be similar code paragraph for each vectorization extension
///     - this forbids passing SIMD type to template argument since GCC will give
///       off warnings on discard attributes
///     - some binary operations are ugly
///     For future improvement, one should wrap SIMD types into structs/classes and
///     https://gcc.gnu.org/onlinedocs/gcc/Vector-Extensions.html also gives a good example
///     to reduce some burden of type-casting.
} // namespace simd_option

#ifdef __linux__
#include <sys/auxv.h>
#ifndef HWCAP2_SVE2
#define HWCAP2_SVE2 (1 << 1)
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

namespace detail
{
#ifdef __aarch64__
static inline bool sve2Supported()
{
    auto hwcaps = getauxval(AT_HWCAP2);
    return (hwcaps & HWCAP2_SVE2) != 0;
}

static inline bool sveSupported()
{
    auto hwcaps = getauxval(AT_HWCAP);
    return (hwcaps & HWCAP_SVE) != 0;
}
#endif
} // namespace detail

#endif

#define TMV_STRINGIFY_IMPL(X) #X
#define TMV_STRINGIFY(X) TMV_STRINGIFY_IMPL(X)

#define TIFLASH_MULTIVERSIONED_VECTORIZATION_X86_64(RETURN, NAME, ARG_LIST, ARG_NAMES, BODY)                                       \
    struct NAME##TiFlashMultiVersion                                                                                               \
    {                                                                                                                              \
        __attribute__((always_inline)) static inline RETURN inlinedImplementation ARG_LIST BODY;                                   \
                                                                                                                                   \
        __attribute__((target("default"))) /* x86-64-v2 is ready on default */                                                     \
        static RETURN dispatchedImplementation ARG_LIST                                                                            \
        {                                                                                                                          \
            return inlinedImplementation ARG_NAMES;                                                                                \
        };                                                                                                                         \
                                                                                                                                   \
        __attribute__((target("avx,avx2,fma,bmi,bmi2"))) /* x86-64-v3 feature flags */                                             \
        static RETURN dispatchedImplementation ARG_LIST                                                                            \
        {                                                                                                                          \
            return inlinedImplementation ARG_NAMES;                                                                                \
        };                                                                                                                         \
                                                                                                                                   \
        __attribute__((target("avx512f,avx512vl,avx512bw,avx512cd,avx512dq,avx,avx2,fma,bmi,bmi2"))) /* x86-64-v4 feature flags */ \
        static RETURN dispatchedImplementation ARG_LIST                                                                            \
        {                                                                                                                          \
            return inlinedImplementation ARG_NAMES;                                                                                \
        };                                                                                                                         \
                                                                                                                                   \
        __attribute__((always_inline)) static inline RETURN invoke ARG_LIST                                                        \
        {                                                                                                                          \
            return dispatchedImplementation ARG_NAMES;                                                                             \
        };                                                                                                                         \
    };

#define TIFLASH_MULTIVERSIONED_VECTORIZATION_AARCH64(RETURN, NAME, ARG_LIST, ARG_NAMES, BODY)    \
    struct NAME##TiFlashMultiVersion                                                             \
    {                                                                                            \
        __attribute__((always_inline)) static inline RETURN inlinedImplementation ARG_LIST BODY; \
                                                                                                 \
        static RETURN genericImplementation ARG_LIST                                             \
        {                                                                                        \
            return inlinedImplementation ARG_NAMES;                                              \
        };                                                                                       \
                                                                                                 \
        __attribute__((target("sve"))) static RETURN sveImplementation ARG_LIST                  \
        {                                                                                        \
            return inlinedImplementation ARG_NAMES;                                              \
        };                                                                                       \
                                                                                                 \
        __attribute__((target("sve2"))) static RETURN sve2Implementation ARG_LIST                \
        {                                                                                        \
            return inlinedImplementation ARG_NAMES;                                              \
        };                                                                                       \
                                                                                                 \
        static RETURN dispatchedImplementation ARG_LIST                                          \
            __attribute__((ifunc(TMV_STRINGIFY(__tiflash_mvec_##NAME##_resolver))));             \
                                                                                                 \
        __attribute__((always_inline)) static inline RETURN invoke ARG_LIST                      \
        {                                                                                        \
            return dispatchedImplementation ARG_NAMES;                                           \
        };                                                                                       \
    };                                                                                           \
    extern "C" void * __tiflash_mvec_##NAME##_resolver()                                         \
    {                                                                                            \
        if (::detail::sveSupported())                                                            \
        {                                                                                        \
            return reinterpret_cast<void *>(&NAME##TiFlashMultiVersion::sveImplementation);      \
        }                                                                                        \
        if (::detail::sve2Supported())                                                           \
        {                                                                                        \
            return reinterpret_cast<void *>(&NAME##TiFlashMultiVersion::sve2Implementation);     \
        }                                                                                        \
        return reinterpret_cast<void *>(&NAME##TiFlashMultiVersion::genericImplementation);      \
    }

#if defined(__linux__) && defined(__aarch64__)
#define TIFLASH_MULTIVERSIONED_VECTORIZATION TIFLASH_MULTIVERSIONED_VECTORIZATION_AARCH64
#elif defined(__linux__) && defined(__x86_64__)
#define TIFLASH_MULTIVERSIONED_VECTORIZATION TIFLASH_MULTIVERSIONED_VECTORIZATION_X86_64
#else
#define TIFLASH_MULTIVERSIONED_VECTORIZATION(RETURN, NAME, ARG_LIST, ARG_NAMES, BODY) \
    struct NAME##TiFlashMultiVersion                                                  \
    {                                                                                 \
        __attribute__((always_inline)) static inline RETURN invoke ARG_LIST BODY;     \
    };
#endif
