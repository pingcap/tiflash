// Copyright 2023 PingCAP, Inc.
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
#include <common/defines.h>
#include <common/simd.h>
#include <common/types.h>
#ifdef __x86_64__
#include <immintrin.h>
#else
#include <arm_neon.h>
#endif
/*
 * The following code is largely inspired by
 * https://github.com/ClickHouse/ClickHouse/blob/ff0d3860d4eb742607cdb7857dc823f6de1105d2/src/Functions/TargetSpecific.h
 *
 * One can use the macro tools to hint the compiler generate code for
 * different targets; this is particularly useful for SIMD Vectorization.
 *
 * Notice that we do not dispatch functions for aarch64 targets.
 * The reason is that by default, GCC will enable `lse` and `simd` flags
 * So we do not need to dispatch them in specific pragma scopes.
 *
 * P.S. aarch64
 * vectorization quality is largely affected by the compiler version. For instance,
 * GCC-7 may not do some good vectorization where GCC-11 can do good job.
 * Please check the generated code with
 * target compilers and consider writing the code with `SimdWord` to hint the compiler
 * more eagerly if aarch64 performance is important for your piece of code.
 *
 * One should consider use one of the following macros in the header:
 * 1. TIFLASH_DECLARE_MULTITARGET_FUNCTION: declare and implement the functions in one shot.
 * 2. TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP: declare and implement the functions in one shot (with template parameters).
 * 3. TIFLASH_DECLARE_MULTITARGET_FUNCTION_ALONE: declare the function without template parameter
 * 4. TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION: implement the function body
 */

// clang-format off
#if defined(__clang__)
#   define TIFLASH_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512cd\"))),apply_to=function)")
#   define TIFLASH_BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2\"))),apply_to=function)")
#   define TIFLASH_BEGIN_SSE4_SPECIFIC_CODE \
        _Pragma("clang attribute push(__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt\"))),apply_to=function)")
#   define TIFLASH_END_TARGET_SPECIFIC_CODE \
        _Pragma("clang attribute pop")
#else
#   define TIFLASH_BEGIN_AVX512_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,avx512f,avx512bw,avx512vl,avx512cd,tune=native\")")
#   define TIFLASH_BEGIN_AVX_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native\")")
#   define TIFLASH_BEGIN_SSE4_SPECIFIC_CODE \
        _Pragma("GCC push_options") \
        _Pragma("GCC target(\"sse,sse2,sse3,ssse3,sse4,popcnt,tune=native\")")
#   define TIFLASH_END_TARGET_SPECIFIC_CODE \
        _Pragma("GCC pop_options")
#endif
// clang-format on

namespace DB::TargetSpecific
{
struct SkipTarget
{
};
#define TIFLASH_TARGET_SPECIFIC_DEFINE_SKIP_TARGET(RETURN, NAME, ARCH, ...)                     \
    __VA_ARGS__                                                                                 \
    struct _TiflashSkip_##ARCH##_Target_##NAME : public ::DB::TargetSpecific::SkipTarget        \
    {                                                                                           \
        using ReturnType = RETURN;                                                              \
        struct Checker                                                                          \
        {                                                                                       \
            __attribute__((pure, always_inline)) static bool runtimeSupport() { return false; } \
        };                                                                                      \
    };

#define TIFLASH_DECLARE_GENERIC_FUNCTION(TPARMS, RETURN, NAME, ...)                           \
    TPARMS                                                                                    \
    struct _TiflashGenericTarget_##NAME                                                       \
    {                                                                                         \
        using ReturnType = RETURN;                                                            \
        static constexpr size_t WORD_SIZE = ::DB::TargetSpecific::Detail::Generic::WORD_SIZE; \
        using SimdWord = ::DB::TargetSpecific::Detail::Generic::Word<WORD_SIZE>;              \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                                     \
        static __attribute__((noinline)) ReturnType invoke __VA_ARGS__                        \
    };
#define TIFLASH_GENERIC_DISPATCH_UNIT(NAME, TYPE) _TiflashGenericTarget_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_GENERIC(RETURN, NAME, ...) \
    __attribute__((noinline)) RETURN TIFLASH_GENERIC_DISPATCH_UNIT(NAME, TYPE)::invoke __VA_ARGS__

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#define TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...)                  \
    TIFLASH_BEGIN_AVX_SPECIFIC_CODE                                                       \
    TPARMS                                                                                \
    struct _TiflashAVXTarget_##NAME                                                       \
    {                                                                                     \
        using ReturnType = RETURN;                                                        \
        using Checker = ::DB::TargetSpecific::AVXChecker;                                 \
        static constexpr size_t WORD_SIZE = ::DB::TargetSpecific::Detail::AVX::WORD_SIZE; \
        using SimdWord = ::DB::TargetSpecific::Detail::AVX::Word<WORD_SIZE>;              \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                                 \
        static __attribute__((noinline)) ReturnType invoke __VA_ARGS__                    \
    };                                                                                    \
    TIFLASH_END_TARGET_SPECIFIC_CODE

#define TIFLASH_AVX_DISPATCH_UNIT(NAME, TYPE) _TiflashAVXTarget_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX(RETURN, NAME, ...)                           \
    TIFLASH_BEGIN_AVX_SPECIFIC_CODE                                                             \
    __attribute__((noinline)) RETURN TIFLASH_AVX_DISPATCH_UNIT(NAME, TYPE)::invoke __VA_ARGS__; \
    TIFLASH_END_TARGET_SPECIFIC_CODE
// today, most applicable targets support AVX2 and AVX in the same time, we hence coalesce the cases.
struct AVXChecker
{
    __attribute__((pure, always_inline)) static bool runtimeSupport()
    {
        return simd_option::ENABLE_AVX && common::cpu_feature_flags.avx2;
    }
};
#else
#define TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...) \
    TIFLASH_TARGET_SPECIFIC_DEFINE_SKIP_TARGET(RETURN, NAME, AVX, TPARMS)
#define TIFLASH_AVX_DISPATCH_UNIT(NAME, TYPE) _TiflashSkip_AVX_Target_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX(...)
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
#define TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...)                  \
    TIFLASH_BEGIN_AVX512_SPECIFIC_CODE                                                       \
    TPARMS                                                                                   \
    struct _TiflashAVX512Target_##NAME                                                       \
    {                                                                                        \
        using ReturnType = RETURN;                                                           \
        using Checker = ::DB::TargetSpecific::AVX512Checker;                                 \
        static constexpr size_t WORD_SIZE = ::DB::TargetSpecific::Detail::AVX512::WORD_SIZE; \
        using SimdWord = ::DB::TargetSpecific::Detail::AVX512::Word<WORD_SIZE>;              \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                                    \
        static __attribute__((noinline)) ReturnType invoke __VA_ARGS__                       \
    };                                                                                       \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#define TIFLASH_AVX512_DISPATCH_UNIT(NAME, TYPE) _TiflashAVX512Target_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX512(RETURN, NAME, ...)                           \
    TIFLASH_BEGIN_AVX512_SPECIFIC_CODE                                                             \
    __attribute__((noinline)) RETURN TIFLASH_AVX512_DISPATCH_UNIT(NAME, TYPE)::invoke __VA_ARGS__; \
    TIFLASH_END_TARGET_SPECIFIC_CODE
// again, it is not worthy to be too specific about targets, there will be too many cases. let us only
// enable this only for targets that is modern enough.
struct AVX512Checker
{
    __attribute__((pure, always_inline)) static bool runtimeSupport()
    {
        using namespace common;
        return simd_option::ENABLE_AVX512 && common::cpu_feature_flags.avx512f && common::cpu_feature_flags.avx512bw
            && common::cpu_feature_flags.avx512vl && common::cpu_feature_flags.avx512cd;
    }
};
#else
#define TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...) \
    TIFLASH_TARGET_SPECIFIC_DEFINE_SKIP_TARGET(RETURN, NAME, AVX512, TPARMS)
#define TIFLASH_AVX512_DISPATCH_UNIT(NAME, TYPE) _TiflashSkip_AVX512_Target_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX512(...)
#endif

#ifdef __x86_64__
#define TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...)                  \
    TIFLASH_BEGIN_SSE4_SPECIFIC_CODE                                                       \
    TPARMS                                                                                 \
    struct _TiflashSSE4Target_##NAME                                                       \
    {                                                                                      \
        using ReturnType = RETURN;                                                         \
        using Checker = ::DB::TargetSpecific::SSE4Checker;                                 \
        static constexpr size_t WORD_SIZE = ::DB::TargetSpecific::Detail::SSE4::WORD_SIZE; \
        using SimdWord = ::DB::TargetSpecific::Detail::SSE4::Word<WORD_SIZE>;              \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                                  \
        static __attribute__((noinline)) ReturnType invoke __VA_ARGS__                     \
    };                                                                                     \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#define TIFLASH_SSE4_DISPATCH_UNIT(NAME, TYPE) _TiflashSSE4Target_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_SSE4(RETURN, NAME, ...)                           \
    TIFLASH_BEGIN_SSE4_SPECIFIC_CODE                                                             \
    __attribute__((noinline)) RETURN TIFLASH_SSE4_DISPATCH_UNIT(NAME, TYPE)::invoke __VA_ARGS__; \
    TIFLASH_END_TARGET_SPECIFIC_CODE

// again, it is not worthy to be too specific about targets, there will be too many cases. let us only
// enable this for targets that is modern enable
struct SSE4Checker
{
    __attribute__((pure, always_inline)) static bool runtimeSupport()
    {
        return common::cpu_feature_flags.sse4_1 && common::cpu_feature_flags.sse4_2;
    }
};
#else
#define TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(TPARMS, RETURN, NAME, ...) \
    TIFLASH_TARGET_SPECIFIC_DEFINE_SKIP_TARGET(RETURN, NAME, SSE4, TPARMS)
#define TIFLASH_SSE4_DISPATCH_UNIT(NAME, TYPE) _TiflashSkip_SSE4_Target_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_SSE4(...)
#endif


template <typename Head, typename... Tail>
struct Dispatch
{
    template <typename... Args>
    __attribute__((always_inline)) static typename Head::ReturnType invoke(Args &&... args)
    {
        if constexpr (!std::is_base_of_v<SkipTarget, Head>)
        {
            if (Head::Checker::runtimeSupport())
            {
                return Head::invoke(std::forward<Args>(args)...);
            }
        }
        return Dispatch<Tail...>::invoke(std::forward<Args>(args)...);
    }
};

template <typename Last>
struct Dispatch<Last>
{
    template <typename... Args>
    __attribute__((always_inline)) static typename Last::ReturnType invoke(Args &&... args)
    {
        return Last::invoke(std::forward<Args>(args)...);
    }
};

#define TIFLASH_MULTITARGET_DISPATCH(RETURN, NAME, ARG_NAMES, ARG_LIST)     \
    RETURN NAME ARG_LIST                                                    \
    {                                                                       \
        return ::DB::TargetSpecific::Dispatch<                              \
            TIFLASH_AVX512_DISPATCH_UNIT(NAME, RETURN),                     \
            TIFLASH_AVX_DISPATCH_UNIT(NAME, RETURN),                        \
            TIFLASH_SSE4_DISPATCH_UNIT(NAME, RETURN),                       \
            TIFLASH_GENERIC_DISPATCH_UNIT(NAME, RETURN)>::invoke ARG_NAMES; \
    }

// clang-format off
#define TIFLASH_MULTITARGET_DISPATCH_TP(TPARMS, TARGS, RETURN, NAME, ARG_NAMES, ARG_LIST)          \
    TPARMS RETURN NAME ARG_LIST                                                                    \
    {                                                                                              \
        return ::DB::TargetSpecific::Dispatch<TIFLASH_AVX512_DISPATCH_UNIT(NAME, RETURN) <TARGS>,  \
               TIFLASH_AVX_DISPATCH_UNIT(NAME, RETURN)<TARGS>,                                     \
               TIFLASH_SSE4_DISPATCH_UNIT(NAME, RETURN)<TARGS>,                                    \
               TIFLASH_GENERIC_DISPATCH_UNIT(NAME, RETURN)<TARGS> > ::invoke ARG_NAMES;            \
    }
// clang-format on
/// TIFLASH_DECLARE_MULTITARGET_FUNCTION
/// \example
/// One can use the macro in the following way:
/// \code{.cpp}
/// TIFLASH_DECLARE_MULTITARGET_FUNCTION(
///     int,
///     plus,
///     (a, b),
///     (int a, int b),
///     {
///         return a + b;
///     })
/// \endcode
/// Then, we can use the function as normal, but the target is automatically dispatched:
/// \code{.cpp}
/// int plus(int a, int b);
/// \endcode
#define TIFLASH_DECLARE_MULTITARGET_FUNCTION(RETURN, NAME, ARG_NAMES, ARG_LIST, ...) \
    TIFLASH_DECLARE_GENERIC_FUNCTION(, RETURN, NAME, ARG_LIST __VA_ARGS__)           \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST __VA_ARGS__)     \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST __VA_ARGS__)      \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST __VA_ARGS__)   \
    TIFLASH_MULTITARGET_DISPATCH(RETURN, NAME, ARG_NAMES, ARG_LIST)

#define TIFLASH_DECLARE_MULTITARGET_FUNCTION_ALONE(RETURN, NAME, ARG_LIST) \
    TIFLASH_DECLARE_GENERIC_FUNCTION(, RETURN, NAME, ARG_LIST;)            \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST;)      \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST;)       \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(, RETURN, NAME, ARG_LIST;)    \
    RETURN NAME ARG_LIST;

#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION(RETURN, NAME, ARG_NAMES, ARG_LIST, ...) \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX(RETURN, NAME, ARG_LIST __VA_ARGS__)     \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX512(RETURN, NAME, ARG_LIST __VA_ARGS__)  \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_SSE4(RETURN, NAME, ARG_LIST __VA_ARGS__)    \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_GENERIC(RETURN, NAME, ARG_LIST __VA_ARGS__) \
    TIFLASH_MULTITARGET_DISPATCH(RETURN, NAME, ARG_NAMES, ARG_LIST)

/// \example
/// \code{.cpp}
/// TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP(
///    (char flip_case_mask),
///    (flip_case_mask),
///    void,
///    upperCaseUtf8Array,
///    (src, src_end, dst),
///    (const UInt8 * src, const UInt8 * src_end, UInt8 * dst),
///    { .... }
/// )
/// \endcode

#define TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP(TPARMS, TARGS, RETURN, NAME, ARG_NAMES, ARG_LIST, ...)    \
    TIFLASH_DECLARE_GENERIC_FUNCTION(TIFLASH_TEMPLATE TPARMS, RETURN, NAME, ARG_LIST __VA_ARGS__)         \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(TIFLASH_TEMPLATE TPARMS, RETURN, NAME, ARG_LIST __VA_ARGS__)   \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(TIFLASH_TEMPLATE TPARMS, RETURN, NAME, ARG_LIST __VA_ARGS__)    \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(TIFLASH_TEMPLATE TPARMS, RETURN, NAME, ARG_LIST __VA_ARGS__) \
    TIFLASH_MULTITARGET_DISPATCH_TP(                                                                      \
        TIFLASH_TEMPLATE TPARMS,                                                                          \
        TIFLASH_MACRO_ARGS TARGS,                                                                         \
        RETURN,                                                                                           \
        NAME,                                                                                             \
        ARG_NAMES,                                                                                        \
        ARG_LIST)

namespace Detail
{
template <size_t LENGTH>
struct SimdImpl;

namespace Generic
{
#ifdef __x86_64__
using InternalType = __m128i;
using AddressType = InternalType *;
using ConstAddressType = InternalType const *;
#else
using InternalType = uint8x16_t;
using AddressType = uint8_t *;
using ConstAddressType = uint8_t const *;
#endif
static inline constexpr size_t WORD_SIZE = sizeof(InternalType);
} // namespace Generic

#ifdef __x86_64__

namespace AVX
{
using InternalType = __m256i;
using AddressType = InternalType *;
using ConstAddressType = InternalType const *;
static inline constexpr size_t WORD_SIZE = sizeof(InternalType);
} // namespace AVX

namespace AVX512
{
using InternalType = __m512i;
using AddressType = InternalType *;
using ConstAddressType = InternalType const *;
static inline constexpr size_t WORD_SIZE = sizeof(InternalType);
} // namespace AVX512

namespace SSE4
{
using InternalType = Generic::InternalType;
using AddressType = InternalType *;
using ConstAddressType = InternalType const *;
static inline constexpr size_t WORD_SIZE = sizeof(InternalType);
} // namespace SSE4

template <>
struct SimdImpl<AVX512::WORD_SIZE>
{
    using InternalType = AVX512::InternalType;
    using AddressType = AVX512::AddressType;
    using ConstAddressType = AVX512::ConstAddressType;

    __attribute__((always_inline, target("avx512bw"))) static bool isByteAllMarked(InternalType val)
    {
        return _mm512_movepi8_mask(val) == 0xFFFF'FFFF'FFFF'FFFFu;
    }

    __attribute__((always_inline, target("avx512f"))) static InternalType fromAligned(const void * val)
    {
        return _mm512_load_si512(reinterpret_cast<ConstAddressType>(val));
    }

    __attribute__((always_inline, target("avx512f"))) static InternalType fromUnaligned(const void * val)
    {
        return _mm512_loadu_si512(reinterpret_cast<ConstAddressType>(val));
    }

    __attribute__((always_inline, target("avx512f"))) static void toAligned(InternalType x, void * val)
    {
        _mm512_store_si512(reinterpret_cast<AddressType>(val), x);
    }

    __attribute__((always_inline, target("avx512f"))) static void toUnaligned(InternalType x, void * val)
    {
        _mm512_storeu_si512(reinterpret_cast<AddressType>(val), x);
    }
};

template <>
struct SimdImpl<AVX::WORD_SIZE>
{
    using InternalType = AVX::InternalType;
    using AddressType = AVX::AddressType;
    using ConstAddressType = AVX::ConstAddressType;

    __attribute__((always_inline, target("avx2"))) static bool isByteAllMarked(InternalType val)
    {
        return static_cast<unsigned>(_mm256_movemask_epi8(val)) == 0xFFFF'FFFF; //0xFFFF'FFFF
    }

    __attribute__((always_inline, target("avx2"))) static InternalType fromAligned(const void * val)
    {
        return _mm256_load_si256(reinterpret_cast<ConstAddressType>(val));
    }

    __attribute__((always_inline, target("avx2"))) static InternalType fromUnaligned(const void * val)
    {
        return _mm256_loadu_si256(reinterpret_cast<ConstAddressType>(val));
    }

    __attribute__((always_inline, target("avx2"))) static void toAligned(InternalType x, void * val)
    {
        _mm256_store_si256(reinterpret_cast<AddressType>(val), x);
    }

    __attribute__((always_inline, target("avx2"))) static void toUnaligned(InternalType x, void * val)
    {
        _mm256_storeu_si256(reinterpret_cast<AddressType>(val), x);
    }
};
#endif

template <>
struct SimdImpl<Generic::WORD_SIZE>
{
    using InternalType = Generic::InternalType;
    using AddressType = Generic::AddressType;
    using ConstAddressType = Generic::ConstAddressType;

    __attribute__((always_inline)) static bool isByteAllMarked(InternalType val)
    {
#ifdef __x86_64__
        return _mm_movemask_epi8(val) == 0xFFFF;
#else
        auto coerced_value = vreinterpretq_u64_u8(val);
        return (coerced_value[0] & coerced_value[1]) == 0xFFFF'FFFF'FFFF'FFFFu;
#endif
    }

    template <class T>
    __attribute__((always_inline)) static InternalType fromAligned(const void * val)
    {
#ifdef __x86_64__
        return _mm_load_si128(reinterpret_cast<ConstAddressType>(val));
#else
        return vld1q_u8(reinterpret_cast<ConstAddressType>(val));
#endif
    }

    __attribute__((always_inline)) static InternalType fromUnaligned(const void * val)
    {
#ifdef __x86_64__
        return _mm_loadu_si128(reinterpret_cast<ConstAddressType>(val));
#else
        return vld1q_u8(reinterpret_cast<ConstAddressType>(val));
#endif
    }

    __attribute__((always_inline)) static void toAligned(InternalType x, void * val)
    {
#ifdef __x86_64__
        _mm_store_si128(reinterpret_cast<AddressType>(val), x);
#else
        vst1q_u8(reinterpret_cast<AddressType>(val), x);
#endif
    }

    __attribute__((always_inline)) static void toUnaligned(InternalType x, void * val)
    {
#ifdef __x86_64__
        _mm_storeu_si128(reinterpret_cast<AddressType>(val), x);
#else
        vst1q_u8(reinterpret_cast<AddressType>(val), x);
#endif
    }
};
} // namespace Detail

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#define TIFLASH_AVX_NAMESPACE(...)            \
    TIFLASH_BEGIN_AVX_SPECIFIC_CODE           \
    namespace DB::TargetSpecific::Detail::AVX \
    {                                         \
    __VA_ARGS__                               \
    }                                         \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#else
#define TIFLASH_AVX_NAMESPACE(...)
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
#define TIFLASH_AVX512_NAMESPACE(...)            \
    TIFLASH_BEGIN_AVX512_SPECIFIC_CODE           \
    namespace DB::TargetSpecific::Detail::AVX512 \
    {                                            \
    __VA_ARGS__                                  \
    }                                            \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#else
#define TIFLASH_AVX512_NAMESPACE(...)
#endif

#ifdef __x86_64__
#define TIFLASH_SSE4_NAMESPACE(...)            \
    TIFLASH_BEGIN_SSE4_SPECIFIC_CODE           \
    namespace DB::TargetSpecific::Detail::SSE4 \
    {                                          \
    __VA_ARGS__                                \
    }                                          \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#else
#define TIFLASH_SSE4_NAMESPACE(...)
#endif

#define TIFLASH_GENERIC_NAMESPACE(...)            \
    namespace DB::TargetSpecific::Detail::Generic \
    {                                             \
    __VA_ARGS__                                   \
    }

#define TIFLASH_TARGET_SPECIFIC_NAMESPACE(...) \
    TIFLASH_AVX_NAMESPACE(__VA_ARGS__)         \
    TIFLASH_AVX512_NAMESPACE(__VA_ARGS__)      \
    TIFLASH_SSE4_NAMESPACE(__VA_ARGS__)        \
    TIFLASH_GENERIC_NAMESPACE(__VA_ARGS__)
} // namespace DB::TargetSpecific


#define DECLARE_TYPE(TYPE_PREFIX)                                                         \
    struct TYPE_PREFIX##_wrapper                                                          \
    {                                                                                     \
        typedef TYPE_PREFIX##_t __attribute__((vector_size(LENGTH), __may_alias__)) type; \
    };                                                                                    \
    using TYPE_PREFIX##vec_t = typename TYPE_PREFIX##_wrapper::type;

#define ENUM_TYPE(TYPE_PREFIX) TYPE_PREFIX##vec_t as_##TYPE_PREFIX;

#define GET_TYPE(TYPE_PREFIX)                         \
    if constexpr (std::is_same_v<TYPE_PREFIX##_t, T>) \
    {                                                 \
        return as_##TYPE_PREFIX;                      \
    }

/// inlining function requires the scope must be within target options, therefore, we need to declare Word
/// within pragma scope
TIFLASH_TARGET_SPECIFIC_NAMESPACE(template <size_t LENGTH> struct Word {
    DECLARE_TYPE(int8);
    DECLARE_TYPE(int16);
    DECLARE_TYPE(int32);
    DECLARE_TYPE(int64);
    DECLARE_TYPE(uint8);
    DECLARE_TYPE(uint16);
    DECLARE_TYPE(uint32);
    DECLARE_TYPE(uint64);

    template <typename First, typename Second>
    struct TypePair
    {
        using FirstType = First;
        using SecondType = Second;
    };

    template <typename Key, typename Head, typename... Tail>
    struct TypeMatch
    {
        using MatchedType = std::conditional_t<
            std::is_same_v<Key, typename Head::FirstType>,
            typename Head::SecondType,
            typename TypeMatch<Key, Tail...>::MatchedType>;
    };

    template <typename Key, typename Head>
    struct TypeMatch<Key, Head>
    {
        using MatchedType = typename Head::SecondType;
    };

    template <typename Key>
    using MatchedVectorType = typename TypeMatch<
        Key,
        TypePair<int8_t, int8_wrapper>,
        TypePair<int16_t, int16_wrapper>,
        TypePair<int32_t, int32_wrapper>,
        TypePair<int64_t, int64_wrapper>,
        TypePair<uint8_t, uint8_wrapper>,
        TypePair<uint16_t, uint16_wrapper>,
        TypePair<uint32_t, uint32_wrapper>,
        TypePair<uint64_t, uint64_wrapper>>::MatchedType::type;

    union
    {
        typename Detail::SimdImpl<LENGTH>::InternalType as_internal;
        ENUM_TYPE(int8);
        ENUM_TYPE(int16);
        ENUM_TYPE(int32);
        ENUM_TYPE(int64);
        ENUM_TYPE(uint8);
        ENUM_TYPE(uint16);
        ENUM_TYPE(uint32);
        ENUM_TYPE(uint64);
    };

    template <class T>
    __attribute__((always_inline)) MatchedVectorType<T> & get()
    {
        GET_TYPE(int8);
        GET_TYPE(int16);
        GET_TYPE(int32);
        GET_TYPE(int64);
        GET_TYPE(uint8);
        GET_TYPE(uint16);
        GET_TYPE(uint32);
        GET_TYPE(uint64);
        __builtin_unreachable();
    }

    template <class T>
    __attribute__((always_inline)) const MatchedVectorType<T> & get() const
    {
        GET_TYPE(int8);
        GET_TYPE(int16);
        GET_TYPE(int32);
        GET_TYPE(int64);
        GET_TYPE(uint8);
        GET_TYPE(uint16);
        GET_TYPE(uint32);
        GET_TYPE(uint64);
        __builtin_unreachable();
    }

    [[nodiscard]] __attribute__((always_inline)) bool isByteAllMarked() const
    {
        return Detail::SimdImpl<LENGTH>::isByteAllMarked(as_internal);
    }

    __attribute__((always_inline)) static Word fromAligned(const void * src)
    {
        Word result{};
        result.as_internal = Detail::SimdImpl<LENGTH>::fromAligned(src);
        return result;
    }

    __attribute__((always_inline)) static Word fromUnaligned(const void * src)
    {
        Word result{};
        result.as_internal = Detail::SimdImpl<LENGTH>::fromUnaligned(src);
        return result;
    }

    __attribute__((always_inline)) void toAligned(void * dst)
    {
        return Detail::SimdImpl<LENGTH>::toAligned(as_internal, dst);
    }

    __attribute__((always_inline)) void toUnaligned(void * dst)
    {
        return Detail::SimdImpl<LENGTH>::toUnaligned(as_internal, dst);
    }

    template <class T>
    __attribute__((always_inline)) static Word fromSingle(T val)
    {
        Word result{};
        for (size_t i = 0; i < LENGTH / sizeof(T); ++i)
        {
            result.template get<T>()[i] = val;
        }
        return result;
    }

    template <class T>
    __attribute__((always_inline)) static Word fromArray(const std::array<T, LENGTH / sizeof(T)> & arr)
    {
        Word result{};
        for (size_t i = 0; i < LENGTH / sizeof(T); ++i)
        {
            result.template get<T>()[i] = arr[i];
        }
        return result;
    }
};)

#undef DECLARE_TYPE
#undef ENUM_TYPE
#undef GET_TYPE
