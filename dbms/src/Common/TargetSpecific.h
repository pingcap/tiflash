#pragma once
#include <common/defines.h>
#include <common/simd.h>
#include <common/types.h>
#ifdef __x86_64__
#include <immintrin.h>
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
 * on default. However, for aarch64, modern GCC is more favored since the SIMD
 * optimization is much better with more recent compilers.
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

/* Clang shows warning when there aren't any objects to apply pragma.
 * To prevent this warning we define this function inside every macros with pragmas.
 */
#   define TIFLASH_DUMMY_FUNCTION_DEFINITION [[maybe_unused]] void __dummy_function_definition();
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
#   define TIFLASH_DUMMY_FUNCTION_DEFINITION
#endif
// clang-format on

namespace DB::TargetSpecific
{
#define TIFLASH_DECLARE_GENERIC_FUNCTION(RETURN, NAME, ...)               \
    struct _TiflashGenericTarget_##NAME                                   \
    {                                                                     \
        using ReturnType = RETURN;                                        \
        static constexpr size_t WORD_SIZE = 16;                           \
        using SimdWord = ::DB::TargetSpecific::Detail::Generic::Word<16>; \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                 \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__        \
    };
#define TIFLASH_GENERIC_DISPATCH_UNIT(NAME, TYPE) _TiflashGenericTarget_##NAME
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_GENERIC(RETURN, NAME, ...) \
    __attribute__((noinline)) RETURN TIFLASH_GENERIC_DISPATCH_UNIT(NAME, TYPE)::invoke __VA_ARGS__

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#define TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(RETURN, NAME, ...)      \
    TIFLASH_BEGIN_AVX_SPECIFIC_CODE                                   \
    struct _TiflashAVXTarget_##NAME                                   \
    {                                                                 \
        using ReturnType = RETURN;                                    \
        using Checker = ::DB::TargetSpecific::AVXChecker;             \
        static constexpr size_t WORD_SIZE = 32;                       \
        using SimdWord = ::DB::TargetSpecific::Detail::AVX::Word<32>; \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                             \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__    \
    };                                                                \
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
        return simd_option::ENABLE_AVX && __builtin_cpu_supports("avx2");
    }
};
#else
#define TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(...)
#define TIFLASH_AVX_DISPATCH_UNIT(NAME, TYPE) ::DB::TargetSpecific::SkipTarget<TYPE>
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX(...)
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
#define TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(RETURN, NAME, ...)      \
    TIFLASH_BEGIN_AVX512_SPECIFIC_CODE                                   \
    struct _TiflashAVX512Target_##NAME                                   \
    {                                                                    \
        using ReturnType = RETURN;                                       \
        using Checker = ::DB::TargetSpecific::AVX512Checker;             \
        static constexpr size_t WORD_SIZE = 64;                          \
        using SimdWord = ::DB::TargetSpecific::Detail::AVX512::Word<64>; \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                                \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__       \
    };                                                                   \
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
        return simd_option::ENABLE_AVX512
            && __builtin_cpu_supports("avx512f")
            && __builtin_cpu_supports("avx512bw")
            && __builtin_cpu_supports("avx512vl")
            && __builtin_cpu_supports("avx512cd");
    }
};
#else
#define TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(...)
#define TIFLASH_AVX512_DISPATCH_UNIT(NAME, TYPE) ::DB::TargetSpecific::SkipTarget<TYPE>
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX512(...)
#endif

#ifdef __x86_64__
#define TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(RETURN, NAME, ...)      \
    TIFLASH_BEGIN_SSE4_SPECIFIC_CODE                                   \
    struct _TiflashSSE4Target_##NAME                                   \
    {                                                                  \
        using ReturnType = RETURN;                                     \
        using Checker = ::DB::TargetSpecific::SSE4Checker;             \
        static constexpr size_t WORD_SIZE = 16;                        \
        using SimdWord = ::DB::TargetSpecific::Detail::SSE4::Word<16>; \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                              \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__     \
    };                                                                 \
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
        return __builtin_cpu_supports("sse4.2");
    }
};
#else
#define TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(...)
#define TIFLASH_SSE4_DISPATCH_UNIT(NAME, TYPE) ::DB::TargetSpecific::SkipTarget<TYPE>
#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_SSE4(...)
#endif

template <class T>
struct SkipTarget
{
    using ReturnType = T;
    struct Checker
    {
        __attribute__((pure, always_inline)) static bool runtimeSupport()
        {
            return false;
        }
    };
};

template <typename Head, typename... Tail>
struct Dispatch
{
    template <typename... Args>
    __attribute__((always_inline)) static typename Head::ReturnType invoke(Args &&... args)
    {
        if constexpr (!std::is_same_v<Head, SkipTarget<typename Head::ReturnType>>)
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

#define TIFLASH_MULTITARGET_ENTRANCE(RETURN, NAME, ARG_NAMES, ARG_LIST)                                       \
    RETURN NAME ARG_LIST                                                                                      \
    {                                                                                                         \
        return ::DB::TargetSpecific::Dispatch<TIFLASH_AVX512_DISPATCH_UNIT(NAME, RETURN),                     \
                                              TIFLASH_AVX_DISPATCH_UNIT(NAME, RETURN),                        \
                                              TIFLASH_SSE4_DISPATCH_UNIT(NAME, RETURN),                       \
                                              TIFLASH_GENERIC_DISPATCH_UNIT(NAME, RETURN)>::invoke ARG_NAMES; \
    }

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
    TIFLASH_DECLARE_GENERIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)             \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)       \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)        \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)     \
    TIFLASH_MULTITARGET_ENTRANCE(RETURN, NAME, ARG_NAMES, ARG_LIST)

#define TIFLASH_DECLARE_MULTITARGET_FUNCTION_ALONE(RETURN, NAME, ARG_LIST) \
    TIFLASH_DECLARE_GENERIC_FUNCTION(RETURN, NAME, ARG_LIST;)              \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST;)        \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST;)         \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST;)      \
    RETURN NAME ARG_LIST;

#define TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION(RETURN, NAME, ARG_NAMES, ARG_LIST, ...) \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX(RETURN, NAME, ARG_LIST __VA_ARGS__)     \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_AVX512(RETURN, NAME, ARG_LIST __VA_ARGS__)  \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_SSE4(RETURN, NAME, ARG_LIST __VA_ARGS__)    \
    TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION_GENERIC(RETURN, NAME, ARG_LIST __VA_ARGS__) \
    TIFLASH_MULTITARGET_ENTRANCE(RETURN, NAME, ARG_NAMES, ARG_LIST)

namespace Detail
{
template <size_t LENGTH>
struct MarkChecker;

#ifdef __x86_64__
template <>
struct MarkChecker<64>
{
    template <class T>
    __attribute__((always_inline, target("avx512bw"))) static bool isByteAllMarked(T val)
    {
        return _mm512_movepi8_mask(reinterpret_cast<__m512i &>(val.as_int8)) == 0xFFFF'FFFF'FFFF'FFFFu;
    }
};

template <>
struct MarkChecker<32>
{
    template <class T>
    __attribute__((always_inline, target("avx2"))) static bool isByteAllMarked(T val)
    {
        return _mm256_movemask_epi8(reinterpret_cast<__m256i &>(val.as_int8)) == -1; //0xFFFF'FFFF
    }
};
#endif

template <>
struct MarkChecker<16>
{
    template <class T>
    __attribute__((always_inline)) static bool isByteAllMarked(T val)
    {
#ifdef __x86_64__
        return _mm_movemask_epi8(reinterpret_cast<__m128i &>(val.as_int8)) == 0xFFFF;
#else
        return (val.as_uint64[0] & val.as_uint64[1]) == 0xFFFF'FFFF'FFFF'FFFFu;
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


#define DECLARE_TYPE(TYPE_PREFIX) \
    typedef TYPE_PREFIX##_t __attribute__((vector_size(LENGTH))) TYPE_PREFIX##vec_t;

#define ENUM_TYPE(TYPE_PREFIX) \
    TYPE_PREFIX##vec_t as_##TYPE_PREFIX;

#define GET_TYPE(TYPE_PREFIX)                         \
    if constexpr (std::is_same_v<TYPE_PREFIX##_t, T>) \
    {                                                 \
        return as_##TYPE_PREFIX;                      \
    }

TIFLASH_TARGET_SPECIFIC_NAMESPACE(
    template <size_t LENGTH>
    struct Word {
        DECLARE_TYPE(int8);
        DECLARE_TYPE(int16);
        DECLARE_TYPE(int32);
        DECLARE_TYPE(int64);
        DECLARE_TYPE(uint8);
        DECLARE_TYPE(uint16);
        DECLARE_TYPE(uint32);
        DECLARE_TYPE(uint64);

        union
        {
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
        __attribute__((always_inline)) auto & get()
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
        __attribute__((always_inline)) const auto & get() const
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
            return Detail::MarkChecker<LENGTH>::isByteAllMarked(*this);
        }

        __attribute__((always_inline)) static Word from_aligned(const void * src)
        {
            Word result{};
            const auto * address = __builtin_assume_aligned(src, LENGTH);
            __builtin_memcpy(&result, address, LENGTH);
            return result;
        }

        __attribute__((always_inline)) static Word from_unaligned(const void * src)
        {
            Word result{};
            __builtin_memcpy(&result, src, LENGTH);
            return result;
        }

        __attribute__((always_inline)) void to_aligned(void * dst)
        {
            const auto * address = __builtin_assume_aligned(dst, LENGTH);
            __builtin_memcpy(dst, this, LENGTH);
        }

        __attribute__((always_inline)) void to_unaligned(void * dst)
        {
            __builtin_memcpy(dst, this, LENGTH);
        }

        template <class T>
        __attribute__((always_inline)) static Word from_single(T val)
        {
            Word result{};
            for (size_t i = 0; i < LENGTH / sizeof(T); ++i)
            {
                result.template get<T>()[i] = val;
            }
            return result;
        }

        template <class T>
        __attribute__((always_inline)) static Word from_array(const std::array<T, LENGTH / sizeof(T)> & arr)
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