#pragma once
#include <common/defines.h>
#include <common/simd.h>
#include <common/types.h>
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
#define TIFLASH_DECLARE_GENERIC_FUNCTION(RETURN, NAME, ...)        \
    struct _TiflashGeneric_##NAME                                  \
    {                                                              \
        using ReturnType = RETURN;                                 \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                          \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__ \
    };

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#define TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(RETURN, NAME, ...)   \
    TIFLASH_BEGIN_AVX_SPECIFIC_CODE                                \
    struct _TiflashAVXTarget_##NAME                                \
    {                                                              \
        using ReturnType = RETURN;                                 \
        using Checker = ::DB::TargetSpecific::AVXChecker;          \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                          \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__ \
    };                                                             \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#define TIFLASH_AVX_DISPATCH_UNIT(NAME, TYPE) _TiflashAVXTarget_##NAME
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
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
#define TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(RETURN, NAME, ...) \
    TIFLASH_BEGIN_AVX512_SPECIFIC_CODE                              \
    struct _TiflashAVX512Target_##NAME                              \
    {                                                               \
        using ReturnType = RETURN;                                  \
        using Checker = ::DB::TargetSpecific::AVX512Checker;        \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                           \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__  \
    };                                                              \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#define TIFLASH_AVX512_DISPATCH_UNIT(NAME, TYPE) _TiflashAVX512Target_##NAME

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
#endif

#ifdef __x86_64__
#define TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(RETURN, NAME, ...)  \
    TIFLASH_BEGIN_SSE4_SPECIFIC_CODE                               \
    struct _TiflashSSE4Target_##NAME                               \
    {                                                              \
        using ReturnType = RETURN;                                 \
        using Checker = ::DB::TargetSpecific::SSE4Checker;         \
        TIFLASH_DUMMY_FUNCTION_DEFINITION                          \
        static __attribute__((noinline)) RETURN invoke __VA_ARGS__ \
    };                                                             \
    TIFLASH_END_TARGET_SPECIFIC_CODE
#define TIFLASH_SSE4_DISPATCH_UNIT(NAME, TYPE) _TiflashSSE4Target_##NAME
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

/// TIFLASH_MULTITARGET_ENTRANCE
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
#define TIFLASH_MULTITARGET_ENTRANCE(RETURN, NAME, ARG_NAMES, ARG_LIST)                   \
    RETURN NAME ARG_LIST                                                                  \
    {                                                                                     \
        return ::DB::TargetSpecific::Dispatch<TIFLASH_AVX512_DISPATCH_UNIT(NAME, RETURN), \
                                              TIFLASH_AVX_DISPATCH_UNIT(NAME, RETURN),    \
                                              TIFLASH_SSE4_DISPATCH_UNIT(NAME, RETURN),   \
                                              _TiflashGeneric_##NAME>::invoke ARG_NAMES;  \
    }


#define TIFLASH_DECLARE_MULTITARGET_FUNCTION(RETURN, NAME, ARG_NAMES, ARG_LIST, ...) \
    TIFLASH_DECLARE_GENERIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)             \
    TIFLASH_DECLARE_SSE4_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)       \
    TIFLASH_DECLARE_AVX_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)        \
    TIFLASH_DECLARE_AVX512_SPECIFIC_FUNCTION(RETURN, NAME, ARG_LIST __VA_ARGS__)     \
    TIFLASH_MULTITARGET_ENTRANCE(RETURN, NAME, ARG_NAMES, ARG_LIST)


} // namespace DB::TargetSpecific
