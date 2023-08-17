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

/// __has_feature supported only by clang.
///
/// But libcxx/libcxxabi overrides it to 0,
/// thus the checks for __has_feature will be wrong.
///
/// NOTE:
/// - __has_feature cannot be simply undefined,
///   since this will be broken if some C++ header will be included after
///   including <common/defines.h>
/// - it should not have fallback to 0,
///   since this may create false-positive detection (common problem)
// clang-format off
#if defined(__clang__) && defined(__has_feature)
#    define ch_has_feature __has_feature
#endif

#if defined(_MSC_VER)
#   if !defined(likely)
#      define likely(x)   (x)
#   endif
#   if !defined(unlikely)
#      define unlikely(x) (x)
#   endif
#else
#   if !defined(likely)
#       define likely(x)   (__builtin_expect(!!(x), 1))
#   endif
#   if !defined(unlikely)
#       define unlikely(x) (__builtin_expect(!!(x), 0))
#   endif
#endif

#if defined(_MSC_VER)
#    define ALWAYS_INLINE __forceinline
#    define NO_INLINE static __declspec(noinline)
#    define MAY_ALIAS
#    define FLATTEN_INLINE_PURE
#    define FLATTEN_INLINE
#else
#    define ALWAYS_INLINE __attribute__((__always_inline__))
#    define NO_INLINE __attribute__((__noinline__))
#    define MAY_ALIAS __attribute__((__may_alias__))
#    define FLATTEN_INLINE_PURE __attribute__((flatten, always_inline, pure))
#    define FLATTEN_INLINE __attribute__((flatten, always_inline))
#endif

#if !defined(__x86_64__) && !defined(__aarch64__) && !defined(__PPC__)
#    error "The only supported platforms are x86_64 and AArch64, PowerPC (work in progress)"
#endif

/// Check for presence of address sanitizer
#if !defined(ADDRESS_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(address_sanitizer)
#            define ADDRESS_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_ADDRESS__)
#        define ADDRESS_SANITIZER 1
#    endif
#endif

#if !defined(THREAD_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(thread_sanitizer)
#            define THREAD_SANITIZER 1
#        endif
#    elif defined(__SANITIZE_THREAD__)
#        define THREAD_SANITIZER 1
#    endif
#endif

#if !defined(MEMORY_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(memory_sanitizer)
#            define MEMORY_SANITIZER 1
#        endif
#    elif defined(__MEMORY_SANITIZER__)
#        define MEMORY_SANITIZER 1
#    endif
#endif

#if !defined(UNDEFINED_BEHAVIOR_SANITIZER)
#    if defined(__has_feature)
#        if __has_feature(undefined_behavior_sanitizer)
#            define UNDEFINED_BEHAVIOR_SANITIZER 1
#        endif
#    elif defined(__UNDEFINED_BEHAVIOR_SANITIZER__)
#        define UNDEFINED_BEHAVIOR_SANITIZER 1
#    endif
#endif

#if defined(ADDRESS_SANITIZER)
#    define BOOST_USE_ASAN 1
#    define BOOST_USE_UCONTEXT 1
#endif

#if defined(THREAD_SANITIZER)
#    define BOOST_USE_TSAN 1
#    define BOOST_USE_UCONTEXT 1
#endif

#if defined(ARCADIA_BUILD) && defined(BOOST_USE_UCONTEXT)
#    undef BOOST_USE_UCONTEXT
#endif

/// TODO: Strange enough, there is no way to detect UB sanitizer.

/// Explicitly allow undefined behaviour for certain functions. Use it as a function attribute.
/// It is useful in case when compiler cannot see (and exploit) it, but UBSan can.
/// Example: multiplication of signed integers with possibility of overflow when both sides are from user input.
#if defined(__clang__)
#    define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#    define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#    define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#    define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED __attribute__((__always_inline__, __no_sanitize__("undefined")))
#else  /// It does not work in GCC. GCC 7 cannot recognize this attribute and GCC 8 simply ignores it.
#    define NO_SANITIZE_UNDEFINED
#    define NO_SANITIZE_ADDRESS
#    define NO_SANITIZE_THREAD
#    define ALWAYS_INLINE_NO_SANITIZE_UNDEFINED ALWAYS_INLINE
#endif

#define TIFLASH_TEMPLATE(...) template <__VA_ARGS__>
#define TIFLASH_MACRO_ARGS(...) __VA_ARGS__

#define TIFLASH_MACRO_CONCAT_IMPL(X, Y) X ## Y
/// \name TIFLASH_MACRO_CONCAT
/// \details Concat two language terms (macro expanded).
/// If you concern about why do we need two levels of macros,
/// please check: https://gcc.gnu.org/onlinedocs/cpp/Argument-Prescan.html
/// TL;DR, concatenation operation in macro will forbid pre-scanning
/// and hence stop the macro in the operands being expanded.
#define TIFLASH_MACRO_CONCAT(X, Y) TIFLASH_MACRO_CONCAT_IMPL(X, Y)
// clang-format on

/// A template function for suppressing warnings about unused variables or function results.
template <typename... Args>
constexpr void UNUSED(Args &&... args [[maybe_unused]])
{}

/// \name TIFLASH_NO_OPTIMIZE
/// \tparam T arbitrary type
/// \param var universal variable reference
/// \details stop the compiler from optimizing out a variable; this
/// can be useful in debug or benchmark
/// \example
/// \code{.cpp}
/// for (size_t i = 0; i < loop_times; ++i) {
///     TIFLASH_NO_OPTIMIZE(i);
/// } // the loop will not be optimized out
/// \endcode
/// the code will yield
/// \code{.asm}
///        cmp     w0, 0
///        ble     .L1
///        mov     w1, 0
///.L3:
///        add     w1, w1, 1
///        cmp     w0, w1
///        bne     .L3
///.L1:
///        ret
/// \endcode
template <typename T>
static ALWAYS_INLINE inline void TIFLASH_NO_OPTIMIZE(T && var)
{
    asm volatile("" : : "r,m"(var) : "memory");
}

/*!
 * \def TIFLASH_DUMMY_FUNCTION_DEFINITION
 * Clang shows warning when there aren't any objects to apply pragma.
 * To prevent this warning we define this function inside every macros with pragmas.
 */
#ifdef __clang__
#define TIFLASH_DUMMY_FUNCTION_DEFINITION \
    [[maybe_unused]] void TIFLASH_MACRO_CONCAT(__dummy_function_definition_, __LINE__)();
#ifndef __APPLE__
#define tiflash_compiler_builtin_memcpy \
    __builtin_memcpy_inline // __builtin_memcpy_inline gurantees that compiler will not emit call to libc's memcpy
#else
#define tiflash_compiler_builtin_memcpy __builtin_memcpy
#endif
#else
#define TIFLASH_DUMMY_FUNCTION_DEFINITION
#define tiflash_compiler_builtin_memcpy __builtin_memcpy
#endif

#ifdef __clang__
#define MAYBE_UNUSED_MEMBER [[maybe_unused]]
#else
#define MAYBE_UNUSED_MEMBER
#endif
