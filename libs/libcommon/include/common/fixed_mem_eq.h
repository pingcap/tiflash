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

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace mem_utils
{

FLATTEN_INLINE_PURE
constexpr inline bool memcmp_eq0(const char *, const char *)
{
    return true;
}

FLATTEN_INLINE_PURE
inline bool memcmp_eq1(const char * a, const char * b)
{
    return a[0] == b[0];
}

FLATTEN_INLINE_PURE
inline bool memcmp_eq2(const char * a, const char * b)
{
    const uint16_t ax = *reinterpret_cast<const uint16_t *>(a);
    const uint16_t bx = *reinterpret_cast<const uint16_t *>(b);
    return ax == bx;
}

/*
If use `&&` and cpu failed to predict `cmp -> jne`, the pipeline will be broken.

    movzx   eax, word ptr [rdi]
    cmp     ax, word ptr [rsi]
    jne     ...
    movzx   eax, word ptr [rdi + 1]
    cmp     ax, word ptr [rsi + 1]
    sete    al
    ret

Use `&` to reduce unnecessary branch. Instructions like (1) and (2) are independent(same for (3) and (4)), it's friendly for parallelism.

    movzx   eax, word ptr [rdi]         // (1)
    movzx   ecx, word ptr [rdi + 1]     // (2)
    xor     ax, word ptr [rsi]          // (3)
    xor     cx, word ptr [rsi + 1]      // (4)
    or      cx, ax
    sete    al
    ret
*/
FLATTEN_INLINE_PURE
inline bool memcmp_eq3(const char * a, const char * b)
{
    return memcmp_eq2(a, b) & memcmp_eq2(a + 1, b + 1);
}

FLATTEN_INLINE_PURE
inline bool memcmp_eq4(const char * a, const char * b)
{
    const uint32_t ax = *reinterpret_cast<const uint32_t *>(a);
    const uint32_t bx = *reinterpret_cast<const uint32_t *>(b);
    return ax == bx;
}

FLATTEN_INLINE_PURE
inline bool memcmp_eq8(const char * a, const char * b)
{
    const uint64_t ax = *reinterpret_cast<const uint64_t *>(a);
    const uint64_t bx = *reinterpret_cast<const uint64_t *>(b);
    return ax == bx;
}

// check memory equal of two pointers in fixed size
template <size_t k>
ALWAYS_INLINE inline bool memcmp_eq_fixed_size(const char * a, const char * b)
{
#ifdef M
    static_assert(false, "`M` is defined");
#else
#define M(s)                       \
    if constexpr (k == (s))        \
    {                              \
        return memcmp_eq##s(a, b); \
    }
#endif

    static_assert(k >= 0);

    if constexpr (k >= 16)
    {
        /*
        For x86-64 clang 13.0.0 with options `-O3 -msse4.2`, `std::memcmp(.. , .. , 16)` will be translated to

            movdqu  xmm0, xmmword ptr [rdi]
            movdqu  xmm1, xmmword ptr [rsi]
            pxor    xmm1, xmm0
            ptest   xmm1, xmm1
            sete    al
            ret

        with options `-O3 -mavx2`, it will be

            vmovdqu xmm0, xmmword ptr [rdi]
            vpxor   xmm0, xmm0, xmmword ptr [rsi]
            vptest  xmm0, xmm0
            sete    al
            ret

        */
        return __builtin_memcmp(a, b, k) == 0;
    }
    else if constexpr (k > 8)
    {
        /*
        if use `std::memcmp(.. , .. , 9)`, it will be
        x86-64
            mov     rax, qword ptr [rdi]
            xor     rax, qword ptr [rsi]
            mov     cl, byte ptr [rdi + 8]
            xor     cl, byte ptr [rsi + 8]
            movzx   ecx, cl
            or      rcx, rax
            sete    al
            ret
        
        arm-64 v8
            ldr     x8, [x0]
            ldr     x9, [x1]
            ldrb    w10, [x0, #8]
            ldrb    w11, [x1, #8]
            eor     x8, x8, x9
            eor     w9, w10, w11
            and     x9, x9, #0xff
            orr     x8, x8, x9
            cmp     x8, #0                         
            cset    w0, eq
            ret


        Make operator fetch same size of memory to reduce instructions and get better parallelism.
        
        x86-64
            mov     rax, qword ptr [rdi]
            mov     rcx, qword ptr [rdi + 1]
            xor     rax, qword ptr [rsi]
            xor     rcx, qword ptr [rsi + 1]
            or      rcx, rax
            sete    al
            ret

        arm-64 v8
            ldr     x8, [x0]
            ldr     x9, [x1]
            ldur    x10, [x0, #1]
            ldur    x11, [x1, #1]
            cmp     x8, x9
            cset    w8, eq
            cmp     x10, x11
            cset    w9, eq
            and     w0, w8, w9
            ret        

        */
        return memcmp_eq8(a, b) & memcmp_eq8(a + k - 8, b + k - 8);
    }
    else if constexpr (k > 4)
    {
        if constexpr (k == 8)
            return memcmp_eq8(a, b);
        else
            return memcmp_eq4(a, b) & memcmp_eq4(a + k - 4, b + k - 4);
    }
    else if constexpr (k > 2)
    {
        M(3);
        M(4);
    }
    else
    {
        M(1);
        M(2);
        M(0);
    }
#undef M
}

} // namespace mem_utils
