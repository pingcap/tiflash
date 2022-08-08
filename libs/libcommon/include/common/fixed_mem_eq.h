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

#include <common/defines.h>

#include <cstddef>
#include <cstdint>

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

template <size_t k>
ALWAYS_INLINE inline bool memcmp_eq_fixed_size(const char * a, const char * b)
{
#ifdef M
    static_assert(false, "`M` is defined");
#else
#define M(s)                       \
    else if constexpr (k == (s))   \
    {                              \
        return memcmp_eq##s(a, b); \
    }
#endif

    static_assert(k >= 0);
    static_assert(k <= 32);

    if constexpr (k > 16)
    {
        return memcmp_eq_fixed_size<16>(a, b) & memcmp_eq_fixed_size<k - 16>(a + 16, b + 16);
    }
    else if constexpr (k > 8)
    {
        return memcmp_eq8(a, b) & memcmp_eq8(a + k - 8, b + k - 8);
    }
    else if constexpr (k > 4)
    {
        if constexpr (k == 8)
            return memcmp_eq8(a, b);
        else
            return memcmp_eq_fixed_size<4>(a, b) & memcmp_eq_fixed_size<4>(a + k - 4, b + k - 4);
    }
    M(1)
    M(2)
    M(3)
    M(4)
    M(0)
#undef M
}

/*
- with cxx flag `-mavx2`
    - memcmp_eq_fixed_size<32>:
        vmovdqu ymm0, ymmword ptr [rdi]
        vpcmpeqq        ymm0, ymm0, ymmword ptr [rsi]
        vmovmskpd       eax, ymm0
        cmp     al, 15
        sete    al
        vzeroupper
        ret
    - memcmp_eq_fixed_size<16>:
        vmovdqu xmm0, xmmword ptr [rdi]
        vpcmpeqq        xmm0, xmm0, xmmword ptr [rsi]
        vmovmskpd       eax, xmm0
        cmp     al, 3
        sete    al
        ret
*/

} // namespace mem_utils
