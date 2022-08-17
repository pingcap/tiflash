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

#include <common/avx2_mem_utils.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace mem_utils
{

ALWAYS_INLINE static inline uint32_t get_block32_cmp_eq_mask(const Block32 * s,
                                                             Block32 check_block)
{
    /*
    vpcmpeqb  ymm0, ymm0, ymmword ptr [...]
    */
    // `_mm256_loadu_si256` and `_mm256_load_si256` are same in such case
    const auto block = _mm256_loadu_si256(s);
    uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, check_block));
    return mask;
}

template <typename F>
ALWAYS_INLINE static inline const char * avx2_strstr_impl(const char * src, const char target, ssize_t n, F && callback)
{
    assert(n >= 1);

    const char * res = nullptr;
    constexpr auto unroll_num = 4;
    const auto check_block32 = _mm256_set1_epi8(target);

    const auto && fn_check_32_may_exceed = [&]() -> bool {
        auto mask = get_block32_cmp_eq_mask(reinterpret_cast<const Block32 *>(src),
                                            check_block32);
        while (mask)
        {
            auto c = get_rightmost_bit_pos(mask);
            // check boundary
            if (c >= n)
            {
                res = nullptr;
                return true;
            }
            //
            const auto * t = c + src;
            if (callback(t))
            {
                res = t;
                return true;
            }
            mask = clear_rightmost(mask);
        }
        return false;
    };

    const auto && fn_check_32 = [&]() -> bool {
        auto mask = get_block32_cmp_eq_mask(reinterpret_cast<const Block32 *>(src),
                                            check_block32);
        while (mask)
        {
            const auto * t = src + get_rightmost_bit_pos(mask);
            if (callback(t))
            {
                res = t;
                return true;
            }
            mask = clear_rightmost(mask);
        }
        return false;
    };

    const auto && fn_check_32_vec_x4 = [&]() -> bool {
        assert(n >= unroll_num * BLOCK32_SIZE);
        {
            /*
            vpcmpeqb        ymm4, ymm0, ymmword ptr [rcx]
            vpcmpeqb        ymm3, ymm0, ymmword ptr [rcx + 32]
            vpor    ymm5, ymm3, ymm4
            vpcmpeqb        ymm2, ymm0, ymmword ptr [rcx + 64]
            vpcmpeqb        ymm1, ymm0, ymmword ptr [rcx + 96]
            vpor    ymm6, ymm2, ymm1
            vpor    ymm5, ymm5, ymm6
            vpmovmskb       eax, ymm5
            test    eax, eax
            */
            uint32_t data{};
            for (size_t i = 0; i < unroll_num; ++i)
                data |= get_block32_cmp_eq_mask(
                    reinterpret_cast<const Block32 *>(src + BLOCK32_SIZE * i),
                    check_block32);

            if (data)
            {
                // there must be matched mask
            }
            else
            {
                return false;
            }
        }

        for (size_t i = 0; i < unroll_num; ++i)
        {
            const auto * start = src + BLOCK32_SIZE * i;
            auto mask = get_block32_cmp_eq_mask(
                reinterpret_cast<const Block32 *>(start),
                check_block32);
            while (mask)
            {
                auto c = get_rightmost_bit_pos(mask);
                const auto * t = c + start;
                if (callback(t))
                {
                    res = t;
                    return true;
                }
                mask = clear_rightmost(mask);
            }
        }
        return true;
    };

    if (uint8_t rcx = OFFSET_ALIGNED(size_t(src), BLOCK32_SIZE); rcx != 0)
    {
        // align to 32
        src = reinterpret_cast<decltype(src)>(ALIGNED_ADDR(size_t(src), BLOCK32_SIZE));

        auto mask = get_block32_cmp_eq_mask(reinterpret_cast<const Block32 *>(src),
                                            check_block32)
            >> rcx;

        while (mask)
        {
            auto c = get_rightmost_bit_pos(mask);
            if (c >= n)
                return nullptr;
            const auto * t = c + src + rcx;
            if (callback(t))
                return t;
            mask = clear_rightmost(mask);
        }

        if ((n -= BLOCK32_SIZE - rcx) <= 0)
        {
            return nullptr;
        }

        src += BLOCK32_SIZE;
    }

    assert(size_t(src) % BLOCK32_SIZE == 0);

    for (; n >= unroll_num * BLOCK32_SIZE; src += unroll_num * BLOCK32_SIZE, n -= unroll_num * BLOCK32_SIZE)
    {
        if (fn_check_32_vec_x4())
            return res;
    }

    assert(n < unroll_num * BLOCK32_SIZE);

    for (; n >= BLOCK32_SIZE; n -= BLOCK32_SIZE, src += BLOCK32_SIZE)
    {
        if (fn_check_32())
            return res;
    }

    if (n == 0)
        return nullptr;

    fn_check_32_may_exceed();
    return res;
}

inline const char * avx2_strstr_impl_genetic(const char * src, size_t n, const char * needle, size_t k)
{
    return avx2_strstr_impl(src, needle[0], n - k + 1, [&](const char * s) -> bool {
        return avx2_mem_equal(s, needle, k);
    });
}

ALWAYS_INLINE static inline const char * avx2_strstr_impl(const char * src, size_t n, const char * needle, size_t k)
{
#ifdef M
    static_assert(false, "`M` is defined");
#else
#define M(x)                                                                               \
    case (x):                                                                              \
    {                                                                                      \
        return avx2_strstr_impl(src, needle[0], n - (x) + 1, [&](const char * s) -> bool { \
            return mem_utils::memcmp_eq_fixed_size<x>(s, needle);                          \
        });                                                                                \
    }
#endif

    if (n < k)
    {
        return nullptr;
    }

    switch (k)
    {
    case 0:
    {
        return src;
    }
    case 1:
    {
        return avx2_strstr_impl(
            src,
            needle[0],
            n,
            [&](const char *) constexpr {
                return true;
            });
    }
        M(2);
        M(3);
        M(4);
        M(5);
        M(6);
        M(7);
        M(8);
        M(9);
        M(10);
        M(11);
        M(12);
        M(13);
        M(14);
        M(15);
        M(16);
    default:
    {
        return avx2_strstr_impl_genetic(src, n, needle, k);
    }
    }
#undef M
}

ALWAYS_INLINE static inline size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k)
{
    const auto * p = avx2_strstr_impl(src, n, needle, k);
    return p ? p - src : std::string_view::npos;
}
ALWAYS_INLINE static inline size_t avx2_strstr(std::string_view src, std::string_view needle)
{
    return avx2_strstr(src.data(), src.size(), needle.data(), needle.size());
}
} // namespace mem_utils
