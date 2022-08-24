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

#include <common/fixed_mem_eq.h>
#include <immintrin.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace mem_utils::details
{

template <typename T>
ALWAYS_INLINE static inline T clear_rightmost(const T value)
{
    assert(value != 0);

    return value & (value - 1);
}

template <typename T>
ALWAYS_INLINE static inline uint32_t get_rightmost_bit_pos(const T value)
{
    assert(value != 0);
    return __builtin_ctz(value);
}

using Block32 = __m256i;
constexpr int BLOCK32_SIZE = sizeof(Block32);
using Block16 = __m128i;
constexpr int BLOCK16_SIZE = sizeof(Block16);
constexpr auto Block32Mask = std::numeric_limits<uint32_t>::max();
constexpr auto AVX2_UNROLL_NUM = 4;

// `N` is `1U << xxx`
#define OFFSET_ALIGNED(ADDR, N) ((ADDR) % (N)) // (ADDR) & (N-1)
#define ALIGNED_ADDR(ADDR, N) ((ADDR) / (N) * (N)) // (ADDR) & (-N)

FLATTEN_INLINE_PURE static inline bool check_block32_eq(const char * a, const char * b)
{
    auto data = _mm256_xor_si256(
        _mm256_loadu_si256(reinterpret_cast<const Block32 *>(a)),
        _mm256_loadu_si256(reinterpret_cast<const Block32 *>(b)));
    return 0 != _mm256_testz_si256(data, data);
};

FLATTEN_INLINE_PURE static inline bool check_block32x4_eq(const char * a, const char * b)
{
    auto all_ones = _mm256_set1_epi8(0xFF);

    Block32 data = all_ones;
    for (size_t i = 0; i < AVX2_UNROLL_NUM; ++i)
        data = _mm256_and_si256(data, _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const Block32 *>(a + i * BLOCK32_SIZE)), _mm256_loadu_si256(reinterpret_cast<const Block32 *>(b + i * BLOCK32_SIZE))));

    return 0 != _mm256_testc_si256(data, all_ones);
};

// https://github.com/lattera/glibc/blob/master/sysdeps/x86_64/multiarch/memcmp-avx2-movbe.S
FLATTEN_INLINE_PURE static inline bool avx2_mem_equal(const char * p1, const char * p2, size_t n)
{
    constexpr size_t loop_4x32_size = AVX2_UNROLL_NUM * BLOCK32_SIZE;

    // n < 32
    if (unlikely(n < BLOCK32_SIZE))
    {
#ifdef M
        static_assert(false, "`M` is defined");
#else
#define M(x)                                                 \
    case (x):                                                \
    {                                                        \
        return mem_utils::memcmp_eq_fixed_size<(x)>(p1, p2); \
    }
#endif
        switch (n)
        {
            M(0);
            M(1);
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
            // 17~31
            if (!mem_utils::memcmp_eq_fixed_size<BLOCK16_SIZE>(p1, p2))
                return false;
            if (!mem_utils::memcmp_eq_fixed_size<BLOCK16_SIZE>(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE))
                return false;
            return true;
        }
        }
#undef M


#if defined(NORMAL_IF_ELSE_TEST)
        if (unlikely(n < 2))
        {
            // 0~1
            if (n == 1)
                return p1[0] == p2[0];
            return true;
        }
        else if (unlikely(n <= 4))
        {
            // 2~4
            auto a1 = *reinterpret_cast<const uint16_t *>(p1);
            auto b1 = *reinterpret_cast<const uint16_t *>(p2);
            auto a2 = *reinterpret_cast<const uint16_t *>(p1 + n - 2);
            auto b2 = *reinterpret_cast<const uint16_t *>(p2 + n - 2);
            return (a1 == b1) & (a2 == b2);
        }
        else if (unlikely(n <= 8))
        {
            // 5~8
            auto a1 = *reinterpret_cast<const uint32_t *>(p1);
            auto b1 = *reinterpret_cast<const uint32_t *>(p2);
            auto a2 = *reinterpret_cast<const uint32_t *>(p1 + n - 4);
            auto b2 = *reinterpret_cast<const uint32_t *>(p2 + n - 4);
            return (a1 == b1) & (a2 == b2);
        }
        else if (unlikely(n <= 16))
        {
            // 9~16
            auto a1 = *reinterpret_cast<const uint64_t *>(p1);
            auto b1 = *reinterpret_cast<const uint64_t *>(p2);
            auto a2 = *reinterpret_cast<const uint64_t *>(p1 + n - 8);
            auto b2 = *reinterpret_cast<const uint64_t *>(p2 + n - 8);
            return (a1 == b1) & (a2 == b2);
        }
        else
        {
            // 17~31
            if (!memcmp_eq_fixed_size<BLOCK16_SIZE>(p1, p2))
                return false;
            if (!memcmp_eq_fixed_size<BLOCK16_SIZE>(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE))
                return false;
            return true;
        }
#endif
    }

    //  8 * 32 < n
    if (likely(8 * BLOCK32_SIZE < n))
    {
        // check first block
        if (unlikely(!check_block32_eq(p1, p2)))
            return false;
        {
            // align addr of one data pointer
            auto offset = BLOCK32_SIZE - OFFSET_ALIGNED(size_t(p2), BLOCK32_SIZE);
            p1 += offset;
            p2 += offset;
            n -= offset;
        }

        for (; n >= loop_4x32_size;)
        {
            if (unlikely(!check_block32x4_eq(p1, p2)))
                return false;

            n -= loop_4x32_size;
            p1 += loop_4x32_size;
            p2 += loop_4x32_size;
        }
        // n < 4 * 32
    }

    if (unlikely(n <= 2 * BLOCK32_SIZE))
    {
        //  32 < n <= 2 * 32
        if (unlikely(!check_block32_eq(p1, p2)))
            return false;
        return check_block32_eq(p1 + n - BLOCK32_SIZE, p2 + n - BLOCK32_SIZE);
    }
    if (unlikely(n <= 4 * BLOCK32_SIZE))
    {
        //  2 * 32 < n <= 4 * 32
        if (unlikely(!check_block32_eq(p1, p2)))
            return false;
        if (unlikely(!check_block32_eq(p1 + BLOCK32_SIZE, p2 + BLOCK32_SIZE)))
            return false;
        p1 = p1 + n - 2 * BLOCK32_SIZE;
        p2 = p2 + n - 2 * BLOCK32_SIZE;
        if (unlikely(!check_block32_eq(p1, p2)))
            return false;
        return check_block32_eq(p1 + BLOCK32_SIZE, p2 + BLOCK32_SIZE);
    }
    // assert (n <= 8 * BLOCK32_SIZE)
    {
        //  4 * 32 < n <= 8 * 32
        if (unlikely(!check_block32x4_eq(p1, p2)))
            return false;
        return check_block32x4_eq(p1 + n - loop_4x32_size, p2 + n - loop_4x32_size);
    }
}
} // namespace mem_utils::details
