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

// remove tail `1`: `111000` -> `110000`
template <typename T>
ALWAYS_INLINE static inline T clear_rightmost_bit_one(const T value)
{
    assert(value != 0);

    return value & (value - 1);
}

// get count of tail `0` bit, same as get index of rightmost bit `1`
// - `11000` -> `3` means the right most `1` is at index `3`
ALWAYS_INLINE static inline uint32_t rightmost_bit_one_index(const uint32_t value)
{
    assert(value != 0);
    return _tzcnt_u32(value);
}

using Block32 = __m256i;
constexpr int BLOCK32_SIZE = sizeof(Block32);
using Block16 = __m128i;
constexpr int BLOCK16_SIZE = sizeof(Block16);
constexpr auto Block32Mask = std::numeric_limits<uint32_t>::max();
constexpr uint32_t Block16Mask = std::numeric_limits<uint16_t>::max();
constexpr auto AVX2_UNROLL_NUM = 4;

// `N` is `1U << xxx`
#define OFFSET_FROM_ALIGNED(ADDR, N) ((ADDR) % (N)) // (ADDR) & (N-1)
#define ALIGNED_ADDR(ADDR, N) ((ADDR) / (N) * (N)) // (ADDR) & (-N)

template <typename T, typename S = T>
FLATTEN_INLINE_PURE static inline T read(const void * data)
{
    T val = *reinterpret_cast<const S *>(data);
    return val;
}
FLATTEN_INLINE_PURE static inline Block32 load_block32(const void * p)
{
    return _mm256_loadu_si256(reinterpret_cast<const Block32 *>(p));
}
FLATTEN_INLINE_PURE static inline Block16 load_block16(const void * p)
{
    return _mm_loadu_si128(reinterpret_cast<const Block16 *>(p));
}
FLATTEN_INLINE_PURE static inline uint32_t get_block32_cmp_eq_mask(const void * p1, const void * p2)
{
    uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(load_block32(p1), load_block32(p2)));
    return mask;
}
FLATTEN_INLINE_PURE static inline uint32_t get_block16_cmp_eq_mask(const void * p1, const void * p2)
{
    uint32_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(load_block16(p1), load_block16(p2)));
    return mask;
}
FLATTEN_INLINE_PURE static inline bool check_block32_eq(const char * a, const char * b)
{
    auto data = _mm256_xor_si256(
        load_block32(a),
        load_block32(b));
    return 0 != _mm256_testz_si256(data, data);
}
FLATTEN_INLINE_PURE static inline int cmp_block1(const void * p1, const void * p2)
{
    return int32_t(read<uint8_t>(p1)) - int32_t(read<uint8_t>(p2));
}

FLATTEN_INLINE_PURE static inline int cmp_block8(const void * p1, const void * p2)
{
    // the left most bit may be 1, use std::memcmp(,,8) to use `sbb`
    /*
        bswap   rcx
        bswap   rdx
        xor     eax, eax
        cmp     rcx, rdx
        seta    al
        sbb     eax, 0
    */
    return std::memcmp(p1, p2, 8);
}

FLATTEN_INLINE_PURE static inline int cmp_block16(const char * p1, const char * p2)
{
    uint32_t mask = get_block16_cmp_eq_mask(p1, p2); // mask is up to 0xffff
    mask -= Block16Mask;
    if (unlikely(mask != 0))
    {
        auto pos = rightmost_bit_one_index(mask);
        return cmp_block1(p1 + pos, p2 + pos);
    }
    return 0;
}
FLATTEN_INLINE_PURE static inline int cmp_block32(const char * p1, const char * p2)
{
    uint32_t mask = get_block32_cmp_eq_mask(p1, p2); // mask is up to 0xffffffff
    mask -= Block32Mask;
    if (unlikely(mask != 0))
    {
        auto pos = rightmost_bit_one_index(mask);
        return cmp_block1(p1 + pos, p2 + pos);
    }
    return 0;
}

template <bool use_vptest_instr = false>
FLATTEN_INLINE_PURE static inline bool check_block32x4_eq(const char * a, const char * b)
{
    if constexpr (use_vptest_instr)
    {
        auto all_ones = _mm256_set1_epi8(0xFF);
        Block32 data = all_ones;
        for (size_t i = 0; i < AVX2_UNROLL_NUM; ++i)
            data = _mm256_and_si256(data, _mm256_cmpeq_epi8(load_block32(a + i * BLOCK32_SIZE), load_block32(b + i * BLOCK32_SIZE)));
        return 0 != _mm256_testc_si256(data, all_ones);
    }
    else
    {
        uint32_t mask = Block32Mask;
        for (size_t i = 0; i < AVX2_UNROLL_NUM; ++i)
            mask &= get_block32_cmp_eq_mask(a + i * BLOCK32_SIZE, b + i * BLOCK32_SIZE);
        return mask == Block32Mask;
    }
}

FLATTEN_INLINE_PURE static inline int cmp_block32x4(const char * a, const char * b)
{
    if (check_block32x4_eq(a, b))
        return 0;
    for (size_t i = 0; i < AVX2_UNROLL_NUM - 1; ++i)
    {
        if (auto ret = cmp_block32(a + i * BLOCK32_SIZE, (b + i * BLOCK32_SIZE)); ret)
            return ret;
    }
    return cmp_block32(a + (AVX2_UNROLL_NUM - 1) * BLOCK32_SIZE, (b + (AVX2_UNROLL_NUM - 1) * BLOCK32_SIZE));
}
FLATTEN_INLINE_PURE static inline uint32_t swap_u32(uint32_t val)
{
    return __builtin_bswap32(val);
}
FLATTEN_INLINE_PURE static inline uint64_t swap_u64(uint64_t val)
{
    return __builtin_bswap64(val);
}

[[maybe_unused]] FLATTEN_INLINE_PURE static inline uint32_t read_u32_swap(const void * data)
{
    return swap_u32(read<uint32_t>(data));
}

[[maybe_unused]] FLATTEN_INLINE_PURE static inline uint64_t read_u64_swap(const void * data)
{
    return swap_u64(read<uint64_t>(data));
}

// ref: https://github.com/lattera/glibc/blob/master/sysdeps/x86_64/multiarch/memcmp-avx2-movbe.S
FLATTEN_INLINE_PURE static inline int avx2_mem_cmp(const char * p1, const char * p2, size_t n)
{
    constexpr size_t loop_block32x4_size = AVX2_UNROLL_NUM * BLOCK32_SIZE;

    // n <= 32
    if (likely(n <= BLOCK32_SIZE))
    {
        if (unlikely(n < 2))
        {
            // 0~1
            if (n == 1)
                return cmp_block1(p1, p2);
            return 0;
        }
        else if (unlikely(n < 4))
        {
            // 2~3
            using T = uint16_t;

            // load 2 bytes to one int32: [0, 0, p1, p0]
            // shift left one byte: [0, p1, p0, 0]
            // reverse swap: [0, p0, p1, 0]
            // add one byte from high addr: [0, p0, p1, p2]
            // the left most bit is always 0, it's safe to use subtraction directly
            int32_t a = read<T>(p1);
            int32_t b = read<T>(p2);
            a <<= sizeof(uint8_t) * 8;
            b <<= sizeof(uint8_t) * 8;
            a = swap_u32(a);
            b = swap_u32(b);
            a |= read<uint8_t>(p1 + n - sizeof(uint8_t));
            b |= read<uint8_t>(p2 + n - sizeof(uint8_t));

            return a - b;
        }
        else if (unlikely(n <= 8))
        {
            // 4~8
            using T = uint32_t;

            // load high 4 bytes to one uint64: [0, 0, 0, 0, pn, p_n-1, p_n-2, p_n-3]
            // shift left 4 byte: [pn, p_n-1, p_n-2, p_n-3, 0, 0, 0, 0]
            // add low 4 bytes from high addr: [pn, p_n-1, p_n-2, p_n-3, p3, p2, p1, p0]
            uint64_t a = read<T>(p1 + n - sizeof(T));
            uint64_t b = read<T>(p2 + n - sizeof(T));
            a <<= sizeof(T) * 8;
            b <<= sizeof(T) * 8;
            a |= read<T>(p1);
            b |= read<T>(p2);

            return cmp_block8(&a, &b);
        }
        else if (likely(n <= 16))
        {
            // 9~16
            if (auto ret = cmp_block8(p1, p2); ret)
                return ret;
            return cmp_block8(p1 + n - 8, p2 + n - 8);
        }
        else
        {
            // 17~32
            if (auto ret = cmp_block16(p1, p2); ret)
                return ret;
            return cmp_block16(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE);
        }
    }
    //  8 * 32 < n
    if (unlikely(8 * BLOCK32_SIZE < n))
    {
        // check first block

        if (auto ret = cmp_block32(p1, p2); unlikely(ret))
            return ret;
        {
            // align addr of one data pointer
            auto offset = BLOCK32_SIZE - OFFSET_FROM_ALIGNED(size_t(p2), BLOCK32_SIZE);
            p1 += offset;
            p2 += offset;
            n -= offset;
        }

        for (; n >= loop_block32x4_size;)
        {
            if (auto ret = cmp_block32x4(p1, p2); unlikely(ret))
                return ret;

            n -= loop_block32x4_size;
            p1 += loop_block32x4_size;
            p2 += loop_block32x4_size;
        }
        // n < 4 * 32
        if (unlikely(n <= BLOCK32_SIZE))
        {
            // n <= 32
            return cmp_block32(p1 + n - BLOCK32_SIZE, p2 + n - BLOCK32_SIZE);
        }
        // 32 < n < 4 * 32
    }

    assert(BLOCK32_SIZE < n);

    if (unlikely(n <= 2 * BLOCK32_SIZE))
    {
        //  32 < n <= 2 * 32
        if (auto ret = cmp_block32(p1, p2); unlikely(ret))
            return ret;
        return cmp_block32(p1 + n - BLOCK32_SIZE, p2 + n - BLOCK32_SIZE);
    }
    if (unlikely(n <= 4 * BLOCK32_SIZE))
    {
        //  2 * 32 < n <= 4 * 32
        if (auto ret = cmp_block32(p1, p2); unlikely(ret))
            return ret;
        if (auto ret = cmp_block32(p1 + BLOCK32_SIZE, p2 + BLOCK32_SIZE); unlikely(ret))
            return ret;
        p1 = p1 + n - 2 * BLOCK32_SIZE;
        p2 = p2 + n - 2 * BLOCK32_SIZE;
        if (auto ret = cmp_block32(p1, p2); unlikely(ret))
            return ret;
        return cmp_block32(p1 + BLOCK32_SIZE, p2 + BLOCK32_SIZE);
    }

    assert(n <= 8 * BLOCK32_SIZE);

    {
        //  4 * 32 < n <= 8 * 32
        if (auto ret = cmp_block32x4(p1, p2); unlikely(ret))
            return ret;
        return cmp_block32x4(p1 + n - loop_block32x4_size, p2 + n - loop_block32x4_size);
    }
}

FLATTEN_INLINE_PURE static inline bool avx2_mem_equal(const char * p1, const char * p2, size_t n)
{
    constexpr size_t loop_block32x4_size = AVX2_UNROLL_NUM * BLOCK32_SIZE;

    // n <= 32
    if (likely(n <= BLOCK32_SIZE))
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
            // 17~32
            if (!mem_utils::memcmp_eq_fixed_size<BLOCK16_SIZE>(p1, p2))
                return false;
            if (!mem_utils::memcmp_eq_fixed_size<BLOCK16_SIZE>(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE))
                return false;
            return true;
        }
        }
#undef M

// an optional way to check small str
#if defined(AVX2_MEM_EQ_NORMAL_IF_ELSE)
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
            // 17~32
            if (!memcmp_eq_fixed_size<BLOCK16_SIZE>(p1, p2))
                return false;
            if (!memcmp_eq_fixed_size<BLOCK16_SIZE>(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE))
                return false;
            return true;
        }
#endif
    }

    //  8 * 32 < n
    if (unlikely(8 * BLOCK32_SIZE < n))
    {
        // check first block
        if (unlikely(!check_block32_eq(p1, p2)))
            return false;
        {
            // align addr of one data pointer
            auto offset = BLOCK32_SIZE - OFFSET_FROM_ALIGNED(size_t(p2), BLOCK32_SIZE);
            p1 += offset;
            p2 += offset;
            n -= offset;
        }

        for (; n >= loop_block32x4_size;)
        {
            if (unlikely(!check_block32x4_eq(p1, p2)))
                return false;

            n -= loop_block32x4_size;
            p1 += loop_block32x4_size;
            p2 += loop_block32x4_size;
        }
        // n < 4 * 32
        if (unlikely(n <= BLOCK32_SIZE))
        {
            // n <= 32
            return check_block32_eq(p1 + n - BLOCK32_SIZE, p2 + n - BLOCK32_SIZE);
        }
        // 32 < n < 4 * 32
    }

    assert(BLOCK32_SIZE < n);

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

    assert(n <= 8 * BLOCK32_SIZE);

    {
        //  4 * 32 < n <= 8 * 32
        if (unlikely(!check_block32x4_eq(p1, p2)))
            return false;
        return check_block32x4_eq(p1 + n - loop_block32x4_size, p2 + n - loop_block32x4_size);
    }
}
} // namespace mem_utils::details
