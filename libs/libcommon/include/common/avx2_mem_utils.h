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

#include <common/fixed_mem_eq.h>
#include <immintrin.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
#define MEM_UTILS_FUNC_NO_SANITIZE [[maybe_unused]] static NO_INLINE NO_SANITIZE_ADDRESS NO_SANITIZE_THREAD
#endif

namespace mem_utils::details
{

// remove tail `1`: `111000` -> `110000`
template <typename T>
ALWAYS_INLINE static inline T clear_rightmost_bit_one(const T value)
{
    assert(value != 0);
    // recommended to use compile flag `-mbmi` under AMD64 platform
    return value & (value - 1);
}

// get count of tail `0` bit, same as get index of rightmost bit `1`
// - `11000` -> `3` means the right most `1` is at index `3`
ALWAYS_INLINE static inline uint32_t rightmost_bit_one_index(const uint32_t value)
{
    assert(value != 0);
    return __builtin_ctz(value);
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
template <typename S>
FLATTEN_INLINE static inline void write(void * tar, const S & src)
{
    *reinterpret_cast<S *>(tar) = src;
}
template <bool aligned = false, bool non_temporal = false>
FLATTEN_INLINE_PURE static inline Block32 load_block32(const void * p)
{
    if constexpr (aligned)
    {
        if constexpr (non_temporal)
            return _mm256_stream_load_si256(reinterpret_cast<const Block32 *>(p));
        else
            return _mm256_load_si256(reinterpret_cast<const Block32 *>(p));
    }
    else
        return _mm256_loadu_si256(reinterpret_cast<const Block32 *>(p));
}
FLATTEN_INLINE_PURE static inline Block16 load_block16(const void * p)
{
    return _mm_loadu_si128(reinterpret_cast<const Block16 *>(p));
}
template <bool aligned = false>
FLATTEN_INLINE static inline void write_block16(void * p, const Block16 & src)
{
    if constexpr (aligned)
        _mm_store_si128(reinterpret_cast<Block16 *>(p), src);
    else
        _mm_storeu_si128(reinterpret_cast<Block16 *>(p), src);
}
template <bool aligned = false, bool non_temporal = false>
FLATTEN_INLINE static inline void write_block32(void * p, const Block32 & src)
{
    if constexpr (aligned)
    {
        if constexpr (non_temporal)
            _mm256_stream_si256(reinterpret_cast<Block32 *>(p), src);
        else
            _mm256_store_si256(reinterpret_cast<Block32 *>(p), src);
    }
    else
        _mm256_storeu_si256(reinterpret_cast<Block32 *>(p), src);
}
FLATTEN_INLINE_PURE static inline uint32_t get_block32_cmp_eq_mask(const void * p1, const void * p2)
{
    uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(load_block32(p1), load_block32(p2)));
    return mask;
}
FLATTEN_INLINE_PURE static inline uint32_t get_block32_cmp_eq_mask(const void * s, const Block32 & check_block)
{
    const auto block = load_block32(s);
    uint32_t mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, check_block));
    return mask;
}
FLATTEN_INLINE_PURE static inline uint32_t get_block16_cmp_eq_mask(const void * p1, const void * p2)
{
    uint32_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(load_block16(p1), load_block16(p2)));
    return mask;
}
FLATTEN_INLINE_PURE static inline uint32_t get_block16_cmp_eq_mask(const void * s, const Block16 & check_block)
{
    const auto block = load_block16(s);
    uint32_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(block, check_block));
    return mask;
}
FLATTEN_INLINE_PURE static inline bool check_block32_eq(const char * a, const char * b)
{
    auto data = _mm256_xor_si256(load_block32(a), load_block32(b));
    return 0 != _mm256_testz_si256(data, data);
}
FLATTEN_INLINE_PURE static inline int cmp_block1(const void * p1, const void * p2)
{
    return int32_t(read<uint8_t>(p1)) - int32_t(read<uint8_t>(p2));
}

FLATTEN_INLINE_PURE static inline int cmp_block16(const char * p1, const char * p2)
{
    uint32_t mask = get_block16_cmp_eq_mask(p1, p2); // mask is up to 0xffff
    mask -= Block16Mask;
    if (unlikely(mask != 0))
    {
        auto pos = rightmost_bit_one_index(mask);
        int ret = cmp_block1(p1 + pos, p2 + pos);
        if (ret == 0)
        {
            __builtin_unreachable();
        }
        else
        {
            return ret;
        }
    }
    return 0;
}

template <bool must_not_eq = false>
FLATTEN_INLINE_PURE static inline int cmp_block32(const char * p1, const char * p2)
{
    uint32_t mask = get_block32_cmp_eq_mask(p1, p2); // mask is up to 0xffffffff
    mask -= Block32Mask;
    if (must_not_eq || unlikely(mask != 0))
    {
        auto pos = rightmost_bit_one_index(mask);
        int ret = cmp_block1(p1 + pos, p2 + pos);
        if (ret == 0)
        {
            __builtin_unreachable();
        }
        else
        {
            return ret;
        }
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
            data = _mm256_and_si256(
                data,
                _mm256_cmpeq_epi8(load_block32(a + i * BLOCK32_SIZE), load_block32(b + i * BLOCK32_SIZE)));
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
    if (likely(check_block32x4_eq(a, b)))
        return 0;
    for (size_t i = 0; i < (AVX2_UNROLL_NUM - 1); ++i)
    {
        if (auto ret = cmp_block32(a + i * BLOCK32_SIZE, (b + i * BLOCK32_SIZE)); unlikely(ret))
            return ret;
    }
    return cmp_block32<true>(a + (AVX2_UNROLL_NUM - 1) * BLOCK32_SIZE, (b + (AVX2_UNROLL_NUM - 1) * BLOCK32_SIZE));
}

// ref: https://github.com/lattera/glibc/blob/master/sysdeps/x86_64/multiarch/memcmp-avx2-movbe.S
FLATTEN_INLINE_PURE static inline int avx2_mem_cmp(const char * p1, const char * p2, size_t n) noexcept
{
    constexpr size_t loop_block32x4_size = AVX2_UNROLL_NUM * BLOCK32_SIZE;

    // n <= 32
    if (likely(n <= BLOCK32_SIZE))
    {
#if !defined(AVX2_MEM_CMP_NORMAL_IF_ELSE)

#ifdef M
        static_assert(false, "`M` is defined");
#else
#define M(x)                                  \
    case (x):                                 \
    {                                         \
        return __builtin_memcmp(p1, p2, (x)); \
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
            if (auto ret = cmp_block16(p1, p2); ret)
                return ret;
            return cmp_block16(p1 + n - BLOCK16_SIZE, p2 + n - BLOCK16_SIZE);
        }
        }
#undef M

#else
        // an optional way to check small str
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
#endif
    }
    //  8 * 32 < n
    if (unlikely(8 * BLOCK32_SIZE < n))
    {
        // check first block

        if (auto ret = cmp_block32(p1, p2); unlikely(ret))
            return ret;
        {
            // align addr of one data pointer
            auto offset = ssize_t(OFFSET_FROM_ALIGNED(size_t(p2), BLOCK32_SIZE)) - BLOCK32_SIZE;
            p1 -= offset;
            p2 -= offset;
            n += offset;
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

FLATTEN_INLINE_PURE static inline bool avx2_mem_equal(const char * p1, const char * p2, size_t n) noexcept
{
    constexpr size_t loop_block32x4_size = AVX2_UNROLL_NUM * BLOCK32_SIZE;

    // n <= 32
    if (likely(n <= BLOCK32_SIZE))
    {
#if !defined(AVX2_MEM_EQ_NORMAL_IF_ELSE)

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

#else
        // an optional way to check small str
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
            auto offset = ssize_t(OFFSET_FROM_ALIGNED(size_t(p2), BLOCK32_SIZE)) - BLOCK32_SIZE;
            p1 -= offset;
            p2 -= offset;
            n += offset;
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

template <size_t bytes>
ALWAYS_INLINE inline void memcpy_ignore_overlap(char * __restrict dst, const char * __restrict src, size_t size);

template <size_t n>
ALWAYS_INLINE inline void memcpy_block32_ignore_overlap(
    char * __restrict dst,
    const char * __restrict src,
    size_t size);

template <typename T>
ALWAYS_INLINE inline void memcpy_ignore_overlap(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= sizeof(T));
    auto a = mem_utils::details::read<T>(src);
    auto b = mem_utils::details::read<T>(src + size - sizeof(T));
    mem_utils::details::write(dst, a);
    mem_utils::details::write(dst + size - sizeof(T), b);
}

template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<2>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 2 && size <= 4);
    using T = uint16_t;
    static_assert(sizeof(T) == 2);
    memcpy_ignore_overlap<T>(dst, src, size);
}
template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<4>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 4 && size <= 8);
    using T = uint32_t;
    static_assert(sizeof(T) == 4);
    memcpy_ignore_overlap<T>(dst, src, size);
}
template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<8>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 8 && size <= 16);
    using T = uint64_t;
    static_assert(sizeof(T) == 8);
    memcpy_ignore_overlap<T>(dst, src, size);
}
template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<16>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 16 && size <= 32);
    auto c0 = mem_utils::details::load_block16(src);
    auto c1 = mem_utils::details::load_block16(src + size - 16);
    mem_utils::details::write_block16(dst, c0);
    mem_utils::details::write_block16(dst + size - 16, c1);
}
#define LOAD_HEAD(n) auto c_head_##n = mem_utils::details::load_block32(src + BLOCK32_SIZE * (n));
#define WRITE_HEAD(n) mem_utils::details::write_block32(dst + BLOCK32_SIZE * (n), c_head_##n);
#define LOAD_END(n) auto c_end_##n = mem_utils::details::load_block32(src + size - BLOCK32_SIZE * ((n) + 1));
#define WRITE_END(n) mem_utils::details::write_block32(dst + size - BLOCK32_SIZE * ((n) + 1), c_end_##n);

template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<32>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 32 && size <= 64);
    LOAD_HEAD(0)
    LOAD_END(0)
    WRITE_HEAD(0)
    WRITE_END(0)
}
template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<64>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 64 && size <= 128);
    LOAD_HEAD(0)
    LOAD_HEAD(1)
    LOAD_END(1)
    LOAD_END(0)
    WRITE_HEAD(0)
    WRITE_HEAD(1)
    WRITE_END(1)
    WRITE_END(0)
}
template <>
ALWAYS_INLINE inline void memcpy_ignore_overlap<128>(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= 128 && size <= 256);
    LOAD_HEAD(0)
    LOAD_HEAD(1)
    LOAD_HEAD(2)
    LOAD_HEAD(3)
    LOAD_END(3)
    LOAD_END(2)
    LOAD_END(1)
    LOAD_END(0)
    WRITE_HEAD(0)
    WRITE_HEAD(1)
    WRITE_HEAD(2)
    WRITE_HEAD(3)
    WRITE_END(3)
    WRITE_END(2)
    WRITE_END(1)
    WRITE_END(0)
}
#undef LOAD_HEAD
#undef WRITE_HEAD
#undef LOAD_END
#undef WRITE_END

} // namespace mem_utils::details
