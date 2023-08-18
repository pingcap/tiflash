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

#include <common/avx2_mem_utils.h>

namespace mem_utils::details
{

template <typename F>
ALWAYS_INLINE static inline bool check_aligned_block32_may_exceed(
    const char * src,
    ssize_t n,
    const char *& res,
    const Block32 & check_block,
    F && callback)
{
    auto mask = get_block32_cmp_eq_mask(src, check_block);
    for (; mask;)
    {
        auto c = rightmost_bit_one_index(mask);
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
        mask = clear_rightmost_bit_one(mask);
    }
    return false;
}

template <typename F>
ALWAYS_INLINE static inline bool check_block32x1(
    const char * src,
    const char *& res,
    const Block32 & check_block,
    F && callback)
{
    auto mask = get_block32_cmp_eq_mask(src, check_block);
    for (; mask;)
    {
        const auto * t = src + rightmost_bit_one_index(mask);
        if (callback(t))
        {
            res = t;
            return true;
        }
        mask = clear_rightmost_bit_one(mask);
    }
    return false;
}

template <typename F>
ALWAYS_INLINE static inline bool check_block32x4(
    const char * src,
    const char *& res,
    const Block32 & check_block,
    F && callback)
{
    {
        uint32_t data{};
        for (size_t i = 0; i < AVX2_UNROLL_NUM; ++i)
            data |= get_block32_cmp_eq_mask(src + BLOCK32_SIZE * i, check_block);

        if (data)
        {
            // there must be matched mask
        }
        else
        {
            return false;
        }
    }

    for (size_t i = 0; i < AVX2_UNROLL_NUM; ++i)
    {
        const auto * start = src + BLOCK32_SIZE * i;
        auto mask = get_block32_cmp_eq_mask(start, check_block);
        for (; mask;)
        {
            auto c = rightmost_bit_one_index(mask);
            const auto * t = c + start;
            if (callback(t))
            {
                res = t;
                return true;
            }
            mask = clear_rightmost_bit_one(mask);
        }
    }
    return false;
};

template <typename F>
ALWAYS_INLINE static inline const char * avx2_strstr_impl(const char * src, const char target, ssize_t n, F && callback)
{
    assert(n >= 1);

    const char * res = nullptr;
    const auto check_block32 = _mm256_set1_epi8(target);

    // align address to 32 for better performance
    // memory allocator will always alloc memory aligned to `Page Size`(usually 4K, one Block `512B` at least) from system
    // if there is valid data at address S, then it is safe to visit address [ALIGN_TO_PAGE_SIZE(S), ALIGN_TO_PAGE_SIZE(S)+PAGE_SIZE).
    if (uint8_t offset = OFFSET_FROM_ALIGNED(size_t(src), BLOCK32_SIZE); offset != 0)
    {
        // align to 32
        src = reinterpret_cast<decltype(src)>(ALIGNED_ADDR(size_t(src), BLOCK32_SIZE));

        // load block 32 from new aligned address may cause false positives when using `AddressSanitizer` because asan will provide a malloc()/free() alternative and detect memory visitation.
        // generally it's safe to visit address which won't cross page boundary.

        // right shift offset to remove useless mask bit
        auto mask = get_block32_cmp_eq_mask(src, check_block32) >> offset;

        for (; mask;)
        {
            auto c = rightmost_bit_one_index(mask);
            if (c >= n)
                return nullptr;
            const auto * t = c + src + offset; // add offset
            if (callback(t))
                return t;
            mask = clear_rightmost_bit_one(mask);
        }

        n -= BLOCK32_SIZE - offset;

        if (n <= 0)
        {
            return nullptr;
        }

        src += BLOCK32_SIZE;
    }

    assert(size_t(src) % BLOCK32_SIZE == 0);

    for (; (n >= AVX2_UNROLL_NUM * BLOCK32_SIZE);)
    {
        if (check_block32x4(src, res, check_block32, callback))
            return res;
        src += AVX2_UNROLL_NUM * BLOCK32_SIZE, n -= AVX2_UNROLL_NUM * BLOCK32_SIZE;
    }

    assert(n < AVX2_UNROLL_NUM * BLOCK32_SIZE);

    for (; (n >= BLOCK32_SIZE);)
    {
        if (check_block32x1(src, res, check_block32, callback))
            return res;
        n -= BLOCK32_SIZE, src += BLOCK32_SIZE;
    }

    if (unlikely(n == 0))
        return nullptr;

    check_aligned_block32_may_exceed(src, n, res, check_block32, callback);
    return res;
}

inline const char * avx2_strstr_impl_generic(const char * src, size_t n, const char * needle, size_t k)
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

    if (unlikely(n < k))
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
            [&](const char *) constexpr { return true; });
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
        return avx2_strstr_impl_generic(src, n, needle, k);
    }
    }
#undef M
}

#if defined(MEM_UTILS_FUNC_NO_SANITIZE)
MEM_UTILS_FUNC_NO_SANITIZE
#else
ALWAYS_INLINE static inline
#endif
size_t avx2_strstr(const char * src, size_t n, const char * needle, size_t k)
{
    const auto * p = avx2_strstr_impl(src, n, needle, k);
    return p ? p - src : std::string_view::npos;
}
ALWAYS_INLINE static inline size_t avx2_strstr(std::string_view src, std::string_view needle)
{
    return avx2_strstr(src.data(), src.size(), needle.data(), needle.size());
}

#if defined(MEM_UTILS_FUNC_NO_SANITIZE)
MEM_UTILS_FUNC_NO_SANITIZE
#else
ALWAYS_INLINE static inline
#endif
const char * avx2_memchr(const char * src, size_t n, char target)
{
    if (unlikely(n < 1))
    {
        return nullptr;
    }
    return avx2_strstr_impl(
        src,
        target,
        n,
        [&](const char *) constexpr { return true; });
}
} // namespace mem_utils::details
