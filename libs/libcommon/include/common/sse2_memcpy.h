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
#include <emmintrin.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

template <size_t bytes>
ALWAYS_INLINE inline void memcpy_ignore_overlap(char * __restrict dst, const char * __restrict src, size_t size);

template <size_t n>
ALWAYS_INLINE inline void memcpy_block32_ignore_overlap(char * __restrict dst, const char * __restrict src, size_t size);

template <typename T>
ALWAYS_INLINE inline void memcpy_ignore_overlap(char * __restrict dst, const char * __restrict src, size_t size)
{
    assert(size >= sizeof(T));
    auto a = *reinterpret_cast<const T *>(src);
    auto b = *reinterpret_cast<const T *>(src + size - sizeof(T));
    *reinterpret_cast<T *>(dst) = a;
    *reinterpret_cast<T *>(dst + size - sizeof(T)) = b;
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
    auto c0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));
    auto c1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16));
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), c0);
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), c1);
}

ALWAYS_INLINE static inline void sse2_inline_memcpy_small(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);

    assert(size <= 32);

    if unlikely (size <= 1)
    {
        if likely (size == 1)
        {
            /// A single byte.
            *dst = *src;
        }
        /// No bytes remaining.
    }
    else if unlikely (size <= 4) // sse2_inline_memcpy(_,_, 4 ) should use 4 bytes register directly
    {
        /// Chunks of 2..4 bytes.
        memcpy_ignore_overlap<2>(dst, src, size);
    }
    else if unlikely (size <= 8) // sse2_inline_memcpy(_,_, 8 ) should use 8 bytes register directly
    {
        /// Chunks of 4..8 bytes.
        memcpy_ignore_overlap<4>(dst, src, size);
    }
    else if unlikely (size <= 16)
    {
        /// Chunks of 8..16 bytes.
        memcpy_ignore_overlap<8>(dst, src, size);
    }
    else
    {
        /// Chunks of 17..32 bytes.
        memcpy_ignore_overlap<16>(dst, src, size);
    }
}

ALWAYS_INLINE static inline void * sse2_inline_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);

    void * ret = dst;

#define MCP_END(n) tiflash_compiler_builtin_memcpy(dst + size - (n), src + size - (n), (n));
#define MCP(n) tiflash_compiler_builtin_memcpy(dst, src, (n));

    if likely (size <= 32)
    {
        sse2_inline_memcpy_small(dst, src, size);
    }
    else
    {
        if (unlikely(size > 128))
        {
            /// Large size with fully unrolled loop.
            {
                MCP(16);

                // reduce instruction: `offset` = or (`dst`, 0xfffffffffffffff0)
                auto offset = ssize_t(size_t(dst) % 16) - 16;
                dst -= offset;
                src -= offset;
                size += offset;
            }

            /// Aligned unrolled copy.
            __m128i c0, c1, c2, c3, c4, c5, c6, c7;

            while (size >= 128)
            {
                c0 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 0);
                c1 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 1);
                c2 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 2);
                c3 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 3);
                c4 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 4);
                c5 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 5);
                c6 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 6);
                c7 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src) + 7);
                src += 128;
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 0), c0);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 1), c1);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 2), c2);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 3), c3);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 4), c4);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 5), c5);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 6), c6);
                _mm_store_si128((reinterpret_cast<__m128i *>(dst) + 7), c7);
                dst += 128;

                size -= 128;
            }
        }

        // size <= 128

        while (size > 16)
        {
            MCP(16);

            dst += 16;
            src += 16;
            size -= 16;
        }

        // size <= 16
        MCP_END(16);
    }
    return ret;

#undef MCP
#undef MCP_END
}
