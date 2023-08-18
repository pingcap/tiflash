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
#include <emmintrin.h>

#include <cstddef>
#include <cstdint>

// Custom inline memcpy implementation for TiFlash.
// - it is recommended to use for inline function with `sse2` supported
// - it perform better than `legacy::inline_memcpy`(from clickhouse) according to `libs/libcommon/src/tests/bench_memcpy.cpp`
// - like `std::memcpy`, the behavior is undefined when the source and the destination objects overlap
// - moving data from register to memory costs more than the reversed way, so it's useful to reduce times about memory copying.
ALWAYS_INLINE static inline void * sse2_inline_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);

    void * ret = dst;

#if defined(MCP) || defined(MCP_END)
    static_assert(false);
#endif

#define MCP_END(n) tiflash_compiler_builtin_memcpy(dst + size - (n), src + size - (n), (n));
#define MCP(n) tiflash_compiler_builtin_memcpy(dst, src, (n));

    if (likely(size <= 32))
    {
        if (unlikely(size <= 1))
        {
            if (likely(size == 1))
            {
                /// A single byte.
                *dst = *src;
            }
            /// No bytes remaining.
        }
        else if (unlikely(size < 4)) // sse2_inline_memcpy(_,_, 4 ) should use 4 bytes register directly
        {
            /// Chunks of 2..3 bytes.
            MCP(2);
            MCP_END(2);
        }
        else if (unlikely(size < 8)) // sse2_inline_memcpy(_,_, 8 ) should use 8 bytes register directly
        {
            /// Chunks of 4..7 bytes.
            MCP(4);
            MCP_END(4);
        }
        else if (unlikely(size <= 16))
        {
            /// Chunks of 8..16 bytes.
            MCP(8);
            MCP_END(8);
        }
        else
        {
            /// Chunks of 17..32 bytes.
            MCP(16);
            MCP_END(16);
        }
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
