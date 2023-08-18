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
#include <common/defines.h>
#include <immintrin.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace mem_utils
{

// Custom inline memcpy implementation for TiFlash.
// - it is recommended to use for inline function with `avx2` supported
// - according to https://github.com/pingcap/tiflash/pull/6281, `avx2_inline_memcpy` perform better than `legacy::inline_memcpy`(from clickhouse)
// - like `std::memcpy`, the behavior is undefined when the source and the destination objects overlap
ALWAYS_INLINE static inline void * avx2_inline_memcpy(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);

    void * ret = dst;

#define MEM_CP_END(n) tiflash_compiler_builtin_memcpy(dst + size - (n), src + size - (n), (n));
#define MEM_CP_HEAD(n) tiflash_compiler_builtin_memcpy(dst, src, (n));
#define PREFETCH(addr) __builtin_prefetch(addr)

    constexpr int block32_size = details::BLOCK32_SIZE;

    if (likely(size <= block32_size))
    {
        if (unlikely(size < 8))
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
            else if (unlikely(size < 4))
            {
                /// Chunks of 2..3 bytes.
                details::memcpy_ignore_overlap<2>(dst, src, size);
            }
            else
            {
                /// Chunks of 4..7 bytes.
                details::memcpy_ignore_overlap<4>(dst, src, size);
            }
        }
        else if (likely(size <= 16))
        {
            /// Chunks of 8..16 bytes.
            details::memcpy_ignore_overlap<8>(dst, src, size);
        }
        else
        {
            /// Chunks of 17..32 bytes.
            details::memcpy_ignore_overlap<16>(dst, src, size);
        }
        return ret;
    }

    constexpr size_t loop_block32_cnt = 8;

    if (unlikely(size > block32_size * 8))
    {
        /// Large size with fully unrolled loop.
        {
            MEM_CP_HEAD(block32_size);

            // reduce instruction: `offset` = or (`dst`, 0xfffffffffffffff0)
            auto offset = ssize_t(size_t(dst) % block32_size) - block32_size;
            dst -= offset;
            src -= offset;
            size += offset;
            assert(size_t(dst) % block32_size == 0);
        }

        // TODO: use non-temporal way(mark data unlikely to be used again soon) to minimize caching for large memory size(bigger than L2/L3 cache size) if necessary.
        // TODO: check whether source address is aligned to 32 and use specific aligned instructions if necessary.

        /// Aligned unrolled copy.
        while (size >= block32_size * loop_block32_cnt)
        {
#define M(n) details::Block32 c##n = mem_utils::details::load_block32(src + block32_size * (n));
            M(0)
            M(1)
            M(2)
            M(3)
            M(4)
            M(5)
            M(6)
            M(7)
#undef M
#define M(n) mem_utils::details::write_block32<true>(dst + block32_size * (n), c##n);
            M(0)
            M(1)
            M(2)
            M(3)
            M(4)
            M(5)
            M(6)
            M(7)
#undef M
            src += block32_size * loop_block32_cnt;
            dst += block32_size * loop_block32_cnt;
            size -= block32_size * loop_block32_cnt;
        }
        if (size <= block32_size)
        {
            MEM_CP_END(block32_size);
            return ret;
        }
    }

    assert(size > block32_size);

    if (unlikely(size <= 2 * block32_size))
    {
        //  32 < n <= 2 * 32
        details::memcpy_ignore_overlap<32>(dst, src, size);
        return ret;
    }

    if (unlikely(size <= 4 * block32_size))
    {
        //  2 * 32 < n <= 4 * 32
        details::memcpy_ignore_overlap<64>(dst, src, size);
        return ret;
    }

    assert(size <= 8 * block32_size);
    {
        //  4 * 32 < n <= 8 * 32
        details::memcpy_ignore_overlap<128>(dst, src, size);
        return ret;
    }

#undef MEM_CP_HEAD
#undef MEM_CP_END
#undef PREFETCH
}
} // namespace mem_utils