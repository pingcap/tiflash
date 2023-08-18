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

#include <bit>

namespace mem_utils::details
{
#if defined(MEM_UTILS_FUNC_NO_SANITIZE)
MEM_UTILS_FUNC_NO_SANITIZE
#else
ALWAYS_INLINE static inline
#endif
uint64_t avx2_byte_count(const char * src, size_t size, char target)
{
    uint64_t tar_byte_cnt = 0;
    const auto check_block32 = _mm256_set1_epi8(target);

    if (uint8_t right_offset = OFFSET_FROM_ALIGNED(size_t(src), BLOCK32_SIZE); right_offset != 0)
    {
        // align to 32
        src = reinterpret_cast<decltype(src)>(ALIGNED_ADDR(size_t(src), BLOCK32_SIZE));

        // right shift offset to remove useless mask bit
        auto mask = get_block32_cmp_eq_mask(src, check_block32);
        size_t left_remain = BLOCK32_SIZE - right_offset;

        if unlikely (left_remain >= size)
        {
            left_remain -= size;
            mask <<= left_remain;
            mask >>= left_remain;
            mask >>= right_offset;
            return std::popcount(mask);
        }

        mask >>= right_offset;
        tar_byte_cnt += std::popcount(mask);
        size -= left_remain;
        src += BLOCK32_SIZE;
    }

    assert(size_t(src) % BLOCK32_SIZE == 0);

    // clang will unroll by step 4 automatically with flags `-mavx2` if size >= 4 * 32
    // unrolled by step 8 with flags `-march=haswell`
    for (; size >= BLOCK32_SIZE;)
    {
        auto mask = get_block32_cmp_eq_mask(src, check_block32);
        tar_byte_cnt += std::popcount(mask);
        size -= BLOCK32_SIZE, src += BLOCK32_SIZE;
    }

    if unlikely (size != 0)
    {
        auto mask = get_block32_cmp_eq_mask(src, check_block32);
        uint32_t left_remain = BLOCK32_SIZE - size;
        mask <<= left_remain;
        mask >>= left_remain;
        tar_byte_cnt += std::popcount(mask);
    }

    return tar_byte_cnt;
}

} // namespace mem_utils::details
