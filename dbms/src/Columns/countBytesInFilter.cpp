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

#include <Columns/ColumnUtil.h>
#include <Columns/IColumn.h>
#include <Columns/countBytesInFilter.h>

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include <bit>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

ALWAYS_INLINE inline static size_t CountBytesInFilter(const UInt8 * filt, size_t start, size_t end)
{
#if defined(__AVX2__)
    size_t size = end - start;
    auto zero_cnt = mem_utils::details::avx2_byte_count(reinterpret_cast<const char *>(filt + start), size, 0);
    return size - zero_cnt;
#else
    size_t count = 0;

    /** NOTE: In theory, `filt` should only contain zeros and ones.
      * But, just in case, here the condition > 0 (to signed bytes) is used.
      * It would be better to use != 0, then this does not allow SSE2.
      */

    const char * pos = reinterpret_cast<const char *>(filt);
    pos += start;

    const char * end_pos = pos + (end - start);
    for (; pos < end_pos; ++pos)
        count += *pos != 0;

    return count;
#endif
}

size_t countBytesInFilter(const UInt8 * filt, size_t sz)
{
    return CountBytesInFilter(filt, 0, sz);
}

size_t countBytesInFilter(const UInt8 * filt, size_t start, size_t sz)
{
    return CountBytesInFilter(filt, start, start + sz);
}

size_t countBytesInFilter(const IColumn::Filter & filt, size_t start, size_t sz)
{
    return CountBytesInFilter(filt.data(), start, start + sz);
}

size_t countBytesInFilter(const IColumn::Filter & filt)
{
    return CountBytesInFilter(filt.data(), 0, filt.size());
}

static inline size_t CountBytesInFilterWithNull(const UInt8 * p1, const UInt8 * p2, size_t size)
{
    size_t count = 0;
    for (size_t i = 0; i < size; ++i)
    {
        count += (p1[i] & ~p2[i]) != 0;
    }
    return count;
}

static inline size_t CountBytesInFilterWithNull(
    const IColumn::Filter & filt,
    const UInt8 * null_map,
    size_t start,
    size_t end)
{
    size_t count = 0;

    /** NOTE: In theory, `filt` should only contain zeros and ones.
      * But, just in case, here the condition > 0 (to signed bytes) is used.
      * It would be better to use != 0, then this does not allow SSE2.
      */

    const auto * p1 = filt.data() + start;
    const auto * p2 = null_map + start;
    size_t size = end - start;

#if defined(__SSE2__) || defined(__AVX2__)
    for (; size >= 64;)
    {
        count += std::popcount(ToBits64(p1) & ~ToBits64(p2));
        p1 += 64, p2 += 64;
        size -= 64;
    }
#endif
    count += CountBytesInFilterWithNull(p1, p2, size);
    return count;
}

size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map)
{
    return CountBytesInFilterWithNull(filt, null_map, 0, filt.size());
}

size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map, size_t start, size_t sz)
{
    return CountBytesInFilterWithNull(filt, null_map, start, start + sz);
}

std::vector<size_t> countColumnsSizeInSelector(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector)
{
    std::vector<size_t> counts(num_columns);
    for (auto idx : selector)
        ++counts[idx];

    return counts;
}

} // namespace DB
