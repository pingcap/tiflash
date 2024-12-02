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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <common/memcpy.h>

#include <bit>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{

namespace
{
inline UInt64 ToBits64(const UInt8 * bytes64)
{
#if defined(__AVX512F__) && defined(__AVX512BW__)
    const __m512i vbytes = _mm512_loadu_si512(reinterpret_cast<const void *>(bytes64));
    UInt64 res = _mm512_testn_epi8_mask(vbytes, vbytes);
#elif defined(__AVX2__)
    const auto check_block = _mm256_setzero_si256();
    uint64_t mask0 = mem_utils::details::get_block32_cmp_eq_mask(bytes64, check_block);
    uint64_t mask1
        = mem_utils::details::get_block32_cmp_eq_mask(bytes64 + mem_utils::details::BLOCK32_SIZE, check_block);
    auto res = mask0 | (mask1 << mem_utils::details::BLOCK32_SIZE);
#elif defined(__SSE2__)
    const auto zero16 = _mm_setzero_si128();
    UInt64 res = static_cast<UInt64>(_mm_movemask_epi8(
                     _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64)), zero16)))
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 16)), zero16)))
           << 16)
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 32)), zero16)))
           << 32)
        | (static_cast<UInt64>(_mm_movemask_epi8(
               _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(bytes64 + 48)), zero16)))
           << 48);
#elif defined(__aarch64__) && defined(__ARM_NEON)
    const uint8x16_t bitmask
        = {0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80};
    const auto * src = reinterpret_cast<const unsigned char *>(bytes64);
    const uint8x16_t p0 = vceqzq_u8(vld1q_u8(src));
    const uint8x16_t p1 = vceqzq_u8(vld1q_u8(src + 16));
    const uint8x16_t p2 = vceqzq_u8(vld1q_u8(src + 32));
    const uint8x16_t p3 = vceqzq_u8(vld1q_u8(src + 48));
    uint8x16_t t0 = vandq_u8(p0, bitmask);
    uint8x16_t t1 = vandq_u8(p1, bitmask);
    uint8x16_t t2 = vandq_u8(p2, bitmask);
    uint8x16_t t3 = vandq_u8(p3, bitmask);
    uint8x16_t sum0 = vpaddq_u8(t0, t1);
    uint8x16_t sum1 = vpaddq_u8(t2, t3);
    sum0 = vpaddq_u8(sum0, sum1);
    sum0 = vpaddq_u8(sum0, sum0);
    UInt64 res = vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
#else
    UInt64 res = 0;
    for (size_t i = 0; i < 64; ++i)
        res |= static_cast<UInt64>(0 == bytes64[i]) << i;
#endif
    return ~res;
}

constexpr size_t FILTER_SIMD_BYTES = 64;

/// If mask is a number of this kind: [0]*[1]+ function returns the length of the cluster of 1s.
/// Otherwise it returns the special value: 0xFF.
/// Note: mask must be non-zero.
inline UInt8 prefixToCopy(UInt64 mask)
{
    static constexpr UInt64 all_match = 0xFFFFFFFFFFFFFFFFULL;
    if (mask == all_match)
        return 64;
    /// std::countl_zero count from the most significant bit of mask, corresponding to the tail of the original filter.
    /// If only the tail of the original filter is zero, we can copy the prefix directly.
    /// The length of tail zero if `leading_zeros`, so the length of the prefix to copy is 64 - #(leading zeroes).
    const UInt64 leading_zeroes = std::countl_zero(mask);
    if (mask == ((all_match << leading_zeroes) >> leading_zeroes))
        return 64 - leading_zeroes;
    else
        return 0xFF;
}

inline UInt8 suffixToCopy(UInt64 mask)
{
    const auto prefix_to_copy = prefixToCopy(~mask);
    return prefix_to_copy >= 64 ? prefix_to_copy : 64 - prefix_to_copy;
}
} // namespace


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

namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{
/// Implementation details of filterArraysImpl function, used as template parameter.
/// Allow to build or not to build offsets array.

struct ResultOffsetsBuilder
{
    IColumn::Offsets & res_offsets;
    IColumn::Offset current_src_offset = 0;

    explicit ResultOffsetsBuilder(IColumn::Offsets * res_offsets_)
        : res_offsets(*res_offsets_)
    {}

    void reserve(size_t result_size_hint) { res_offsets.reserve(result_size_hint); }

    void insertChunk(
        size_t n,
        const IColumn::Offset * src_offsets_pos,
        bool first,
        IColumn::Offset chunk_offset,
        size_t chunk_size)
    {
        const auto offsets_size_old = res_offsets.size();
        res_offsets.resize(offsets_size_old + n);
        inline_memcpy(&res_offsets[offsets_size_old], src_offsets_pos, n * sizeof(IColumn::Offset));

        if (!first)
        {
            /// difference between current and actual offset
            const auto diff_offset = chunk_offset - current_src_offset;

            if (diff_offset > 0)
            {
                auto * res_offsets_pos = &res_offsets[offsets_size_old];

                /// adjust offsets
                for (size_t i = 0; i < n; ++i)
                    res_offsets_pos[i] -= diff_offset;
            }
        }
        current_src_offset += chunk_size;
    }
};

struct NoResultOffsetsBuilder
{
    explicit NoResultOffsetsBuilder(IColumn::Offsets *) {}
    void reserve(size_t) {}

    void insertChunk(size_t, const IColumn::Offset *, bool, IColumn::Offset, size_t) {}
};

template <typename T, typename ResultOffsetsBuilder>
void filterArraysImplGeneric(
    const PaddedPODArray<T> & src_elems,
    const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    IColumn::Offsets * res_offsets,
    const IColumn::Filter & filt,
    ssize_t result_size_hint)
{
    const size_t size = src_offsets.size();
    if (size != filt.size())
        throw Exception(
            fmt::format("size of filter {} doesn't match size of column {}", filt.size(), size),
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    ResultOffsetsBuilder result_offsets_builder(res_offsets);

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);

        result_offsets_builder.reserve(result_size_hint);

        if (result_size_hint < 1000000000 && src_elems.size() < 1000000000) /// Avoid overflow.
            res_elems.reserve((result_size_hint * src_elems.size() + size - 1) / size);
    }

    const UInt8 * filt_pos = filt.data();
    const auto * filt_end = filt_pos + size;

    const auto * offsets_pos = src_offsets.data();
    const auto * offsets_begin = offsets_pos;

    /// copy n arrays from ending at *end_offset_ptr
    const auto copy_chunk = [&](const IColumn::Offset * offset_ptr, size_t n) {
        const auto first = offset_ptr == offsets_begin;

        const auto chunk_offset = first ? 0 : offset_ptr[-1];
        const auto chunk_size = offset_ptr[n - 1] - chunk_offset;

        result_offsets_builder.insertChunk(n, offset_ptr, first, chunk_offset, chunk_size);

        /// copy elements for n arrays at once
        const auto elems_size_old = res_elems.size();
        res_elems.resize(elems_size_old + chunk_size);
        inline_memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(T));
    };

    const auto * filt_end_aligned = filt_pos + size / FILTER_SIMD_BYTES * FILTER_SIMD_BYTES;
    while (filt_pos < filt_end_aligned)
    {
        auto mask = ToBits64(filt_pos);
        if likely (0 != mask)
        {
            if (const auto prefix_to_copy = prefixToCopy(mask); 0xFF != prefix_to_copy)
            {
                copy_chunk(offsets_pos, prefix_to_copy);
            }
            else
            {
                if (const auto suffix_to_copy = suffixToCopy(mask); 0xFF != suffix_to_copy)
                {
                    copy_chunk(offsets_pos + FILTER_SIMD_BYTES - suffix_to_copy, suffix_to_copy);
                }
                else
                {
                    while (mask)
                    {
                        size_t index = std::countr_zero(mask);
                        copy_chunk(offsets_pos + index, 1);
                        mask &= mask - 1;
                    }
                }
            }
        }

        filt_pos += FILTER_SIMD_BYTES;
        offsets_pos += FILTER_SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            copy_chunk(offsets_pos, 1);

        ++filt_pos;
        ++offsets_pos;
    }
}
} // namespace


template <typename T>
void filterArraysImpl(
    const PaddedPODArray<T> & src_elems,
    const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    IColumn::Offsets & res_offsets,
    const IColumn::Filter & filt,
    ssize_t result_size_hint)
{
    return filterArraysImplGeneric<T, ResultOffsetsBuilder>(
        src_elems,
        src_offsets,
        res_elems,
        &res_offsets,
        filt,
        result_size_hint);
}

template <typename T>
void filterArraysImplOnlyData(
    const PaddedPODArray<T> & src_elems,
    const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    const IColumn::Filter & filt,
    ssize_t result_size_hint)
{
    return filterArraysImplGeneric<T, NoResultOffsetsBuilder>(
        src_elems,
        src_offsets,
        res_elems,
        nullptr,
        filt,
        result_size_hint);
}


/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(TYPE)                         \
    template void filterArraysImpl<TYPE>(         \
        const PaddedPODArray<TYPE> &,             \
        const IColumn::Offsets &,                 \
        PaddedPODArray<TYPE> &,                   \
        IColumn::Offsets &,                       \
        const IColumn::Filter &,                  \
        ssize_t);                                 \
    template void filterArraysImplOnlyData<TYPE>( \
        const PaddedPODArray<TYPE> &,             \
        const IColumn::Offsets &,                 \
        PaddedPODArray<TYPE> &,                   \
        const IColumn::Filter &,                  \
        ssize_t);

INSTANTIATE(UInt8)
INSTANTIATE(UInt16)
INSTANTIATE(UInt32)
INSTANTIATE(UInt64)
INSTANTIATE(Int8)
INSTANTIATE(Int16)
INSTANTIATE(Int32)
INSTANTIATE(Int64)
INSTANTIATE(Float32)
INSTANTIATE(Float64)

#undef INSTANTIATE

namespace
{
template <typename T, typename Container>
inline void filterImplAligned(
    const UInt8 *& filt_pos,
    const UInt8 *& filt_end_aligned,
    const T *& data_pos,
    Container & res_data)
{
    while (filt_pos < filt_end_aligned)
    {
        UInt64 mask = ToBits64(filt_pos);
        if likely (0 != mask)
        {
            if (const UInt8 prefix_to_copy = prefixToCopy(mask); 0xFF != prefix_to_copy)
            {
                res_data.insert(data_pos, data_pos + prefix_to_copy);
            }
            else
            {
                if (const UInt8 suffix_to_copy = suffixToCopy(mask); 0xFF != suffix_to_copy)
                {
                    res_data.insert(data_pos + FILTER_SIMD_BYTES - suffix_to_copy, data_pos + FILTER_SIMD_BYTES);
                }
                else
                {
                    while (mask)
                    {
                        size_t index = std::countr_zero(mask);
                        res_data.push_back(data_pos[index]);
                        mask &= mask - 1;
                    }
                }
            }
        }

        filt_pos += FILTER_SIMD_BYTES;
        data_pos += FILTER_SIMD_BYTES;
    }
}
} // namespace


template <typename T, typename Container>
void filterImpl(const UInt8 *& filt_pos, const UInt8 *& filt_end, const T *& data_pos, Container & res_data)
{
    const UInt8 * filt_end_aligned = filt_pos + (filt_end - filt_pos) / FILTER_SIMD_BYTES * FILTER_SIMD_BYTES;
    filterImplAligned<T, Container>(filt_pos, filt_end_aligned, data_pos, res_data);

    /// Process the tail.
    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);
        ++filt_pos;
        ++data_pos;
    }
}

/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(T, Container)           \
    template void filterImpl<T, Container>( \
        const UInt8 *& filt_pos,            \
        const UInt8 *& filt_end,            \
        const T *& data_pos,                \
        Container & res_data); // NOLINT

INSTANTIATE(UInt8, PaddedPODArray<UInt8>)
INSTANTIATE(UInt16, PaddedPODArray<UInt16>)
INSTANTIATE(UInt32, PaddedPODArray<UInt32>)
INSTANTIATE(UInt64, PaddedPODArray<UInt64>)
INSTANTIATE(UInt128, PaddedPODArray<UInt128>)
INSTANTIATE(Int8, PaddedPODArray<Int8>)
INSTANTIATE(Int16, PaddedPODArray<Int16>)
INSTANTIATE(Int32, PaddedPODArray<Int32>)
INSTANTIATE(Int64, PaddedPODArray<Int64>)
INSTANTIATE(Int128, PaddedPODArray<Int128>)
INSTANTIATE(Float32, PaddedPODArray<Float32>)
INSTANTIATE(Float64, PaddedPODArray<Float64>)
INSTANTIATE(Decimal32, DecimalPaddedPODArray<Decimal32>)
INSTANTIATE(Decimal64, DecimalPaddedPODArray<Decimal64>)
INSTANTIATE(Decimal128, DecimalPaddedPODArray<Decimal128>)
INSTANTIATE(Decimal256, DecimalPaddedPODArray<Decimal256>)

#undef INSTANTIATE

} // namespace DB
