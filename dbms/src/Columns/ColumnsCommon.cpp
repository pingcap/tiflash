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

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <common/memcpy.h>

#include <bit>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{
#if defined(__AVX2__) || defined(__SSE2__)
/// Transform 64-byte mask to 64-bit mask.
inline UInt64 ToBits64(const Int8 * bytes64)
{
#if defined(__AVX2__)
    const auto check_block = _mm256_setzero_si256();
    uint64_t mask0 = mem_utils::details::get_block32_cmp_eq_mask(bytes64, check_block);
    uint64_t mask1
        = mem_utils::details::get_block32_cmp_eq_mask(bytes64 + mem_utils::details::BLOCK32_SIZE, check_block);
    auto res = mask0 | (mask1 << mem_utils::details::BLOCK32_SIZE);
    return ~res;
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
    return ~res;
#endif
}
#endif

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

static inline size_t CountBytesInFilterWithNull(const Int8 * p1, const Int8 * p2, size_t size)
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

    const Int8 * p1 = reinterpret_cast<const Int8 *>(filt.data()) + start;
    const Int8 * p2 = reinterpret_cast<const Int8 *>(null_map) + start;
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

    void insertOne(size_t array_size)
    {
        current_src_offset += array_size;
        res_offsets.push_back(current_src_offset);
    }

    template <size_t SIMD_BYTES>
    void insertChunk(
        const IColumn::Offset * src_offsets_pos,
        bool first,
        IColumn::Offset chunk_offset,
        size_t chunk_size)
    {
        const auto offsets_size_old = res_offsets.size();
        res_offsets.resize(offsets_size_old + SIMD_BYTES);
        inline_memcpy(&res_offsets[offsets_size_old], src_offsets_pos, SIMD_BYTES * sizeof(IColumn::Offset));

        if (!first)
        {
            /// difference between current and actual offset
            const auto diff_offset = chunk_offset - current_src_offset;

            if (diff_offset > 0)
            {
                auto * res_offsets_pos = &res_offsets[offsets_size_old];

                /// adjust offsets
                for (size_t i = 0; i < SIMD_BYTES; ++i)
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
    void insertOne(size_t) {}

    template <size_t SIMD_BYTES>
    void insertChunk(const IColumn::Offset *, bool, IColumn::Offset, size_t)
    {}
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

    /// copy array ending at *end_offset_ptr
    const auto copy_array = [&](const IColumn::Offset * offset_ptr) {
        const auto arr_offset = offset_ptr == offsets_begin ? 0 : offset_ptr[-1];
        const auto arr_size = *offset_ptr - arr_offset;

        result_offsets_builder.insertOne(arr_size);

        const auto elems_size_old = res_elems.size();
        res_elems.resize(elems_size_old + arr_size);
        inline_memcpy(&res_elems[elems_size_old], &src_elems[arr_offset], arr_size * sizeof(T));
    };

#if __SSE2__
    static constexpr size_t SIMD_BYTES = 16;
    const auto * filt_end_aligned = filt_pos + size / SIMD_BYTES * SIMD_BYTES;
    const auto zero_vec = _mm_setzero_si128();

    while (filt_pos < filt_end_aligned)
    {
        uint32_t mask
            = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero_vec));

        if (0xffff == mask)
        {
            /// SIMD_BYTES consecutive rows pass the filter
            const auto first = offsets_pos == offsets_begin;

            const auto chunk_offset = first ? 0 : offsets_pos[-1];
            const auto chunk_size = offsets_pos[SIMD_BYTES - 1] - chunk_offset;

            result_offsets_builder.template insertChunk<SIMD_BYTES>(offsets_pos, first, chunk_offset, chunk_size);

            /// copy elements for SIMD_BYTES arrays at once
            const auto elems_size_old = res_elems.size();
            res_elems.resize(elems_size_old + chunk_size);
            inline_memcpy(&res_elems[elems_size_old], &src_elems[chunk_offset], chunk_size * sizeof(T));
        }
        else
        {
            while (mask)
            {
                size_t index = std::countr_zero(mask);
                copy_array(offsets_pos + index);
                mask = mask & (mask - 1);
            }
        }

        filt_pos += SIMD_BYTES;
        offsets_pos += SIMD_BYTES;
    }
#endif

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            copy_array(offsets_pos);

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

} // namespace DB
