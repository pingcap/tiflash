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

#include <Columns/ColumnFixedString.h>
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/memcpySmall.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>
#include <common/memcpy.h>

#if __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_LARGE_STRING_SIZE;
extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
extern const int PARAMETER_OUT_OF_BOUND;
} // namespace ErrorCodes


MutableColumnPtr ColumnFixedString::cloneResized(size_t size) const
{
    MutableColumnPtr new_col_holder = ColumnFixedString::create(n);

    if (size > 0)
    {
        auto & new_col = static_cast<ColumnFixedString &>(*new_col_holder);
        new_col.chars.resize(size * n);

        size_t count = std::min(this->size(), size);
        memcpy(&(new_col.chars[0]), &chars[0], count * n * sizeof(chars[0]));

        if (size > count)
            memset(&(new_col.chars[count * n]), '\0', (size - count) * n);
    }

    return new_col_holder;
}

void ColumnFixedString::insert(const Field & x)
{
    const auto & s = DB::get<const String &>(x);
    insertData(s.data(), s.size());
}

void ColumnFixedString::insertFrom(const IColumn & src_, size_t index)
{
    const auto & src = static_cast<const ColumnFixedString &>(src_);

    if (n != src.getN())
        throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);

    size_t old_size = chars.size();
    chars.resize(old_size + n);
    memcpySmallAllowReadWriteOverflow15(&chars[old_size], &src.chars[n * index], n);
}

void ColumnFixedString::insertManyFrom(const IColumn & src_, size_t position, size_t length)
{
    const auto & src = static_cast<const ColumnFixedString &>(src_);
    if (n != src.getN())
        throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    size_t old_size = chars.size();
    size_t new_size = old_size + n * length;
    chars.resize(new_size);
    const auto * src_char_ptr = &src.chars[n * position];
    for (size_t i = old_size; i < new_size; i += n)
        memcpySmallAllowReadWriteOverflow15(&chars[i], src_char_ptr, n);
}

void ColumnFixedString::insertDisjunctFrom(const IColumn & src_, const std::vector<size_t> & position_vec)
{
    const auto & src = static_cast<const ColumnFixedString &>(src_);
    if (n != src.getN())
        throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    size_t old_size = chars.size();
    size_t new_size = old_size + position_vec.size() * n;
    chars.resize(new_size);
    const auto & src_chars = src.chars;
    for (size_t i = old_size, j = 0; i < new_size; i += n, ++j)
        memcpySmallAllowReadWriteOverflow15(&chars[i], &src_chars[position_vec[j] * n], n);
}

void ColumnFixedString::insertData(const char * pos, size_t length)
{
    if (length > n)
        throw Exception("Too large string for FixedString column", ErrorCodes::TOO_LARGE_STRING_SIZE);

    size_t old_size = chars.size();
    chars.resize(old_size + n);
    inline_memcpy(chars.data() + old_size, pos, length);
    memset(chars.data() + old_size + length, 0, n - length);
}

StringRef ColumnFixedString::serializeValueIntoArena(
    size_t index,
    Arena & arena,
    char const *& begin,
    const TiDB::TiDBCollatorPtr &,
    String &) const
{
    auto * pos = arena.allocContinue(n, begin);
    inline_memcpy(pos, &chars[n * index], n);
    return StringRef(pos, n);
}

const char * ColumnFixedString::deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &)
{
    size_t old_size = chars.size();
    chars.resize(old_size + n);
    inline_memcpy(&chars[old_size], pos, n);
    return pos + n;
}

void ColumnFixedString::countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const
{
    RUNTIME_CHECK_MSG(byte_size.size() == size(), "size of byte_size({}) != column size({})", byte_size.size(), size());

    size_t size = byte_size.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += n;
}

void ColumnFixedString::countSerializeByteSizeForColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets) const
{
    RUNTIME_CHECK_MSG(
        byte_size.size() == array_offsets.size(),
        "size of byte_size({}) != size of array_offsets({})",
        byte_size.size(),
        array_offsets.size());

    size_t size = array_offsets.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += n * (array_offsets[i] - array_offsets[i - 1]);
}

void ColumnFixedString::serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const
{
    if (has_null)
        serializeToPosImpl<true>(pos, start, length);
    else
        serializeToPosImpl<false>(pos, start, length);
}

template <bool has_null>
void ColumnFixedString::serializeToPosImpl(PaddedPODArray<char *> & pos, size_t start, size_t length) const
{
    RUNTIME_CHECK_MSG(length <= pos.size(), "length({}) > size of pos({})", length, pos.size());
    RUNTIME_CHECK_MSG(start + length <= size(), "start({}) + length({}) > size of column({})", start, length, size());

    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }
        inline_memcpy(pos[i], &chars[n * (start + i)], n);
        pos[i] += n;
    }
}

void ColumnFixedString::serializeToPosForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const IColumn::Offsets & array_offsets) const
{
    if (has_null)
        batchSerializeForColumnArrayImpl<true>(pos, start, length, array_offsets);
    else
        batchSerializeForColumnArrayImpl<false>(pos, start, length, array_offsets);
}

template <bool has_null>
void ColumnFixedString::batchSerializeForColumnArrayImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const IColumn::Offsets & array_offsets) const
{
    RUNTIME_CHECK_MSG(length <= pos.size(), "length({}) > size of pos({})", length, pos.size());
    RUNTIME_CHECK_MSG(
        start + length <= array_offsets.size(),
        "start({}) + length({}) > size of array_offsets({})",
        start,
        length,
        array_offsets.size());
    RUNTIME_CHECK_MSG(
        array_offsets.empty() || array_offsets.back() == size(),
        "The last array offset({}) doesn't match size of column({})",
        array_offsets.back(),
        size());

    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }

        size_t len = array_offsets[start + i] - array_offsets[start + i - 1];
        inline_memcpy(pos[i], &chars[n * array_offsets[start + i - 1]], n * len);
        pos[i] += n * len;
    }
}

/// TODO: optimize by using align_buffer
void ColumnFixedString::deserializeAndInsertFromPos(PaddedPODArray<const char *> & pos, bool /* use_nt_align_buffer */)
{
    size_t size = pos.size();
    size_t old_char_size = chars.size();
    chars.resize(old_char_size + n * size);
    for (size_t i = 0; i < size; ++i)
    {
        inline_memcpy(&chars[old_char_size], pos[i], n);
        old_char_size += n;
        pos[i] += n;
    }
}

void ColumnFixedString::deserializeAndInsertFromPosForColumnArray(
    PaddedPODArray<const char *> & pos,
    const IColumn::Offsets & array_offsets,
    bool /* use_nt_align_buffer */)
{
    if unlikely (pos.empty())
        return;
    RUNTIME_CHECK_MSG(
        pos.size() <= array_offsets.size(),
        "size of pos({}) > size of array_offsets({})",
        pos.size(),
        array_offsets.size());
    size_t start_point = array_offsets.size() - pos.size();
    RUNTIME_CHECK_MSG(
        array_offsets[start_point - 1] == size(),
        "array_offset[start_point({}) - 1]({}) doesn't match size of column({})",
        start_point,
        array_offsets[start_point - 1],
        size());

    chars.resize(array_offsets.back() * n);

    size_t size = pos.size();
    for (size_t i = 0; i < size; ++i)
    {
        size_t len = array_offsets[start_point + i] - array_offsets[start_point + i - 1];
        inline_memcpy(&chars[n * array_offsets[start_point + i - 1]], pos[i], n * len);
        pos[i] += n * len;
    }
}

void ColumnFixedString::updateHashWithValue(size_t index, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    hash.update(reinterpret_cast<const char *>(&chars[n * index]), n);
}

void ColumnFixedString::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
    const
{
    for (size_t i = 0, sz = chars.size() / n; i < sz; ++i)
    {
        hash_values[i].update(reinterpret_cast<const char *>(&chars[n * i]), n);
    }
}

void ColumnFixedString::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    updateWeakHash32Impl<false>(hash, {});
}

void ColumnFixedString::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr &,
    String &,
    const BlockSelective & selective) const
{
    updateWeakHash32Impl<true>(hash, selective);
}

template <bool selective_block>
void ColumnFixedString::updateWeakHash32Impl(WeakHash32 & hash, const BlockSelective & selective) const
{
    size_t rows;
    if constexpr (selective_block)
    {
        rows = selective.size();
    }
    else
    {
        rows = size();
    }

    RUNTIME_CHECK_MSG(
        hash.getData().size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        hash.getData().size(),
        rows);

    const UInt8 * begin = chars.data();
    UInt32 * hash_data = hash.getData().data();

    for (size_t i = 0; i < rows; ++i)
    {
        size_t row = i;
        if constexpr (selective_block)
            row = selective[i];

        *hash_data = ::updateWeakHash32(begin + n * row, n, *hash_data);
        ++hash_data;
    }
}

template <bool positive>
struct ColumnFixedString::less
{
    const ColumnFixedString & parent;
    explicit less(const ColumnFixedString & parent_)
        : parent(parent_)
    {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        /// TODO: memcmp slows down.
        int res = memcmp(&parent.chars[lhs * parent.n], &parent.chars[rhs * parent.n], parent.n);
        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnFixedString::getPermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    size_t s = size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), less<false>(*this));
        else
            std::sort(res.begin(), res.end(), less<true>(*this));
    }
}

void ColumnFixedString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_concrete = static_cast<const ColumnFixedString &>(src);

    if (start + length > src_concrete.size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnFixedString::insertRangeFrom method, start={}, length={}, "
                "src.size()={}",
                start,
                length,
                src_concrete.size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = chars.size();
    chars.resize(old_size + length * n);
    memcpy(&chars[old_size], &src_concrete.chars[start * n], length * n);
}

ColumnPtr ColumnFixedString::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t col_size = size();
    if (col_size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnFixedString::create(n);

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res->chars.reserve(result_size_hint * n);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + col_size;
    const UInt8 * data_pos = &chars[0];

#if __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + col_size / SIMD_BYTES * SIMD_BYTES;
    const size_t chars_per_simd_elements = SIMD_BYTES * n;

    while (filt_pos < filt_end_sse)
    {
        int mask
            = _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));

        if (0 == mask)
        {
            /// Nothing is inserted.
            data_pos += chars_per_simd_elements;
        }
        else if (0xFFFF == mask)
        {
            res->chars.insert(data_pos, data_pos + chars_per_simd_elements);
            data_pos += chars_per_simd_elements;
        }
        else
        {
            size_t res_chars_size = res->chars.size();
            for (size_t i = 0; i < SIMD_BYTES; ++i)
            {
                if (filt_pos[i])
                {
                    res->chars.resize(res_chars_size + n);
                    memcpySmallAllowReadWriteOverflow15(&res->chars[res_chars_size], data_pos, n);
                    res_chars_size += n;
                }
                data_pos += n;
            }
        }

        filt_pos += SIMD_BYTES;
    }
#endif

    size_t res_chars_size = res->chars.size();
    while (filt_pos < filt_end)
    {
        if (*filt_pos)
        {
            res->chars.resize(res_chars_size + n);
            memcpySmallAllowReadWriteOverflow15(&res->chars[res_chars_size], data_pos, n);
            res_chars_size += n;
        }

        ++filt_pos;
        data_pos += n;
    }

    return res;
}

ColumnPtr ColumnFixedString::permute(const Permutation & perm, size_t limit) const
{
    size_t col_size = size();

    if (limit == 0)
        limit = col_size;
    else
        limit = std::min(col_size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnFixedString::create(n);

    auto res = ColumnFixedString::create(n);

    Chars_t & res_chars = res->chars;

    res_chars.resize(n * limit);

    size_t offset = 0;
    for (size_t i = 0; i < limit; ++i, offset += n)
        memcpySmallAllowReadWriteOverflow15(&res_chars[offset], &chars[perm[i] * n], n);

    return res;
}

ColumnPtr ColumnFixedString::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    size_t col_rows = size();
    if (col_rows != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= col_rows);

    auto res = ColumnFixedString::create(n);

    if (0 == col_rows)
        return res;

    Chars_t & res_chars = res->chars;
    res_chars.resize(n * (offsets[end_row - 1]));

    Offset curr_offset = 0;
    for (size_t i = start_row; i < end_row; ++i)
        for (size_t next_offset = offsets[i]; curr_offset < next_offset; ++curr_offset)
            memcpySmallAllowReadWriteOverflow15(&res->chars[curr_offset * n], &chars[i * n], n);

    return res;
}

void ColumnFixedString::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnFixedString::getExtremes(Field & min, Field & max) const
{
    min = String();
    max = String();

    size_t col_size = size();

    if (col_size == 0)
        return;

    size_t min_idx = 0;
    size_t max_idx = 0;

    less<true> less_op(*this);

    for (size_t i = 1; i < col_size; ++i)
    {
        if (less_op(i, min_idx))
            min_idx = i;
        else if (less_op(max_idx, i))
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}

} // namespace DB
