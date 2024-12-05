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

#include <Columns/ColumnVector.h>
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/NaNUtils.h>
#include <Common/SipHash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>
#include <common/memcpy.h>

#include <cstring>
#include <ext/bit_cast.h>
#include <ext/scope_guard.h>

#if __SSE2__
#include <emmintrin.h>
#endif

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes


template <typename T>
StringRef ColumnVector<T>::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const TiDB::TiDBCollatorPtr &,
    String &) const
{
    auto * pos = arena.allocContinue(sizeof(T), begin);
    tiflash_compiler_builtin_memcpy(pos, &data[n], sizeof(T));
    return StringRef(pos, sizeof(T));
}

template <typename T>
void ColumnVector<T>::countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const
{
    RUNTIME_CHECK_MSG(byte_size.size() == size(), "size of byte_size({}) != column size({})", byte_size.size(), size());

    size_t size = byte_size.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += sizeof(T);
}

template <typename T>
void ColumnVector<T>::countSerializeByteSizeForColumnArray(
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
        byte_size[i] += sizeof(T) * (array_offsets[i] - array_offsets[i - 1]);
}

template <typename T>
void ColumnVector<T>::serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const
{
    if (has_null)
        serializeToPosImpl<true>(pos, start, length);
    else
        serializeToPosImpl<false>(pos, start, length);
}

template <typename T>
template <bool has_null>
void ColumnVector<T>::serializeToPosImpl(PaddedPODArray<char *> & pos, size_t start, size_t length) const
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
        tiflash_compiler_builtin_memcpy(pos[i], &data[start + i], sizeof(T));
        pos[i] += sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::serializeToPosForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const IColumn::Offsets & array_offsets) const
{
    if (has_null)
        serializeToPosForColumnArrayImpl<true>(pos, start, length, array_offsets);
    else
        serializeToPosForColumnArrayImpl<false>(pos, start, length, array_offsets);
}

template <typename T>
template <bool has_null>
void ColumnVector<T>::serializeToPosForColumnArrayImpl(
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
        if (len <= 4)
        {
            for (size_t j = 0; j < len; ++j)
                tiflash_compiler_builtin_memcpy(
                    pos[i] + j * sizeof(T),
                    &data[array_offsets[start + i - 1] + j],
                    sizeof(T));
        }
        else
        {
            inline_memcpy(pos[i], &data[array_offsets[start + i - 1]], len * sizeof(T));
        }
        pos[i] += len * sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::deserializeAndInsertFromPos(
    PaddedPODArray<char *> & pos,
    bool use_nt_align_buffer [[maybe_unused]])
{
    size_t prev_size = data.size();
    size_t size = pos.size();

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if (use_nt_align_buffer)
    {
        if constexpr (FULL_VECTOR_SIZE_AVX2 % sizeof(T) == 0)
        {
            bool is_aligned = reinterpret_cast<std::uintptr_t>(&data[prev_size]) % FULL_VECTOR_SIZE_AVX2 == 0;
            if likely (is_aligned)
            {
                if unlikely (align_buffer_ptr == nullptr)
                    align_buffer_ptr = std::make_unique<ColumnNTAlignBufferAVX2>();

                NTAlignBufferAVX2 & buffer = align_buffer_ptr->getBuffer();
                UInt8 buffer_size = align_buffer_ptr->getSize();
                SCOPE_EXIT({ align_buffer_ptr->setSize(buffer_size); });

                constexpr size_t avx2_width = FULL_VECTOR_SIZE_AVX2 / sizeof(T);
                size_t i = 0;
                if (buffer_size != 0)
                {
                    size_t count = std::min(size, (FULL_VECTOR_SIZE_AVX2 - buffer_size) / sizeof(T));
                    for (; i < count; ++i)
                    {
                        tiflash_compiler_builtin_memcpy(&buffer.data[buffer_size], pos[i], sizeof(T));
                        buffer_size += sizeof(T);
                        pos[i] += sizeof(T);
                    }

                    if (buffer_size < FULL_VECTOR_SIZE_AVX2)
                        return;

                    assert(buffer_size == FULL_VECTOR_SIZE_AVX2);
                    data.resize(prev_size + avx2_width, FULL_VECTOR_SIZE_AVX2);

                    nonTemporalStore64B(&data[prev_size], buffer);
                    prev_size += FULL_VECTOR_SIZE_AVX2 / sizeof(T);
                    buffer_size = 0;
                }

                NTAlignBufferAVX2 tmp_buffer;
                UInt8 tmp_buffer_size = 0;

                data.resize(prev_size + (size - i) / avx2_width * avx2_width, FULL_VECTOR_SIZE_AVX2);
                for (; i + avx2_width <= size; i += avx2_width)
                {
                    /// Loop unrolling
                    for (size_t j = 0; j < avx2_width; ++j)
                    {
                        tiflash_compiler_builtin_memcpy(
                            &tmp_buffer.data[tmp_buffer_size + j * sizeof(T)],
                            pos[i + j],
                            sizeof(T));
                        pos[i + j] += sizeof(T);
                    }
                    tmp_buffer_size += avx2_width * sizeof(T);

                    nonTemporalStore64B(&data[prev_size], tmp_buffer);
                    prev_size += FULL_VECTOR_SIZE_AVX2 / sizeof(T);
                    tmp_buffer_size = 0;
                }

                for (; i < size; ++i)
                {
                    tiflash_compiler_builtin_memcpy(&buffer.data[buffer_size], pos[i], sizeof(T));
                    buffer_size += sizeof(T);
                    pos[i] += sizeof(T);
                }

                _mm_sfence();
                return;
            }
        }
    }

    RUNTIME_CHECK_MSG(
        align_buffer_ptr == nullptr,
        "align_buffer_ptr is not nullptr but use_nt_align_buffer({}) is false or data is unaligned",
        use_nt_align_buffer);
#endif

    data.resize(prev_size + size);
    for (size_t i = 0; i < size; ++i)
    {
        tiflash_compiler_builtin_memcpy(&data[prev_size + i], pos[i], sizeof(T));
        pos[i] += sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::deserializeAndInsertFromPosForColumnArray(
    PaddedPODArray<char *> & pos,
    const IColumn::Offsets & array_offsets,
    bool use_nt_align_buffer [[maybe_unused]])
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

    data.resize(array_offsets.back());

    size_t size = pos.size();
    for (size_t i = 0; i < size; ++i)
    {
        size_t len = array_offsets[start_point + i] - array_offsets[start_point + i - 1];
        if (len <= 4)
        {
            for (size_t j = 0; j < len; ++j)
                tiflash_compiler_builtin_memcpy(
                    &data[array_offsets[start_point + i - 1] + j],
                    pos[i] + j * sizeof(T),
                    sizeof(T));
        }
        else
        {
            inline_memcpy(&data[array_offsets[start_point + i - 1]], pos[i], len * sizeof(T));
        }
        pos[i] += len * sizeof(T);
    }
}

template <typename T>
void ColumnVector<T>::flushNTAlignBuffer()
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if (align_buffer_ptr)
    {
        NTAlignBufferAVX2 & buffer = align_buffer_ptr->getBuffer();
        UInt8 buffer_size = align_buffer_ptr->getSize();
        if (buffer_size > 0)
        {
            size_t prev_size = data.size();
            data.resize(prev_size + buffer_size / sizeof(T));
            inline_memcpy(&data[prev_size], buffer.data, buffer_size);
        }
        align_buffer_ptr.reset();
    }
#endif
}

template <typename T>
void ColumnVector<T>::updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnVector<T>::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
    const
{
    for (size_t i = 0, sz = size(); i < sz; ++i)
    {
        hash_values[i].update(data[i]);
    }
}

template <typename T>
void ColumnVector<T>::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    updateWeakHash32Impl<false>(hash, {});
}

template <typename T>
void ColumnVector<T>::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr &,
    String &,
    const BlockSelective & selective) const
{
    updateWeakHash32Impl<true>(hash, selective);
}

template <typename T>
template <bool selective_block>
void ColumnVector<T>::updateWeakHash32Impl(WeakHash32 & hash, const BlockSelective & selective) const
{
    size_t rows;
    if constexpr (selective_block)
    {
        rows = selective.size();
    }
    else
    {
        rows = data.size();
    }

    RUNTIME_CHECK_MSG(
        hash.getData().size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        hash.getData().size(),
        rows);

    const T * begin = data.data();
    UInt32 * hash_data = hash.getData().data();

    for (size_t i = 0; i < rows; ++i)
    {
        size_t row = i;
        if constexpr (selective_block)
            row = selective[i];

        if constexpr (is_fit_register<T>)
            *hash_data = intHashCRC32(*(begin + row), *hash_data);
        else
            *hash_data = wideIntHashCRC32(*(begin + row), *hash_data);

        ++hash_data;
    }
}

template <typename T>
struct ColumnVector<T>::less
{
    const Self & parent;
    int nan_direction_hint;
    less(const Self & parent_, int nan_direction_hint_)
        : parent(parent_)
        , nan_direction_hint(nan_direction_hint_)
    {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        return CompareHelper<T>::less(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

template <typename T>
struct ColumnVector<T>::greater
{
    const Self & parent;
    int nan_direction_hint;
    greater(const Self & parent_, int nan_direction_hint_)
        : parent(parent_)
        , nan_direction_hint(nan_direction_hint_)
    {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        return CompareHelper<T>::greater(parent.data[lhs], parent.data[rhs], nan_direction_hint);
    }
};

template <typename T>
void ColumnVector<T>::getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res)
    const
{
    size_t s = data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), greater(*this, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less(*this, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), greater(*this, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), less(*this, nan_direction_hint));
    }
}

template <typename T>
const char * ColumnVector<T>::getFamilyName() const
{
    return TypeName<T>::get();
}

template <typename T>
MutableColumnPtr ColumnVector<T>::cloneResized(size_t size) const
{
    auto res = this->create();

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);

        size_t count = std::min(this->size(), size);
        memcpy(&new_col.data[0], &data[0], count * sizeof(data[0]));

        if (size > count)
            memset(&new_col.data[count], static_cast<int>(value_type()), size - count);
    }

    return res;
}

template <typename T>
UInt64 ColumnVector<T>::get64(size_t n) const
{
    return ext::bit_cast<UInt64>(data[n]);
}

template <typename T>
UInt64 ColumnVector<T>::getUInt(size_t n) const
{
    return static_cast<UInt64>(data[n]);
}

template <typename T>
Int64 ColumnVector<T>::getInt(size_t n) const
{
    return static_cast<Int64>(data[n]);
}

template <typename T>
void ColumnVector<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_vec = static_cast<const ColumnVector &>(src);

    if (start + length > src_vec.data.size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnVector<T>::insertRangeFrom method, start={}, length={}, "
                "src.size()={}",
                start,
                length,
                src_vec.data.size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    memcpy(&data[old_size], &src_vec.data[start], length * sizeof(data[0]));
}

template <typename T>
ColumnPtr ColumnVector<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create();
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = &filt[0];
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = &data[0];

    filterImpl(filt_pos, filt_end, data_pos, res_data);

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(limit);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

template <typename T>
ColumnPtr ColumnVector<T>::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= size);

    if (0 == size)
        return this->create();

    auto res = this->create();
    typename Self::Container & res_data = res->getData();

    res_data.reserve(offsets[end_row - 1]);

    IColumn::Offset prev_offset = 0;

    for (size_t i = start_row; i < end_row; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            res_data.push_back(data[i]);
        }
    }

    return res;
}

template <typename T>
void ColumnVector<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnVector<T>::getExtremes(Field & min, Field & max) const
{
    size_t size = data.size();

    if (size == 0)
    {
        min = static_cast<typename NearestFieldType<T>::Type>(0);
        max = static_cast<typename NearestFieldType<T>::Type>(0);
        return;
    }

    bool has_value = false;

    /** Skip all NaNs in extremes calculation.
        * If all values are NaNs, then return NaN.
        * NOTE: There exist many different NaNs.
        * Different NaN could be returned: not bit-exact value as one of NaNs from column.
        */

    T cur_min = NaNOrZero<T>();
    T cur_max = NaNOrZero<T>();

    for (const T x : data)
    {
        if (isNaN(x))
            continue;

        if (!has_value)
        {
            cur_min = x;
            cur_max = x;
            has_value = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    min = static_cast<typename NearestFieldType<T>::Type>(cur_min);
    max = static_cast<typename NearestFieldType<T>::Type>(cur_max);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class ColumnVector<UInt8>;
template class ColumnVector<UInt16>;
template class ColumnVector<UInt32>;
template class ColumnVector<UInt64>;
template class ColumnVector<UInt128>;
template class ColumnVector<Int8>;
template class ColumnVector<Int16>;
template class ColumnVector<Int32>;
template class ColumnVector<Int64>;
template class ColumnVector<Float32>;
template class ColumnVector<Float64>;
} // namespace DB
