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
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/DataTypeDecimal.h>
#include <IO/WriteHelpers.h>
#include <common/memcpy.h>
#include <common/unaligned.h>

#include <ext/scope_guard.h>

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
ASSERT_USE_AVX2_COMPILE_FLAG
#endif

template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

template <typename T>
T DecodeDecimalImpl(size_t & cursor, const String & raw_value, PrecType prec, ScaleType frac);

template <typename T>
int ColumnDecimal<T>::compareAt(size_t n, size_t m, const IColumn & rhs_, int) const
{
    auto & other = static_cast<const Self &>(rhs_);
    const T & a = data[n];
    const T & b = other.data[m];

    return decimalLess<T>(b, a, other.scale, scale) ? 1 : (decimalLess<T>(a, b, scale, other.scale) ? -1 : 0);
}

template <typename T>
StringRef ColumnDecimal<T>::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const TiDB::TiDBCollatorPtr &,
    String &) const
{
    if constexpr (is_Decimal256)
    {
        /// serialize Decimal256 in `Non-trivial, Binary` way, the serialization logical is
        /// copied from https://github.com/pingcap/boost-extra/blob/master/boost/multiprecision/cpp_int/serialize.hpp#L149
        const typename T::NativeType::backend_type & val = data[n].value.backend();
        bool s = val.sign();
        size_t limb_count = val.size();

        size_t mem_size = sizeof(bool) + sizeof(size_t) + limb_count * sizeof(boost::multiprecision::limb_type);

        auto * pos = arena.allocContinue(mem_size, begin);
        auto * current_pos = pos;
        memcpy(current_pos, &s, sizeof(bool));
        current_pos += sizeof(bool);

        memcpy(current_pos, &limb_count, sizeof(std::size_t));
        current_pos += sizeof(size_t);

        memcpy(current_pos, val.limbs(), limb_count * sizeof(boost::multiprecision::limb_type));

        return StringRef(pos, mem_size);
    }
    else
    {
        auto * pos = arena.allocContinue(sizeof(T), begin);
        memcpy(pos, &data[n], sizeof(T));
        return StringRef(pos, sizeof(T));
    }
}

template <typename T>
const char * ColumnDecimal<T>::deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &)
{
    if constexpr (is_Decimal256)
    {
        /// deserialize Decimal256 in `Non-trivial, Binary` way, the deserialization logical is
        /// copied from https://github.com/pingcap/boost-extra/blob/master/boost/multiprecision/cpp_int/serialize.hpp#L133
        T value;
        auto & val = value.value.backend();

        size_t offset = 0;
        bool s = unalignedLoad<bool>(pos + offset);
        offset += sizeof(bool);
        auto limb_count = unalignedLoad<size_t>(pos + offset);
        offset += sizeof(size_t);

        val.resize(limb_count, limb_count);
        memcpy(val.limbs(), pos + offset, limb_count * sizeof(boost::multiprecision::limb_type));
        if (s != val.sign())
            val.negate();
        val.normalize();
        data.push_back(value);

        return pos + offset + limb_count * sizeof(boost::multiprecision::limb_type);
    }
    else
    {
        data.push_back(unalignedLoad<T>(pos));
        return pos + sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const
{
    if unlikely (byte_size.size() != size())
        throw Exception("byte_size.size() != column size", ErrorCodes::LOGICAL_ERROR);

    size_t size = byte_size.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += sizeof(T);
}

template <typename T>
void ColumnDecimal<T>::countSerializeByteSizeForColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets) const
{
    if unlikely (byte_size.size() != array_offsets.size())
        throw Exception("byte_size.size() != array_offsets.size()", ErrorCodes::LOGICAL_ERROR);

    size_t size = array_offsets.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += sizeof(T) * (array_offsets[i] - array_offsets[i - 1]);
}

template <typename T>
void ColumnDecimal<T>::serializeToPos(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    bool ensure_uniqueness) const
{
    /// Only Decimal256 needs special handling for the ensure_uniqueness case
    if (has_null)
    {
        if constexpr (is_Decimal256)
        {
            if (ensure_uniqueness)
            {
                serializeToPosImpl<true, true>(pos, start, length);
                return;
            }
        }
        serializeToPosImpl<true, false>(pos, start, length);
    }
    else
    {
        if constexpr (is_Decimal256)
        {
            if (ensure_uniqueness)
            {
                serializeToPosImpl<false, true>(pos, start, length);
                return;
            }
        }
        serializeToPosImpl<false, false>(pos, start, length);
    }
}

template <typename T>
template <bool has_null, bool ensure_uniqueness>
void ColumnDecimal<T>::serializeToPosImpl(PaddedPODArray<char *> & pos, size_t start, size_t length) const
{
    if unlikely (length > pos.size())
        throw Exception("length > pos.size()", ErrorCodes::LOGICAL_ERROR);
    if unlikely (start + length > size())
        throw Exception("start + length > size of column", ErrorCodes::LOGICAL_ERROR);

    T tmp_data{};
    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }
        if constexpr (ensure_uniqueness)
        {
            /// Clear the data and only set the necessary parts to ensure the uniqueness
            memset(static_cast<void *>(&tmp_data), 0, sizeof(T));

            const auto & val = data[start + i].value.backend();
            const auto limb_count = val.size();
            auto & tmp_val = tmp_data.value.backend();
            tmp_val.resize(limb_count, limb_count);
            inline_memcpy(tmp_val.limbs(), val.limbs(), limb_count * sizeof(boost::multiprecision::limb_type));
            if (val.sign() != tmp_val.sign())
                tmp_val.negate();
            tiflash_compiler_builtin_memcpy(pos[i], &tmp_data, sizeof(T));
        }
        else
        {
            tiflash_compiler_builtin_memcpy(pos[i], &data[start + i], sizeof(T));
        }
        pos[i] += sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::serializeToPosForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    bool ensure_uniqueness,
    const IColumn::Offsets & array_offsets) const
{
    /// Only Decimal256 needs special handling for the ensure_uniqueness case
    if (has_null)
    {
        if constexpr (is_Decimal256)
        {
            if (ensure_uniqueness)
            {
                serializeToPosForColumnArrayImpl<true, true>(pos, start, length, array_offsets);
                return;
            }
        }
        serializeToPosForColumnArrayImpl<true, false>(pos, start, length, array_offsets);
    }
    else
    {
        if constexpr (is_Decimal256)
        {
            if (ensure_uniqueness)
            {
                serializeToPosForColumnArrayImpl<false, true>(pos, start, length, array_offsets);
                return;
            }
        }
        serializeToPosForColumnArrayImpl<false, false>(pos, start, length, array_offsets);
    }
}

template <typename T>
template <bool has_null, bool ensure_uniqueness>
void ColumnDecimal<T>::serializeToPosForColumnArrayImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const IColumn::Offsets & array_offsets) const
{
    if unlikely (length > pos.size())
        throw Exception("length > pos.size()", ErrorCodes::LOGICAL_ERROR);
    if unlikely (start + length > array_offsets.size())
        throw Exception("start + length > array_offsets.size()", ErrorCodes::LOGICAL_ERROR);
    if unlikely (!array_offsets.empty() && array_offsets.back() != size())
        throw Exception("The last array offset doesn't match column size", ErrorCodes::LOGICAL_ERROR);

    T tmp_data{};
    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }
        for (size_t j = array_offsets[start + i - 1]; j < array_offsets[start + i]; ++j)
        {
            if constexpr (ensure_uniqueness)
            {
                /// Clear the data and only set the necessary parts to ensure the uniqueness
                memset(static_cast<void *>(&tmp_data), 0, sizeof(T));

                const auto & val = data[j].value.backend();
                const auto limb_count = val.size();
                auto & tmp_val = tmp_data.value.backend();
                tmp_val.resize(limb_count, limb_count);
                inline_memcpy(tmp_val.limbs(), val.limbs(), limb_count * sizeof(boost::multiprecision::limb_type));
                if (val.sign() != tmp_val.sign())
                    tmp_val.negate();
                tiflash_compiler_builtin_memcpy(pos[i], &tmp_data, sizeof(T));
            }
            else
            {
                tiflash_compiler_builtin_memcpy(pos[i], &data[j], sizeof(T));
            }
            pos[i] += sizeof(T);
        }
    }
}

template <typename T>
void ColumnDecimal<T>::deserializeAndInsertFromPos(
    PaddedPODArray<char *> & pos,
    ColumnsAlignBufferAVX2 & align_buffer [[maybe_unused]])
{
    size_t prev_size = data.size();
    size_t size = pos.size();

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if constexpr (AlignBufferAVX2::full_vector_size % sizeof(T) == 0)
    {
        size_t buffer_index = align_buffer.nextIndex();
        AlignBufferAVX2 & buffer = align_buffer.getAlignBuffer(buffer_index);
        UInt8 & buffer_size_ref = align_buffer.getSize(buffer_index);
        /// Better use a register rather than a reference for a frequently-updated variable
        UInt8 buffer_size = buffer_size_ref;
        SCOPE_EXIT({ buffer_size_ref = buffer_size; });

        bool is_aligned = reinterpret_cast<std::uintptr_t>(&data[prev_size]) % AlignBufferAVX2::full_vector_size == 0;
        if likely (is_aligned)
        {
            constexpr size_t avx2_width = AlignBufferAVX2::full_vector_size / sizeof(T);
            size_t i = 0;
            if (buffer_size != 0)
            {
                size_t count = std::min(size, (AlignBufferAVX2::full_vector_size - buffer_size) / sizeof(T));
                for (; i < count; ++i)
                {
                    tiflash_compiler_builtin_memcpy(&buffer.data[buffer_size], pos[i], sizeof(T));
                    buffer_size += sizeof(T);
                    pos[i] += sizeof(T);
                }

                if (buffer_size < AlignBufferAVX2::full_vector_size)
                {
                    if unlikely (align_buffer.needFlush())
                    {
                        data.resize(prev_size + buffer_size / sizeof(T), AlignBufferAVX2::full_vector_size);
                        inline_memcpy(static_cast<void *>(&data[prev_size]), buffer.data, buffer_size);
                        buffer_size = 0;
                    }
                    return;
                }

                assert(buffer_size == AlignBufferAVX2::full_vector_size);
                data.resize(prev_size + avx2_width, AlignBufferAVX2::full_vector_size);

                _mm256_stream_si256(reinterpret_cast<__m256i *>(&data[prev_size]), buffer.v[0]);
                prev_size += AlignBufferAVX2::vector_size / sizeof(T);
                _mm256_stream_si256(reinterpret_cast<__m256i *>(&data[prev_size]), buffer.v[1]);
                prev_size += AlignBufferAVX2::vector_size / sizeof(T);
                buffer_size = 0;
            }

            union alignas(AlignBufferAVX2::full_vector_size)
            {
                char vec_data[AlignBufferAVX2::full_vector_size]{};
                __m256i v[2];
            };
            size_t vec_size = 0;

            data.resize(prev_size + (size - i) / avx2_width * avx2_width, AlignBufferAVX2::full_vector_size);
            for (; i + avx2_width <= size; i += avx2_width)
            {
                for (size_t j = 0; j < avx2_width; ++j)
                {
                    tiflash_compiler_builtin_memcpy(&vec_data[vec_size], pos[i + j], sizeof(T));
                    vec_size += sizeof(T);
                    pos[i + j] += sizeof(T);
                }

                _mm256_stream_si256(reinterpret_cast<__m256i *>(&data[prev_size]), v[0]);
                prev_size += AlignBufferAVX2::vector_size / sizeof(T);
                _mm256_stream_si256(reinterpret_cast<__m256i *>(&data[prev_size]), v[1]);
                prev_size += AlignBufferAVX2::vector_size / sizeof(T);
                vec_size = 0;
            }

            for (; i < size; ++i)
            {
                tiflash_compiler_builtin_memcpy(&buffer.data[buffer_size], pos[i], sizeof(T));
                buffer_size += sizeof(T);
                pos[i] += sizeof(T);
            }

            if unlikely (align_buffer.needFlush())
            {
                data.resize(prev_size + buffer_size / sizeof(T), AlignBufferAVX2::full_vector_size);
                inline_memcpy(static_cast<void *>(&data[prev_size]), buffer.data, buffer_size);
                buffer_size = 0;
            }

            return;
        }

        /// Bad case when this column data is aligned first and then becomes unaligned due to calling other functions
        if unlikely (buffer_size != 0)
        {
            data.resize(prev_size + buffer_size / sizeof(T), AlignBufferAVX2::full_vector_size);
            inline_memcpy(static_cast<void *>(&data[prev_size]), buffer.data, buffer_size);
            prev_size += buffer_size / sizeof(T);
            buffer_size = 0;
        }
    }
#endif

    data.resize(prev_size + size);
    for (size_t i = 0; i < size; ++i)
    {
        tiflash_compiler_builtin_memcpy(&data[prev_size], pos[i], sizeof(T));
        ++prev_size;
        pos[i] += sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::deserializeAndInsertFromPosForColumnArray(
    PaddedPODArray<char *> & pos,
    const IColumn::Offsets & array_offsets)
{
    if unlikely (pos.empty())
        return;
    if unlikely (pos.size() > array_offsets.size())
        throw Exception("Invalid size of pos or offsets", ErrorCodes::LOGICAL_ERROR);
    size_t start_point = array_offsets.size() - pos.size();
    if unlikely (array_offsets[start_point - 1] != data.size())
        throw Exception("Offset doesn't match column size", ErrorCodes::LOGICAL_ERROR);

    size_t prev_size = data.size();
    data.resize(array_offsets.back());

    size_t size = pos.size();
    for (size_t i = 0; i < size; ++i)
    {
        size_t length = array_offsets[start_point + i] - array_offsets[start_point + i - 1];
        for (size_t j = 0; j < length; ++j)
        {
            tiflash_compiler_builtin_memcpy(&data[prev_size], pos[i], sizeof(T));
            ++prev_size;
            pos[i] += sizeof(T);
        }
    }
    assert(prev_size == array_offsets.back());
}

template <typename T>
UInt64 ColumnDecimal<T>::get64(size_t n) const
{
    if constexpr (sizeof(T) > sizeof(UInt64))
        throw Exception(String("Method get64 is not supported for ") + getFamilyName(), ErrorCodes::NOT_IMPLEMENTED);
    return static_cast<UInt64>(static_cast<typename T::NativeType>(data[n]));
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    hash.update(data[n]);
}

template <typename T>
void ColumnDecimal<T>::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
    const
{
    for (size_t i = 0; i < data.size(); ++i)
    {
        hash_values[i].update(data[i]);
    }
}

template <typename T>
void ColumnDecimal<T>::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    updateWeakHash32Impl<false>(hash, {});
}

template <typename T>
void ColumnDecimal<T>::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr &,
    String &,
    const BlockSelective & selective) const
{
    updateWeakHash32Impl<true>(hash, selective);
}

template <typename T>
template <bool selective_block>
void ColumnDecimal<T>::updateWeakHash32Impl(WeakHash32 & hash, const BlockSelective & selective) const
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

        *hash_data = wideIntHashCRC32(*(begin + row), *hash_data);
        ++hash_data;
    }
}

template <typename T>
void ColumnDecimal<T>::getPermutation(bool reverse, size_t limit, int, IColumn::Permutation & res) const
{
#if 1 /// TODO: perf test
    if (data.size() <= std::numeric_limits<UInt32>::max())
    {
        PaddedPODArray<UInt32> tmp_res;
        permutation(reverse, limit, tmp_res);

        res.resize(tmp_res.size());
        for (size_t i = 0; i < tmp_res.size(); ++i)
            res[i] = tmp_res[i];
        return;
    }
#endif

    permutation(reverse, limit, res);
}

template <typename T>
ColumnPtr ColumnDecimal<T>::permute(const IColumn::Permutation & perm, size_t limit) const
{
    size_t size = limit ? std::min(data.size(), limit) : data.size();
    if (perm.size() < size)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(size, scale);
    typename Self::Container & res_data = res->getData();

    for (size_t i = 0; i < size; ++i)
        res_data[i] = data[perm[i]];

    return res;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#ifndef __clang__
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
template <typename T>
MutableColumnPtr ColumnDecimal<T>::cloneResized(size_t size) const
{
    auto res = this->create(0, scale);

    if (size > 0)
    {
        auto & new_col = static_cast<Self &>(*res);
        new_col.data.resize(size);
        size_t count = std::min(this->size(), size);
        if constexpr (is_Decimal256)
        {
            for (size_t i = 0; i != count; ++i)
                new_col.data[i] = data[i];

            if (size > count)
            {
                T zero{};
                for (size_t i = count; i != size; ++i)
                    new_col.data[i] = zero;
            }
        }
        else
        {
            memcpy(new_col.data.data(), data.data(), count * sizeof(data[0]));

            if (size > count)
            {
                void * tail = &new_col.data[count];
                memset(tail, 0, (size - count) * sizeof(T));
            }
        }
    }

    return res;
}

template <typename T>
void ColumnDecimal<T>::insertData(const char * src [[maybe_unused]], size_t /*length*/)
{
    if constexpr (is_Decimal256)
    {
        throw Exception("insertData is not supported for " + IColumn::getName());
    }
    else
    {
        T tmp{};
        memcpy(&tmp, src, sizeof(T));
        data.emplace_back(tmp);
    }
}

template <typename T>
bool ColumnDecimal<T>::decodeTiDBRowV2Datum(
    size_t cursor,
    const String & raw_value,
    size_t /* length */,
    bool /* force_decode */)
{
    PrecType dec_prec = static_cast<uint8_t>(raw_value[cursor++]);
    ScaleType dec_scale = static_cast<uint8_t>(raw_value[cursor++]);
    auto dec_type = createDecimal(dec_prec, dec_scale);
    if (unlikely(!checkDecimal<T>(*dec_type)))
    {
        throw Exception(
            "Detected unmatched decimal value type: Decimal( " + std::to_string(dec_prec) + ", "
                + std::to_string(dec_scale) + ") when decoding with column type " + this->getName(),
            ErrorCodes::LOGICAL_ERROR);
    }
    auto res = DecodeDecimalImpl<T>(cursor, raw_value, dec_prec, dec_scale);
    data.push_back(DecimalField<T>(res, dec_scale));
    return true;
}

template <typename T>
void ColumnDecimal<T>::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & src_vec = static_cast<const ColumnDecimal &>(src);

    if (start + length > src_vec.data.size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnDecimal<T>::insertRangeFrom method, start={}, length={}, "
                "src.size()={}",
                start,
                length,
                src_vec.data.size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = data.size();
    data.resize(old_size + length);
    if constexpr (is_Decimal256)
    {
        for (size_t i = 0; i != length; ++i)
            data[i + old_size] = src_vec.data[i + start];
    }
    else
    {
        memcpy(data.data() + old_size, &src_vec.data[start], length * sizeof(data[0]));
    }
}

template <typename T>
void ColumnDecimal<T>::insertManyFrom(const IColumn & src, size_t position, size_t length)
{
    size_t old_size = data.size();
    auto & value = static_cast<const Self &>(src).getData()[position];
    data.resize_fill(old_size + length, value);
}

template <typename T>
void ColumnDecimal<T>::insertDisjunctFrom(const IColumn & src, const std::vector<size_t> & position_vec)
{
    const auto & src_data = static_cast<const ColumnDecimal &>(src).data;
    size_t old_size = data.size();
    size_t to_add_size = position_vec.size();
    data.resize(old_size + to_add_size);
    for (size_t i = 0; i < to_add_size; ++i)
        data[i + old_size] = src_data[position_vec[i]];
}

#pragma GCC diagnostic pop

template <typename T>
ColumnPtr ColumnDecimal<T>::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(0, scale);
    Container & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filt);
        res_data.reserve(result_size_hint);
    }

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + size;
    const T * data_pos = data.data();

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
            res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnDecimal<T>::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= size);

    auto res = this->create(0, scale);
    if (0 == size)
        return res;

    typename Self::Container & res_data = res->getData();
    res_data.reserve(offsets[end_row - 1]);

    IColumn::Offset prev_offset = 0;
    for (size_t i = start_row; i < end_row; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_data.push_back(data[i]);
    }

    return res;
}


template <typename T>
void ColumnDecimal<T>::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

template <typename T>
void ColumnDecimal<T>::getExtremes(Field & min, Field & max) const
{
    if (data.size() == 0)
    {
        min = typename NearestFieldType<T>::Type(T(0), scale);
        max = typename NearestFieldType<T>::Type(T(0), scale);
        return;
    }

    T cur_min = data[0];
    T cur_max = data[0];

    for (const T & x : data)
    {
        if (x.value < cur_min.value)
            cur_min = x;
        else if (x.value > cur_max.value)
            cur_max = x;
    }

    min = typename NearestFieldType<T>::Type(cur_min, scale);
    max = typename NearestFieldType<T>::Type(cur_max, scale);
}

template class ColumnDecimal<Decimal32>;
template class ColumnDecimal<Decimal64>;
template class ColumnDecimal<Decimal128>;
template class ColumnDecimal<Decimal256>;

} // namespace DB
