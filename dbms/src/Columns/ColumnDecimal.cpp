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
#include <Columns/countBytesInFilter.h>
#include <Columns/filterColumn.h>
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

ALWAYS_INLINE inline size_t getDecimal256BytesSize(const Decimal256 & val)
{
    return sizeof(bool) + sizeof(size_t) + val.value.backend().size() * sizeof(boost::multiprecision::limb_type);
}

ALWAYS_INLINE inline char * serializeDecimal256Helper(char * dst, const Decimal256 & data)
{
    /// deserialize Decimal256 in `Non-trivial, Binary` way, the deserialization logical is
    /// copied from https://github.com/pingcap/boost-extra/blob/master/boost/multiprecision/cpp_int/serialize.hpp#L133
    const auto & val = data.value.backend();

    const bool s = val.sign();
    tiflash_compiler_builtin_memcpy(dst, &s, sizeof(bool));
    dst += sizeof(bool);

    const size_t limb_count = val.size();
    tiflash_compiler_builtin_memcpy(dst, &limb_count, sizeof(size_t));
    dst += sizeof(size_t);

    const size_t limb_size = limb_count * sizeof(boost::multiprecision::limb_type);
    memcpy(dst, val.limbs(), limb_size);
    dst += limb_size;
    return dst;
}

ALWAYS_INLINE inline const char * deserializeDecimal256Helper(Decimal256 & new_value, const char * ptr)
{
    Decimal256 value;
    auto & val = value.value.backend();

    size_t offset = 0;
    bool s = unalignedLoad<bool>(ptr + offset);
    offset += sizeof(bool);
    auto limb_count = unalignedLoad<size_t>(ptr + offset);
    offset += sizeof(size_t);

    val.resize(limb_count, limb_count);
    memcpy(val.limbs(), ptr + offset, limb_count * sizeof(boost::multiprecision::limb_type));
    if (s != val.sign())
        val.negate();
    val.normalize();
    offset += limb_count * sizeof(boost::multiprecision::limb_type);

    new_value = value;
    return ptr + offset;
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
        size_t mem_size = getDecimal256BytesSize(data[n]);
        auto * pos = arena.allocContinue(mem_size, begin);
        pos = serializeDecimal256Helper(pos, data[n]);
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
        data.resize(data.size() + 1);
        return deserializeDecimal256Helper(data.back(), pos);
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
    RUNTIME_CHECK_MSG(byte_size.size() == size(), "size of byte_size({}) != column size({})", byte_size.size(), size());

    size_t size = byte_size.size();
    for (size_t i = 0; i < size; ++i)
        byte_size[i] += sizeof(T);
}

template <typename T>
void ColumnDecimal<T>::countSerializeByteSizeForCmpColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets,
    const NullMap * nullmap,
    const TiDB::TiDBCollatorPtr &) const
{
    if (nullmap != nullptr)
        countSerializeByteSizeForColumnArrayImpl<true>(byte_size, array_offsets, nullmap);
    else
        countSerializeByteSizeForColumnArrayImpl<false>(byte_size, array_offsets, nullptr);
}

template <typename T>
void ColumnDecimal<T>::countSerializeByteSizeForColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets) const
{
    countSerializeByteSizeForColumnArrayImpl<false>(byte_size, array_offsets, nullptr);
}

template <typename T>
template <bool has_nullmap>
void ColumnDecimal<T>::countSerializeByteSizeForColumnArrayImpl(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets,
    const NullMap * nullmap) const
{
    RUNTIME_CHECK_MSG(
        byte_size.size() == array_offsets.size(),
        "size of byte_size({}) != size of array_offsets({})",
        byte_size.size(),
        array_offsets.size());

    size_t size = array_offsets.size();
    for (size_t i = 0; i < size; ++i)
    {
        if constexpr (has_nullmap)
        {
            if (DB::isNullAt(*nullmap, i))
                continue;
        }
        byte_size[i] += sizeof(T) * (array_offsets[i] - array_offsets[i - 1]);
    }
}

template <typename T>
void ColumnDecimal<T>::serializeToPosForCmp(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const NullMap * nullmap,
    const TiDB::TiDBCollatorPtr &,
    String *) const
{
#define CALL(has_null, has_nullmap)                                                   \
    {                                                                                 \
        serializeToPosImpl<has_null, true, has_nullmap>(pos, start, length, nullmap); \
    }

    if (has_null)
    {
        if (nullmap != nullptr)
            CALL(true, true)
        else
            CALL(true, false)
    }
    else
    {
        if (nullmap != nullptr)
            CALL(false, true)
        else
            CALL(false, false)
    }

#undef CALL
}

template <typename T>
void ColumnDecimal<T>::serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const
{
    if (has_null)
        serializeToPosImpl</*has_null=*/true, /*compare_semantics=*/false, /*has_nullmap=*/false>(
            pos,
            start,
            length,
            nullptr);
    else
        serializeToPosImpl</*has_null=*/false, /*compare_semantics=*/false, /*has_nullmap=*/false>(
            pos,
            start,
            length,
            nullptr);
}

template <typename T>
template <bool has_null, bool compare_semantics, bool has_nullmap>
void ColumnDecimal<T>::serializeToPosImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const NullMap * nullmap) const
{
    RUNTIME_CHECK_MSG(length <= pos.size(), "length({}) > size of pos({})", length, pos.size());
    RUNTIME_CHECK_MSG(start + length <= size(), "start({}) + length({}) > size of column({})", start, length, size());

    RUNTIME_CHECK(!has_nullmap || (nullmap && nullmap->size() == size()));

    static constexpr T def_val{};
    T tmp_val{};
    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }
        if constexpr (has_nullmap)
        {
            if (DB::isNullAt(*nullmap, start + i))
            {
                tiflash_compiler_builtin_memcpy(pos[i], &def_val, sizeof(T));
                pos[i] += sizeof(T);
                continue;
            }
        }

        if constexpr (compare_semantics && is_Decimal256)
        {
            // Clear the value and only set the necessary parts for compare semantic
            memset(static_cast<void *>(&tmp_val), 0, sizeof(T));
            tmp_val.value.backend().assign(data[start + i].value.backend());
            tiflash_compiler_builtin_memcpy(pos[i], &tmp_val, sizeof(T));
        }
        else
        {
            tiflash_compiler_builtin_memcpy(pos[i], &data[start + i], sizeof(T));
        }
        pos[i] += sizeof(T);
    }
}

template <typename T>
void ColumnDecimal<T>::serializeToPosForCmpColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const NullMap * nullmap,
    const IColumn::Offsets & array_offsets,
    const TiDB::TiDBCollatorPtr &,
    String *) const
{
#define CALL(has_null, has_nullmap)                                                          \
    {                                                                                        \
        serializeToPosForColumnArrayImpl<has_null, /*compare_semantics=*/true, has_nullmap>( \
            pos,                                                                             \
            start,                                                                           \
            length,                                                                          \
            array_offsets,                                                                   \
            nullmap);                                                                        \
    }

    if (has_null)
    {
        if (nullmap != nullptr)
            CALL(true, true)
        else
            CALL(true, false)
    }
    else
    {
        if (nullmap != nullptr)
            CALL(false, true)
        else
            CALL(false, false)
    }

#undef CALL
}

template <typename T>
void ColumnDecimal<T>::serializeToPosForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const IColumn::Offsets & array_offsets) const
{
    if (has_null)
        serializeToPosForColumnArrayImpl</*has_null=*/true, /*compare_semantics=*/false, /*has_nullmap=*/false>(
            pos,
            start,
            length,
            array_offsets,
            nullptr);
    else
        serializeToPosForColumnArrayImpl</*has_null=*/false, /*compare_semantics=*/false, /*has_nullmap=*/false>(
            pos,
            start,
            length,
            array_offsets,
            nullptr);
}

template <typename T>
template <bool has_null, bool compare_semantics, bool has_nullmap>
void ColumnDecimal<T>::serializeToPosForColumnArrayImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const IColumn::Offsets & array_offsets,
    const NullMap * nullmap) const
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

    RUNTIME_CHECK(!has_nullmap || (nullmap && nullmap->size() == array_offsets.size()));

    T tmp_val{};
    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }
        if constexpr (has_nullmap)
        {
            if (DB::isNullAt(*nullmap, start + i))
                continue;
        }

        size_t len = array_offsets[start + i] - array_offsets[start + i - 1];
        size_t start_idx = array_offsets[start + i - 1];
        if constexpr (compare_semantics && is_Decimal256)
        {
            auto * p = pos[i];
            for (size_t j = 0; j < len; ++j)
            {
                // Clear the value and only set the necessary parts for compare semantics
                memset(static_cast<void *>(&tmp_val), 0, sizeof(T));
                tmp_val.value.backend().assign(data[start_idx + j].value.backend());

                tiflash_compiler_builtin_memcpy(p, &tmp_val, sizeof(T));
                p += sizeof(T);
            }
            pos[i] = p;
        }
        else
        {
            if (len <= 4)
            {
                auto * p = pos[i];
                for (size_t j = 0; j < len; ++j)
                {
                    tiflash_compiler_builtin_memcpy(p, &data[start_idx + j], sizeof(T));
                    p += sizeof(T);
                }
                pos[i] = p;
            }
            else
            {
                inline_memcpy(pos[i], &data[start_idx], len * sizeof(T));
                pos[i] += len * sizeof(T);
            }
        }
    }
}

template <typename T>
void ColumnDecimal<T>::deserializeAndInsertFromPos(
    PaddedPODArray<char *> & pos,
    bool use_nt_align_buffer [[maybe_unused]])
{
    size_t prev_size = data.size();
    size_t size = pos.size();

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if (use_nt_align_buffer)
    {
        if constexpr ((FULL_VECTOR_SIZE_AVX2 % sizeof(T) == 0))
        {
            bool is_aligned = reinterpret_cast<std::uintptr_t>(&data[prev_size]) % FULL_VECTOR_SIZE_AVX2 == 0;
            if likely (is_aligned)
            {
                if (align_buffer_ptr == nullptr)
                    align_buffer_ptr = std::make_unique<ColumnNTAlignBufferAVX2>();

                NTAlignBufferAVX2 & buffer = align_buffer_ptr->getBuffer();
                UInt8 buffer_size = align_buffer_ptr->getSize();
                SCOPE_EXIT({ align_buffer_ptr->setSize(buffer_size); });

                constexpr size_t avx2_width = FULL_VECTOR_SIZE_AVX2 / sizeof(T);
                size_t i = 0;
                if unlikely (buffer_size != 0)
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
void ColumnDecimal<T>::deserializeAndInsertFromPosForColumnArray(
    PaddedPODArray<char *> & pos,
    const IColumn::Offsets & array_offsets,
    bool use_nt_align_buffer [[maybe_unused]])
{
    // Check if pos is empty is necessary.
    // If pos is not empty, then array_offsets is not empty either due to pos.size() <= array_offsets.size().
    // Then reading array_offsets[-1] and array_offsets.back() is valid.
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
        size_t start_idx = array_offsets[start_point + i - 1];
        if (len <= 4)
        {
            auto * p = pos[i];
            for (size_t j = 0; j < len; ++j)
            {
                tiflash_compiler_builtin_memcpy(&data[start_idx + j], p, sizeof(T));
                p += sizeof(T);
            }
            pos[i] = p;
        }
        else
        {
            inline_memcpy(&data[start_idx], pos[i], len * sizeof(T));
            pos[i] += len * sizeof(T);
        }
    }
}

template <typename T>
void ColumnDecimal<T>::flushNTAlignBuffer()
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
void ColumnDecimal<T>::deserializeAndAdvancePosForColumnArray(
    PaddedPODArray<char *> & pos,
    const IColumn::Offsets & array_offsets) const
{
    RUNTIME_CHECK_MSG(
        pos.size() == array_offsets.size(),
        "size of pos({}) != size of array_offsets({})",
        pos.size(),
        array_offsets.size());
    size_t size = pos.size();
    for (size_t i = 0; i < size; ++i)
    {
        size_t len = array_offsets[i] - array_offsets[i - 1];
        pos[i] += len * sizeof(T);
    }
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
void ColumnDecimal<T>::reserveWithStrategy(size_t n, IColumn::ReserveStrategy strategy)
{
    switch (strategy)
    {
    case IColumn::ReserveStrategy::Default:
        data.reserve(n);
        break;
    case IColumn::ReserveStrategy::ScaleFactor1_5:
        data.reserve_exact(n / 2 * 3);
        break;
    }
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
void ColumnDecimal<T>::insertSelectiveRangeFrom(
    const IColumn & src,
    const IColumn::Offsets & selective_offsets,
    size_t start,
    size_t length)
{
    RUNTIME_CHECK(selective_offsets.size() >= start + length);
    const auto & src_data = static_cast<const ColumnDecimal &>(src).data;
    size_t old_size = data.size();
    data.resize(old_size + length);
    for (size_t i = 0; i < length; ++i)
        data[i + old_size] = src_data[selective_offsets[i + start]];
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

    filterImpl(filt_pos, filt_end, data_pos, res_data);

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
