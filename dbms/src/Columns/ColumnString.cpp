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

#include <Columns/ColumnString.h>
#include <Columns/filterColumn.h>
#include <Common/HashTable/Hash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <common/memcpy.h>
#include <fmt/core.h>

#include <ext/scope_guard.h>

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

MutableColumnPtr ColumnString::cloneResized(size_t to_size) const
{
    auto res = ColumnString::create();

    if (to_size == 0)
        return res;

    size_t from_size = size();

    if (to_size <= from_size)
    {
        /// Just cut column.

        res->offsets.assign(offsets.begin(), offsets.begin() + to_size);
        res->chars.assign(chars.begin(), chars.begin() + offsets[to_size - 1]);
    }
    else
    {
        /// Copy column and append empty strings for extra elements.

        Offset offset = 0;
        if (from_size > 0)
        {
            res->offsets.assign(offsets.begin(), offsets.end());
            res->chars.assign(chars.begin(), chars.end());
            offset = offsets.back();
        }

        /// Empty strings are just zero terminating bytes.

        res->chars.resize_fill_zero(res->chars.size() + to_size - from_size);

        res->offsets.resize(to_size);
        for (size_t i = from_size; i < to_size; ++i)
        {
            ++offset;
            res->offsets[i] = offset;
        }
    }

    return res;
}


void ColumnString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const auto & src_concrete = static_cast<const ColumnString &>(src);

    if (start + length > src_concrete.offsets.size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnString::insertRangeFrom method, start={}, length={}, "
                "src.size()={}",
                start,
                length,
                src_concrete.size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    inline_memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty())
    {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    }
    else
    {
        size_t old_size = offsets.size();
        size_t prev_max_offset = old_size ? offsets.back() : 0;
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            offsets[old_size + i] = src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
    }
}


ColumnPtr ColumnString::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (offsets.empty())
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    filterArraysImpl<UInt8>(chars, offsets, res_chars, res_offsets, filt, result_size_hint);
    return res;
}


ColumnPtr ColumnString::permute(const Permutation & perm, size_t limit) const
{
    size_t size = offsets.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    if (limit == size)
        res_chars.resize(chars.size());
    else
    {
        size_t new_chars_size = 0;
        for (size_t i = 0; i < limit; ++i)
            new_chars_size += sizeAt(perm[i]);
        res_chars.resize(new_chars_size);
    }

    res_offsets.resize(limit);

    Offset current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        size_t j = perm[i];
        size_t string_offset = j == 0 ? 0 : offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}


template <bool positive>
struct ColumnString::less
{
    const ColumnString & parent;
    explicit less(const ColumnString & parent_)
        : parent(parent_)
    {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        size_t left_len = parent.sizeAt(lhs);
        size_t right_len = parent.sizeAt(rhs);

        int res = memcmp(
            &parent.chars[parent.offsetAt(lhs)],
            &parent.chars[parent.offsetAt(rhs)],
            std::min(left_len, right_len));

        if (res != 0)
            return positive ? (res < 0) : (res > 0);
        else
            return positive ? (left_len < right_len) : (left_len > right_len);
    }
};

void ColumnString::getPermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    size_t s = offsets.size();
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

ColumnPtr ColumnString::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & replicate_offsets)
    const
{
    size_t col_rows = size();
    if (col_rows != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= col_rows);

    auto res = ColumnString::create();

    if (0 == col_rows)
        return res;

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;
    res_chars.reserve(chars.size() / col_rows * (replicate_offsets[end_row - 1]));
    res_offsets.reserve(replicate_offsets[end_row - 1]);

    Offset prev_replicate_offset = 0;
    Offset prev_string_offset = start_row == 0 ? 0 : offsets[start_row - 1];
    Offset current_new_offset = 0;

    for (size_t i = start_row; i < end_row; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t string_size = offsets[i] - prev_string_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += string_size;
            res_offsets.push_back(current_new_offset);

            res_chars.resize(res_chars.size() + string_size);
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[res_chars.size() - string_size],
                &chars[prev_string_offset],
                string_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_string_offset = offsets[i];
    }

    return res;
}


void ColumnString::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}


void ColumnString::reserve(size_t n)
{
    offsets.reserve(n);
    chars.reserve(n * APPROX_STRING_SIZE);
}

void ColumnString::reserveAlign(size_t n, size_t alignment)
{
    offsets.reserve(n, alignment);
    chars.reserve(n * APPROX_STRING_SIZE, alignment);
}

void ColumnString::reserveWithTotalMemoryHint(size_t n, Int64 total_memory_hint)
{
    offsets.reserve(n);
    total_memory_hint -= n * sizeof(offsets[0]);
    if (total_memory_hint >= 0)
        chars.reserve(total_memory_hint);
    else
        chars.reserve(n * APPROX_STRING_SIZE);
}

void ColumnString::reserveAlignWithTotalMemoryHint(size_t n, Int64 total_memory_hint, size_t alignment)
{
    offsets.reserve(n, alignment);
    total_memory_hint -= n * sizeof(offsets[0]);
    if (total_memory_hint >= 0)
        chars.reserve(total_memory_hint, alignment);
    else
        chars.reserve(n * APPROX_STRING_SIZE, alignment);
}

void ColumnString::getExtremes(Field & min, Field & max) const
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


int ColumnString::compareAtWithCollationImpl(
    size_t n,
    size_t m,
    const IColumn & rhs_,
    const TiDB::ITiDBCollator & collator) const
{
    const auto & rhs = static_cast<const ColumnString &>(rhs_);

    auto a = getDataAt(n);
    auto b = rhs.getDataAt(m);

    return collator.compare(a.data, a.size, b.data, b.size);
}

// Derived must implement function `int compare(const char *, size_t, const char *, size_t)`.
template <bool positive, typename Derived>
struct ColumnString::LessWithCollation
{
    const ColumnString & parent;
    const Derived & inner;

    LessWithCollation(const ColumnString & parent_, const Derived & inner_)
        : parent(parent_)
        , inner(inner_)
    {}

    FLATTEN_INLINE_PURE inline bool operator()(size_t lhs, size_t rhs) const
    {
        int res = inner.compare(
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]),
            parent.sizeAt(lhs) - 1, // Skip last zero byte.
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]),
            parent.sizeAt(rhs) - 1 // Skip last zero byte.
        );

        if constexpr (positive)
        {
            return (res < 0);
        }
        else
        {
            return (res > 0);
        }
    }
};

template <bool padding>
struct CompareBinCollator
{
    static FLATTEN_INLINE_PURE inline int compare(const char * s1, size_t length1, const char * s2, size_t length2)
    {
        return DB::BinCollatorCompare<padding>(s1, length1, s2, length2);
    }
};

// common util functions
template <>
struct ColumnString::LessWithCollation<false, void>
{
    // `CollationCmpImpl` must implement function `int compare(const char *, size_t, const char *, size_t)`.
    template <typename CollationCmpImpl>
    static void getPermutationWithCollationImpl(
        const ColumnString & src,
        const CollationCmpImpl & collator_cmp_impl,
        bool reverse,
        size_t limit,
        Permutation & res)
    {
        size_t s = src.offsets.size();
        res.resize(s);
        for (size_t i = 0; i < s; ++i)
            res[i] = i;

        if (limit >= s)
            limit = 0;

        if (limit)
        {
            if (reverse)
                std::partial_sort(
                    res.begin(),
                    res.begin() + limit,
                    res.end(),
                    LessWithCollation<false, CollationCmpImpl>(src, collator_cmp_impl));
            else
                std::partial_sort(
                    res.begin(),
                    res.begin() + limit,
                    res.end(),
                    LessWithCollation<true, CollationCmpImpl>(src, collator_cmp_impl));
        }
        else
        {
            if (reverse)
                std::sort(res.begin(), res.end(), LessWithCollation<false, CollationCmpImpl>(src, collator_cmp_impl));
            else
                std::sort(res.begin(), res.end(), LessWithCollation<true, CollationCmpImpl>(src, collator_cmp_impl));
        }
    }
};

void ColumnString::getPermutationWithCollationImpl(
    const TiDB::ITiDBCollator & collator,
    bool reverse,
    size_t limit,
    Permutation & res) const
{
    using PermutationWithCollationUtils = ColumnString::LessWithCollation<false, void>;

    switch (TiDB::GetTiDBCollatorType(&collator))
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    {
        CompareBinCollator<true> cmp_impl;
        PermutationWithCollationUtils::getPermutationWithCollationImpl(*this, cmp_impl, reverse, limit, res);
        break;
    }
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    {
        CompareBinCollator<false> cmp_impl;
        PermutationWithCollationUtils::getPermutationWithCollationImpl(*this, cmp_impl, reverse, limit, res);
        break;
    }
    default:
    {
        PermutationWithCollationUtils::getPermutationWithCollationImpl(*this, collator, reverse, limit, res);
    }
    }
}

void ColumnString::countSerializeByteSizeUnique(
    PaddedPODArray<size_t> & byte_size,
    const TiDB::TiDBCollatorPtr & collator) const
{
    if likely (collator != nullptr)
        countSerializeByteSizeImpl<true>(byte_size, collator);
    else
        countSerializeByteSizeImpl<false>(byte_size, nullptr);
}

void ColumnString::countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const
{
    countSerializeByteSizeImpl<false>(byte_size, nullptr);
}

template <bool has_collator>
void ColumnString::countSerializeByteSizeImpl(
    PaddedPODArray<size_t> & byte_size,
    const TiDB::TiDBCollatorPtr & collator) const
{
    RUNTIME_CHECK_MSG(byte_size.size() == size(), "size of byte_size({}) != column size({})", byte_size.size(), size());

    if unlikely (!offsets.empty() && offsets.back() > UINT32_MAX)
    {
        size_t sz = size();
        for (size_t i = 0; i < sz; ++i)
            RUNTIME_CHECK_MSG(
                sizeAt(i) <= UINT32_MAX,
                "size of ({}) is ({}), which is greater than UINT32_MAX",
                i,
                sizeAt(i));
    }

    if constexpr (has_collator)
    {
        RUNTIME_CHECK(collator);

        const size_t size = byte_size.size();
        const size_t max_bytes_one_char = collator->maxBytesForOneChar();
        for (size_t i = 0; i < size; ++i)
        {
            assert(sizeAt(i) >= 1);
            // Minus 1 because of terminating zero.
            byte_size[i] += sizeof(UInt32) + (sizeAt(i) - 1) * max_bytes_one_char;
        }
    }
    else
    {
        size_t size = byte_size.size();
        for (size_t i = 0; i < size; ++i)
            byte_size[i] += sizeof(UInt32) + sizeAt(i);
    }
}

void ColumnString::countSerializeByteSizeUniqueForColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets,
    const TiDB::TiDBCollatorPtr & collator) const
{
    if likely (collator != nullptr)
        countSerializeByteSizeForColumnArrayImpl<true>(byte_size, array_offsets, collator);
    else
        countSerializeByteSizeForColumnArrayImpl<false>(byte_size, array_offsets, nullptr);
}

void ColumnString::countSerializeByteSizeForColumnArray(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets) const
{
    countSerializeByteSizeForColumnArrayImpl<false>(byte_size, array_offsets, nullptr);
}

template <bool has_collator>
void ColumnString::countSerializeByteSizeForColumnArrayImpl(
    PaddedPODArray<size_t> & byte_size,
    const IColumn::Offsets & array_offsets,
    const TiDB::TiDBCollatorPtr & collator) const
{
    RUNTIME_CHECK_MSG(
        byte_size.size() == array_offsets.size(),
        "size of byte_size({}) != size of array_offsets({})",
        byte_size.size(),
        array_offsets.size());
    RUNTIME_CHECK_MSG(
        array_offsets.empty() || array_offsets.back() == size(),
        "The last array offset({}) doesn't match size of column({})",
        array_offsets.back(),
        size());

    if unlikely (!offsets.empty() && offsets.back() > UINT32_MAX)
    {
        size_t sz = size();
        for (size_t i = 0; i < sz; ++i)
            RUNTIME_CHECK_MSG(
                sizeAt(i) <= UINT32_MAX,
                "size of ({}) is ({}), which is greater than UINT32_MAX",
                i,
                sizeAt(i));
    }

    if constexpr (has_collator)
    {
        RUNTIME_CHECK(collator);

        size_t size = array_offsets.size();
        const auto max_bytes_one_char = collator->maxBytesForOneChar();
        for (size_t i = 0; i < size; ++i)
        {
            assert(offsetAt(array_offsets[i]) - offsetAt(array_offsets[i - 1]) >= 1);
            const size_t ele_count = array_offsets[i] - array_offsets[i - 1];
            byte_size[i] += sizeof(UInt32) * (ele_count)
                // For each sub element, minus 1 because of terminating zero.
                + max_bytes_one_char * (offsetAt(array_offsets[i]) - offsetAt(array_offsets[i - 1]) - ele_count);
        }
    }
    else
    {
        size_t size = array_offsets.size();
        for (size_t i = 0; i < size; ++i)
            byte_size[i] += sizeof(UInt32) * (array_offsets[i] - array_offsets[i - 1]) + offsetAt(array_offsets[i])
                - offsetAt(array_offsets[i - 1]);
    }
}

void ColumnString::serializeToPosUnique(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const TiDB::TiDBCollatorPtr & collator,
    String * sort_key_container) const
{
    if (has_null)
    {
        if likely (collator != nullptr)
            serializeToPosImpl</*has_null=*/true, /*has_collator=*/true>(
                pos,
                start,
                length,
                collator,
                sort_key_container);
        else
            serializeToPosImpl</*has_null=*/true, /*has_collator=*/false>(pos, start, length, nullptr, nullptr);
    }
    else
    {
        if likely (collator != nullptr)
            serializeToPosImpl</*has_null=*/false, /*has_collator=*/true>(
                pos,
                start,
                length,
                collator,
                sort_key_container);
        else
            serializeToPosImpl</*has_null=*/false, /*has_collator=*/false>(pos, start, length, nullptr, nullptr);
    }
}

void ColumnString::serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const
{
    if (has_null)
        serializeToPosImpl</*has_null=*/true, /*has_collator=*/false>(pos, start, length, nullptr, nullptr);
    else
        serializeToPosImpl</*has_null=*/false, /*has_collator=*/false>(pos, start, length, nullptr, nullptr);
}

template <bool has_null, bool has_collator>
void ColumnString::serializeToPosImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const TiDB::TiDBCollatorPtr & collator,
    String * sort_key_container) const
{
    RUNTIME_CHECK_MSG(length <= pos.size(), "length({}) > size of pos({})", length, pos.size());
    RUNTIME_CHECK_MSG(start + length <= size(), "start({}) + length({}) > size of column({})", start, length, size());
    if constexpr (has_collator)
        RUNTIME_CHECK(collator && sort_key_container);

    /// countSerializeByteSizeUnique has already checked that the size of one element is not greater than UINT32_MAX
    for (size_t i = 0; i < length; ++i)
    {
        if constexpr (has_null)
        {
            if (pos[i] == nullptr)
                continue;
        }

        UInt32 str_size = sizeAt(start + i);
        const void * src = &chars[offsetAt(start + i)];
        if constexpr (has_collator)
        {
            auto sort_key = collator->sortKey(reinterpret_cast<const char *>(src), str_size - 1, *sort_key_container);
            str_size = sort_key.size;
            src = sort_key.data;
        }
        tiflash_compiler_builtin_memcpy(pos[i], &str_size, sizeof(UInt32));
        pos[i] += sizeof(UInt32);
        inline_memcpy(pos[i], src, str_size);
        pos[i] += str_size;
    }
}

void ColumnString::serializeToPosUniqueForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const IColumn::Offsets & array_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    String * sort_key_container) const
{
    if (has_null)
    {
        if likely (collator != nullptr)
            serializeToPosForColumnArrayImpl</*has_null=*/true, /*has_collator=*/true>(
                pos,
                start,
                length,
                array_offsets,
                collator,
                sort_key_container);
        else
            serializeToPosForColumnArrayImpl</*has_null=*/true, /*has_collator=*/false>(
                pos,
                start,
                length,
                array_offsets,
                nullptr,
                nullptr);
    }
    else
    {
        if likely (collator != nullptr)
            serializeToPosForColumnArrayImpl</*has_null=*/false, /*has_collator=*/true>(
                pos,
                start,
                length,
                array_offsets,
                collator,
                sort_key_container);
        else
            serializeToPosForColumnArrayImpl</*has_null=*/false, /*has_collator=*/true>(
                pos,
                start,
                length,
                array_offsets,
                nullptr,
                nullptr);
    }
}

void ColumnString::serializeToPosForColumnArray(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    bool has_null,
    const IColumn::Offsets & array_offsets) const
{
    if (has_null)
        serializeToPosForColumnArrayImpl</*has_null=*/true, /*has_collator=*/false>(
            pos,
            start,
            length,
            array_offsets,
            nullptr,
            nullptr);
    else
        serializeToPosForColumnArrayImpl</*has_null=*/false, /*has_collator=*/false>(
            pos,
            start,
            length,
            array_offsets,
            nullptr,
            nullptr);
}

template <bool has_null, bool has_collator>
void ColumnString::serializeToPosForColumnArrayImpl(
    PaddedPODArray<char *> & pos,
    size_t start,
    size_t length,
    const IColumn::Offsets & array_offsets,
    const TiDB::TiDBCollatorPtr & collator,
    String * sort_key_container) const
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

    /// countSerializeByteSizeUniqueForColumnArray has already checked that the size of one element is not greater than UINT32_MAX
    if constexpr (has_collator)
    {
        RUNTIME_CHECK(collator && sort_key_container);
        for (size_t i = 0; i < length; ++i)
        {
            if constexpr (has_null)
            {
                if (pos[i] == nullptr)
                    continue;
            }
            for (size_t j = array_offsets[start + i - 1]; j < array_offsets[start + i]; ++j)
            {
                UInt32 str_size = sizeAt(j);
                const void * src = &chars[offsetAt(j)];
                auto sort_key
                    = collator->sortKey(reinterpret_cast<const char *>(src), str_size - 1, *sort_key_container);
                str_size = sort_key.size;
                src = sort_key.data;

                tiflash_compiler_builtin_memcpy(pos[i], &str_size, sizeof(UInt32));
                pos[i] += sizeof(UInt32);
                inline_memcpy(pos[i], src, str_size);
                pos[i] += str_size;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < length; ++i)
        {
            if constexpr (has_null)
            {
                if (pos[i] == nullptr)
                    continue;
            }
            for (size_t j = array_offsets[start + i - 1]; j < array_offsets[start + i]; ++j)
            {
                UInt32 str_size = sizeAt(j);
                tiflash_compiler_builtin_memcpy(pos[i], &str_size, sizeof(UInt32));
                pos[i] += sizeof(UInt32);
            }
            size_t strs_size = offsetAt(array_offsets[start + i]) - offsetAt(array_offsets[start + i - 1]);
            inline_memcpy(pos[i], &chars[offsetAt(array_offsets[start + i - 1])], strs_size);
            pos[i] += strs_size;
        }
    }
}

void ColumnString::deserializeAndInsertFromPosUnique(
    PaddedPODArray<const char *> & pos,
    bool use_nt_align_buffer,
    const TiDB::TiDBCollatorPtr & collator)
{
    if likely (collator != nullptr)
        deserializeAndInsertFromPosImpl<true>(pos, use_nt_align_buffer);
    else
        deserializeAndInsertFromPosImpl<false>(pos, use_nt_align_buffer);
}

void ColumnString::deserializeAndInsertFromPos(PaddedPODArray<const char *> & pos, bool use_nt_align_buffer)
{
    deserializeAndInsertFromPosImpl<false>(pos, use_nt_align_buffer);
}

template <bool add_terminating_zero>
void ColumnString::deserializeAndInsertFromPosImpl(
    PaddedPODArray<const char *> & pos,
    bool use_nt_align_buffer [[maybe_unused]])
{
    size_t prev_size = offsets.size();
    size_t char_size = chars.size();
    size_t size = pos.size();

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if constexpr (!add_terminating_zero)
    {
        if (use_nt_align_buffer)
        {
            bool is_offset_aligned = reinterpret_cast<std::uintptr_t>(&offsets[prev_size]) % FULL_VECTOR_SIZE_AVX2 == 0;
            bool is_char_aligned = reinterpret_cast<std::uintptr_t>(&chars[char_size]) % FULL_VECTOR_SIZE_AVX2 == 0;
            if likely (is_offset_aligned && is_char_aligned)
            {
                if unlikely (align_buffer_ptrs == nullptr)
                    align_buffer_ptrs = std::make_unique<ColumnNTAlignBufferAVX2[]>(2);

                NTAlignBufferAVX2 & saved_char_buffer = align_buffer_ptrs[0].getBuffer();
                UInt8 char_buffer_size = align_buffer_ptrs[0].getSize();
                NTAlignBufferAVX2 & offset_buffer = align_buffer_ptrs[1].getBuffer();
                UInt8 offset_buffer_size = align_buffer_ptrs[1].getSize();

                /// Add 15 bytes padding in order to use memcpyMax64BAllowReadWriteOverflow15
                struct PaddedNTAlignBuffer
                {
                    NTAlignBufferAVX2 buffer;
                    char padding[15]{};
                } padded_align_buf;

                NTAlignBufferAVX2 & char_buffer = padded_align_buf.buffer;

                tiflash_compiler_builtin_memcpy(&char_buffer, &saved_char_buffer, sizeof(NTAlignBufferAVX2));
                SCOPE_EXIT({
                    tiflash_compiler_builtin_memcpy(&saved_char_buffer, &char_buffer, sizeof(NTAlignBufferAVX2));
                    align_buffer_ptrs[0].setSize(char_buffer_size);
                    align_buffer_ptrs[1].setSize(offset_buffer_size);
                });

                offsets.reserve(offsets.size() + size + offset_buffer_size / sizeof(size_t));
                for (size_t i = 0; i < size; ++i)
                {
                    UInt32 str_size;
                    tiflash_compiler_builtin_memcpy(&str_size, pos[i], sizeof(UInt32));
                    pos[i] += sizeof(UInt32);

                    const auto * p = pos[i];
                    while (true)
                    {
                        UInt8 remain = FULL_VECTOR_SIZE_AVX2 - char_buffer_size;
                        if (remain > str_size)
                        {
                            memcpyMax64BAllowReadWriteOverflow15(&char_buffer.data[char_buffer_size], p, str_size);
                            p += str_size;
                            char_buffer_size += str_size;
                            break;
                        }

                        memcpyMax64BAllowReadWriteOverflow15(&char_buffer.data[char_buffer_size], p, remain);
                        p += remain;
                        chars.resize(char_size + FULL_VECTOR_SIZE_AVX2, FULL_VECTOR_SIZE_AVX2);
                        nonTemporalStore64B(&chars[char_size], char_buffer);
                        char_size += FULL_VECTOR_SIZE_AVX2;
                        char_buffer_size = 0;
                        if (remain == str_size)
                            break;
                        str_size -= remain;
                    }
                    pos[i] = p;

                    size_t offset = char_size + char_buffer_size;
                    tiflash_compiler_builtin_memcpy(&offset_buffer.data[offset_buffer_size], &offset, sizeof(size_t));
                    offset_buffer_size += sizeof(size_t);
                    static_assert(FULL_VECTOR_SIZE_AVX2 % sizeof(size_t) == 0);
                    if unlikely (offset_buffer_size == FULL_VECTOR_SIZE_AVX2)
                    {
                        offsets.resize(prev_size + FULL_VECTOR_SIZE_AVX2 / sizeof(size_t), FULL_VECTOR_SIZE_AVX2);
                        nonTemporalStore64B(&offsets[prev_size], offset_buffer);
                        prev_size += FULL_VECTOR_SIZE_AVX2 / sizeof(size_t);
                        offset_buffer_size = 0;
                    }
                }

                _mm_sfence();
                return;
            }
        }

        RUNTIME_CHECK_MSG(
            align_buffer_ptrs == nullptr,
            "align_buffer_ptrs is not nullptr but use_nt_align_buffer({}) is false or data is unaligned",
            use_nt_align_buffer);
    }
#endif

    offsets.resize(prev_size + size);
    for (size_t i = 0; i < size; ++i)
    {
        UInt32 str_size;
        tiflash_compiler_builtin_memcpy(&str_size, pos[i], sizeof(UInt32));
        pos[i] += sizeof(UInt32);

        if constexpr (add_terminating_zero)
            chars.resize(char_size + str_size + 1);
        else
            chars.resize(char_size + str_size);

        memcpySmallAllowReadWriteOverflow15(&chars[char_size], pos[i], str_size);
        char_size += str_size;

        // todo add unit test
        if constexpr (add_terminating_zero)
        {
            chars[char_size] = 0;
            char_size++;
        }
        offsets[prev_size + i] = char_size;
        pos[i] += str_size;
    }
}

void ColumnString::deserializeAndInsertFromPosUniqueForColumnArray(
    PaddedPODArray<const char *> & pos,
    const IColumn::Offsets & array_offsets,
    bool use_nt_align_buffer,
    const TiDB::TiDBCollatorPtr & collator)
{
    if likely (collator != nullptr)
        deserializeAndInsertFromPosForColumnArrayImpl<true>(pos, array_offsets, use_nt_align_buffer);
    else
        deserializeAndInsertFromPosForColumnArrayImpl<false>(pos, array_offsets, use_nt_align_buffer);
}

void ColumnString::deserializeAndInsertFromPosForColumnArray(
    PaddedPODArray<const char *> & pos,
    const IColumn::Offsets & array_offsets,
    bool use_nt_align_buffer)
{
    deserializeAndInsertFromPosForColumnArrayImpl<false>(pos, array_offsets, use_nt_align_buffer);
}

template <bool add_terminating_zero>
void ColumnString::deserializeAndInsertFromPosForColumnArrayImpl(
    PaddedPODArray<const char *> & pos,
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

    offsets.resize(array_offsets.back());

    size_t size = pos.size();
    size_t char_size = chars.size();
    if constexpr (add_terminating_zero)
    {
        for (size_t i = 0; i < size; ++i)
        {
            // TODO add unit test
            for (size_t j = array_offsets[start_point + i - 1]; j < array_offsets[start_point + i]; ++j)
            {
                UInt32 str_size;
                tiflash_compiler_builtin_memcpy(&str_size, pos[i], sizeof(UInt32));
                pos[i] += sizeof(UInt32);

                chars.resize(char_size + str_size + 1);
                memcpySmallAllowReadWriteOverflow15(&chars[char_size], pos[i], str_size);

                char_size += str_size;
                chars[char_size] = 0;
                char_size++;
                offsets[j] = char_size;

                pos[i] += str_size;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
        {
            size_t prev_char_size = char_size;
            for (size_t j = array_offsets[start_point + i - 1]; j < array_offsets[start_point + i]; ++j)
            {
                UInt32 str_size;
                tiflash_compiler_builtin_memcpy(&str_size, pos[i], sizeof(UInt32));
                pos[i] += sizeof(UInt32);
                char_size += str_size;
                offsets[j] = char_size;
            }
            chars.resize(char_size);
            memcpySmallAllowReadWriteOverflow15(&chars[prev_char_size], pos[i], char_size - prev_char_size);
            pos[i] += char_size - prev_char_size;
        }
    }
}

void ColumnString::flushNTAlignBuffer()
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    if (align_buffer_ptrs)
    {
        size_t prev_size = offsets.size();
        size_t char_size = chars.size();
        NTAlignBufferAVX2 & char_buffer = align_buffer_ptrs[0].getBuffer();
        UInt8 char_buffer_size = align_buffer_ptrs[0].getSize();
        if (char_buffer_size != 0)
        {
            chars.resize(char_size + char_buffer_size);
            inline_memcpy(&chars[char_size], char_buffer.data, char_buffer_size);
        }
        NTAlignBufferAVX2 & offset_buffer = align_buffer_ptrs[1].getBuffer();
        UInt8 offset_buffer_size = align_buffer_ptrs[1].getSize();
        if (offset_buffer_size != 0)
        {
            offsets.resize(prev_size + offset_buffer_size / sizeof(size_t));
            inline_memcpy(&offsets[prev_size], offset_buffer.data, offset_buffer_size);
        }
        align_buffer_ptrs.reset();
    }
#endif
}

void updateWeakHash32BinPadding(const std::string_view & view, size_t idx, ColumnString::WeakHash32Info & info)
{
    auto sort_key = BinCollatorSortKey<true>(view.data(), view.size());
    (*info.hash_data)[idx]
        = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, (*info.hash_data)[idx]);
}

void updateWeakHash32BinNoPadding(const std::string_view & view, size_t idx, ColumnString::WeakHash32Info & info)
{
    auto sort_key = BinCollatorSortKey<false>(view.data(), view.size());
    (*info.hash_data)[idx]
        = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, (*info.hash_data)[idx]);
}

void updateWeakHash32NonBin(const std::string_view & view, size_t idx, ColumnString::WeakHash32Info & info)
{
    auto sort_key = info.collator->sortKey(view.data(), view.size(), info.sort_key_container);
    (*info.hash_data)[idx]
        = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, (*info.hash_data)[idx]);
}

void updateWeakHash32NoCollator(const std::string_view & view, size_t idx, ColumnString::WeakHash32Info & info)
{
    (*info.hash_data)[idx]
        = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(view.data()), view.size(), (*info.hash_data)[idx]);
}

using LoopColumnWithHashInfoFunc = void(const std::string_view &, size_t, ColumnString::WeakHash32Info &);
template <bool selective_block>
FLATTEN_INLINE static inline void LoopOneColumnWithHashInfo(
    const ColumnString::Chars_t & a_data,
    const IColumn::Offsets & a_offsets,
    ColumnString::WeakHash32Info & info,
    LoopColumnWithHashInfoFunc && func)
{
    size_t rows;
    if constexpr (selective_block)
    {
        RUNTIME_CHECK(info.selective_ptr);
        rows = info.selective_ptr->size();
    }
    else
    {
        rows = a_offsets.size();
    }

    RUNTIME_CHECK_MSG(
        info.hash_data->size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        info.hash_data->size(),
        rows);

    for (size_t i = 0; i < rows; ++i)
    {
        size_t row = i;
        if constexpr (selective_block)
            row = (*info.selective_ptr)[i];

        size_t a_prev_offset = 0;
        if likely (row > 0)
            a_prev_offset = a_offsets[row - 1];

        auto a_size = a_offsets[row] - a_prev_offset;

        func({reinterpret_cast<const char *>(&a_data[a_prev_offset]), a_size - 1}, i, info);
    }
}

template <typename LoopFunc>
void ColumnString::updateWeakHash32Impl(WeakHash32Info & info, const LoopFunc & loop_func) const
{
    if (info.collator != nullptr)
    {
        switch (info.collator->getCollatorType())
        {
        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
        case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
        case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
        case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
        {
            loop_func(chars, offsets, info, updateWeakHash32BinPadding);
            break;
        }
        case TiDB::ITiDBCollator::CollatorType::BINARY:
        {
            loop_func(chars, offsets, info, updateWeakHash32BinNoPadding);
            break;
        }
        default:
        {
            loop_func(chars, offsets, info, updateWeakHash32NonBin);
            break;
        }
        }
    }
    else
    {
        loop_func(chars, offsets, info, updateWeakHash32NoCollator);
    }
}

void ColumnString::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    WeakHash32Info info{
        .hash_data = &hash.getData(),
        .sort_key_container = sort_key_container,
        .collator = collator,
        .selective_ptr = nullptr,
    };

    updateWeakHash32Impl(info, LoopOneColumnWithHashInfo<false>);
}

void ColumnString::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container,
    const BlockSelective & selective) const
{
    WeakHash32Info info{
        .hash_data = &hash.getData(),
        .sort_key_container = sort_key_container,
        .collator = collator,
        .selective_ptr = &selective,
    };
    updateWeakHash32Impl(info, LoopOneColumnWithHashInfo<true>);
}

void ColumnString::updateHashWithValues(
    IColumn::HashValues & hash_values,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    if (collator != nullptr)
    {
        switch (collator->getCollatorType())
        {
        case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
        case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
        case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
        case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
        {
            // Skip last zero byte.
            LoopOneColumn(chars, offsets, offsets.size(), [&hash_values](const std::string_view & view, size_t i) {
                auto sort_key = BinCollatorSortKey<true>(view.data(), view.size());
                size_t string_size = sort_key.size;
                hash_values[i].update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
                hash_values[i].update(sort_key.data, sort_key.size);
            });
            break;
        }
        case TiDB::ITiDBCollator::CollatorType::BINARY:
        {
            // Skip last zero byte.
            LoopOneColumn(chars, offsets, offsets.size(), [&hash_values](const std::string_view & view, size_t i) {
                auto sort_key = BinCollatorSortKey<false>(view.data(), view.size());
                size_t string_size = sort_key.size;
                hash_values[i].update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
                hash_values[i].update(sort_key.data, sort_key.size);
            });
            break;
        }
        default:
        {
            // Skip last zero byte.
            LoopOneColumn(chars, offsets, offsets.size(), [&](const std::string_view & view, size_t i) {
                auto sort_key = collator->sortKey(view.data(), view.size(), sort_key_container);
                size_t string_size = sort_key.size;
                hash_values[i].update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
                hash_values[i].update(sort_key.data, sort_key.size);
            });
            break;
        }
        }
    }
    else
    {
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t string_size = sizeAt(i);
            size_t offset = offsetAt(i);

            hash_values[i].update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
            hash_values[i].update(reinterpret_cast<const char *>(&chars[offset]), string_size);
        }
    }
}


} // namespace DB
