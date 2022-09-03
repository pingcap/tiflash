// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/Collator.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/Hash.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Storages/Transaction/CollatorUtils.h>
#include <fmt/core.h>


/// Used in the `reserve` method, when the number of rows is known, but sizes of elements are not.
#define APPROX_STRING_SIZE 64


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

        res->chars.resize_fill(res->chars.size() + to_size - from_size);

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
        throw Exception("Parameter out of bound in IColumnString::insertRangeFrom method.",
                        ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

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

        int res = memcmp(&parent.chars[parent.offsetAt(lhs)], &parent.chars[parent.offsetAt(rhs)], std::min(left_len, right_len));

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


ColumnPtr ColumnString::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnString::create();

    if (0 == col_size)
        return res;

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;
    res_chars.reserve(chars.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_string_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
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


int ColumnString::compareAtWithCollationImpl(size_t n, size_t m, const IColumn & rhs_, const ICollator & collator) const
{
    const auto & rhs = static_cast<const ColumnString &>(rhs_);

    return collator.compare(
        reinterpret_cast<const char *>(&chars[offsetAt(n)]),
        sizeAt(n) - 1, // Skip last zero byte.
        reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]),
        rhs.sizeAt(m) - 1 // Skip last zero byte.
    );
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
    static void getPermutationWithCollationImpl(const ColumnString & src, const CollationCmpImpl & collator_cmp_impl, bool reverse, size_t limit, Permutation & res)
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
                std::partial_sort(res.begin(), res.begin() + limit, res.end(), LessWithCollation<false, CollationCmpImpl>(src, collator_cmp_impl));
            else
                std::partial_sort(res.begin(), res.begin() + limit, res.end(), LessWithCollation<true, CollationCmpImpl>(src, collator_cmp_impl));
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

void ColumnString::getPermutationWithCollationImpl(const ICollator & collator, bool reverse, size_t limit, Permutation & res) const
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

void ColumnString::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr & collator, String & sort_key_container) const
{
    auto s = offsets.size();

    if (hash.getData().size() != s)
        throw Exception(fmt::format("Size of WeakHash32 does not match size of column: column size is {}, hash size is {}", s, hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    UInt32 * hash_data = hash.getData().data();

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
            LoopOneColumn(chars, offsets, offsets.size(), [&](const std::string_view & view, size_t) {
                auto sort_key = BinCollatorSortKey<true>(view.data(), view.size());
                *hash_data = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, *hash_data);
                ++hash_data;
            });
            break;
        }
        case TiDB::ITiDBCollator::CollatorType::BINARY:
        {
            // Skip last zero byte.
            LoopOneColumn(chars, offsets, offsets.size(), [&](const std::string_view & view, size_t) {
                auto sort_key = BinCollatorSortKey<false>(view.data(), view.size());
                *hash_data = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, *hash_data);
                ++hash_data;
            });
            break;
        }
        default:
        {
            // Skip last zero byte.
            LoopOneColumn(chars, offsets, offsets.size(), [&](const std::string_view & view, size_t) {
                auto sort_key = collator->sortKey(view.data(), view.size(), sort_key_container);
                *hash_data = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(sort_key.data), sort_key.size, *hash_data);
                ++hash_data;
            });
            break;
        }
        }
    }
    else
    {
        // Skip last zero byte.
        LoopOneColumn(chars, offsets, offsets.size(), [&](const std::string_view & view, size_t) {
            *hash_data = ::updateWeakHash32(reinterpret_cast<const UInt8 *>(view.data()), view.size(), *hash_data);
            ++hash_data;
        });
    }
}

void ColumnString::updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr & collator, String & sort_key_container) const
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
