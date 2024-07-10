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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/Arena.h>
#include <Common/NaNUtils.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataStreams/ColumnGathererStream.h>
#include <fmt/core.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT;
} // namespace ErrorCodes


ColumnNullable::ColumnNullable(MutableColumnPtr && nested_column_, MutableColumnPtr && null_map_)
    : nested_column(std::move(nested_column_))
    , null_map(std::move(null_map_))
{
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    if (ColumnPtr nested_column_materialized = getNestedColumn().convertToFullColumnIfConst())
        nested_column = nested_column_materialized;

    if (!getNestedColumn().canBeInsideNullable())
        throw Exception(
            fmt::format("{} cannot be inside Nullable column", getNestedColumn().getName()),
            ErrorCodes::ILLEGAL_COLUMN);

    if (null_map->isColumnConst())
        throw Exception("ColumnNullable cannot have constant null map", ErrorCodes::ILLEGAL_COLUMN);
}


void ColumnNullable::updateHashWithValue(
    size_t n,
    SipHash & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    const auto & arr = getNullMapData();
    if (arr[n] == 0)
    {
        getNestedColumn().updateHashWithValue(n, hash, collator, sort_key_container);
    }
    else
    {
        hash.update(arr[n]);
    }
}

void ColumnNullable::updateHashWithValues(
    IColumn::HashValues & hash_values,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    const auto & arr = getNullMapData();
    size_t null_count = 0;

    /// TODO: use an more efficient way to calc the sum.
    for (const auto i : arr)
    {
        null_count += i;
    }

    if (null_count == 0)
    {
        getNestedColumn().updateHashWithValues(hash_values, collator, sort_key_container);
    }
    else
    {
        IColumn::HashValues original_values;
        original_values.reserve(null_count);
        for (size_t i = 0, n = arr.size(); i < n; ++i)
        {
            if (arr[i])
                original_values.push_back(hash_values[i]);
        }
        getNestedColumn().updateHashWithValues(hash_values, collator, sort_key_container);
        for (size_t i = 0, j = 0, n = arr.size(); i < n; ++i)
        {
            if (arr[i])
                hash_values[i] = original_values[j++];
        }
    }
}

void ColumnNullable::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    updateWeakHash32Impl<false>(hash, collator, sort_key_container, nullptr);
}

void ColumnNullable::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container,
    const BlockSelectivePtr & selective_ptr) const
{
    updateWeakHash32Impl<true>(hash, collator, sort_key_container, selective_ptr);
}

template <bool selective>
void ColumnNullable::updateWeakHash32Impl(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container,
    const BlockSelectivePtr & selective_ptr) const
{
    size_t rows = size();
    if constexpr (selective)
    {
        RUNTIME_CHECK(selective_ptr);
        rows = selective_ptr->size();
    }

    RUNTIME_CHECK_MSG(
        hash.getData().size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        hash.getData().size(),
        rows);

    WeakHash32 old_hash = hash;
    if constexpr (selective)
        nested_column->updateWeakHash32(hash, collator, sort_key_container, selective_ptr);
    else
        nested_column->updateWeakHash32(hash, collator, sort_key_container);

    const auto & null_map_data = getNullMapData();
    auto & hash_data = hash.getData();
    auto & old_hash_data = old_hash.getData();

    for (size_t i = 0; i < rows; ++i)
    {
        size_t row = i;
        if constexpr (selective)
            row = (*selective_ptr)[i];

        if (null_map_data[row])
            hash_data[i] = old_hash_data[i];
    }
}

MutableColumnPtr ColumnNullable::cloneResized(size_t new_size) const
{
    MutableColumnPtr new_nested_col = getNestedColumn().cloneResized(new_size);
    auto new_null_map = ColumnUInt8::create();

    if (new_size > 0)
    {
        new_null_map->getData().resize(new_size);

        size_t count = std::min(size(), new_size);
        memcpy(new_null_map->getData().data(), getNullMapData().data(), count * sizeof(getNullMapData()[0]));

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count)
            memset(&new_null_map->getData()[count], 1, new_size - count);
    }

    return ColumnNullable::create(std::move(new_nested_col), std::move(new_null_map));
}


Field ColumnNullable::operator[](size_t n) const
{
    return isNullAt(n) ? Null() : getNestedColumn()[n];
}


void ColumnNullable::get(size_t n, Field & res) const
{
    if (isNullAt(n))
        res = Null();
    else
        getNestedColumn().get(n, res);
}

StringRef ColumnNullable::getDataAt(size_t /*n*/) const
{
    throw Exception(fmt::format("Method getDataAt is not supported for {}", getName()), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnNullable::insertData(const char * /*pos*/, size_t /*length*/)
{
    throw Exception(fmt::format("Method insertData is not supported for {}", getName()), ErrorCodes::NOT_IMPLEMENTED);
}

bool ColumnNullable::decodeTiDBRowV2Datum(size_t cursor, const String & raw_value, size_t length, bool force_decode)
{
    if (!getNestedColumn().decodeTiDBRowV2Datum(cursor, raw_value, length, force_decode))
        return false;
    getNullMapData().push_back(0);
    return true;
}

StringRef ColumnNullable::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    const auto & arr = getNullMapData();
    static constexpr auto s = sizeof(arr[0]);

    auto * pos = arena.allocContinue(s, begin);
    memcpy(pos, &arr[n], s);

    size_t nested_size = 0;

    if (arr[n] == 0)
        nested_size = getNestedColumn().serializeValueIntoArena(n, arena, begin, collator, sort_key_container).size;

    return StringRef{begin, s + nested_size};
}

const char * ColumnNullable::deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr & collator)
{
    UInt8 val = *reinterpret_cast<const UInt8 *>(pos);
    pos += sizeof(val);

    getNullMapData().push_back(val);

    if (val == 0)
        pos = getNestedColumn().deserializeAndInsertFromArena(pos, collator);
    else
        getNestedColumn().insertDefault();

    return pos;
}

void ColumnNullable::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const auto & nullable_col = static_cast<const ColumnNullable &>(src);
    getNullMapColumn().insertRangeFrom(*nullable_col.null_map, start, length);
    getNestedColumn().insertRangeFrom(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert(const Field & x)
{
    if (x.isNull())
    {
        getNestedColumn().insertDefault();
        getNullMapData().push_back(1);
    }
    else
    {
        getNestedColumn().insert(x);
        getNullMapData().push_back(0);
    }
}

void ColumnNullable::insertFrom(const IColumn & src, size_t n)
{
    const auto & src_concrete = static_cast<const ColumnNullable &>(src);
    getNestedColumn().insertFrom(src_concrete.getNestedColumn(), n);
    getNullMapData().push_back(src_concrete.getNullMapData()[n]);
}

void ColumnNullable::insertManyFrom(const IColumn & src, size_t n, size_t length)
{
    const auto & src_concrete = static_cast<const ColumnNullable &>(src);
    getNestedColumn().insertManyFrom(src_concrete.getNestedColumn(), n, length);
    auto & map = getNullMapData();
    map.resize_fill(map.size() + length, src_concrete.getNullMapData()[n]);
}

void ColumnNullable::insertDisjunctFrom(const IColumn & src, const std::vector<size_t> & position_vec)
{
    const auto & src_concrete = static_cast<const ColumnNullable &>(src);
    getNestedColumn().insertDisjunctFrom(src_concrete.getNestedColumn(), position_vec);
    auto & map = getNullMapData();
    const auto & src_map = src_concrete.getNullMapData();
    size_t old_size = map.size();
    size_t to_add_size = position_vec.size();
    map.resize(old_size + to_add_size);
    for (size_t i = 0; i < to_add_size; ++i)
        map[i + old_size] = src_map[position_vec[i]];
}

void ColumnNullable::popBack(size_t n)
{
    getNestedColumn().popBack(n);
    getNullMapColumn().popBack(n);
}

ColumnPtr ColumnNullable::filter(const Filter & filt, ssize_t result_size_hint) const
{
    ColumnPtr filtered_data = getNestedColumn().filter(filt, result_size_hint);
    ColumnPtr filtered_null_map = getNullMapColumn().filter(filt, result_size_hint);
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

ColumnPtr ColumnNullable::permute(const Permutation & perm, size_t limit) const
{
    ColumnPtr permuted_data = getNestedColumn().permute(perm, limit);
    ColumnPtr permuted_null_map = getNullMapColumn().permute(perm, limit);
    return ColumnNullable::create(permuted_data, permuted_null_map);
}

std::tuple<bool, int> ColumnNullable::compareAtCheckNull(
    size_t n,
    size_t m,
    const ColumnNullable & rhs,
    int null_direction_hint) const
{
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compareAt is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.

    bool lval_is_null = isNullAt(n);
    bool rval_is_null = rhs.isNullAt(m);

    int res = 0;
    bool has_null = false;
    if (unlikely(lval_is_null || rval_is_null))
    {
        has_null = true;
        if (lval_is_null && rval_is_null)
            res = 0;
        else
            res = lval_is_null ? null_direction_hint : -null_direction_hint;
    }
    else
        has_null = false;

    return std::make_tuple(has_null, res);
}

int ColumnNullable::compareAt(
    size_t n,
    size_t m,
    const IColumn & rhs_,
    int null_direction_hint,
    const TiDB::ITiDBCollator & collator) const
{
    const auto & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);
    auto [has_null, res] = compareAtCheckNull(n, m, nullable_rhs, null_direction_hint);
    if (has_null)
        return res;
    const IColumn & nested_rhs = nullable_rhs.getNestedColumn();
    return getNestedColumn().compareAt(n, m, nested_rhs, null_direction_hint, collator);
}

int ColumnNullable::compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const
{
    const auto & nullable_rhs = static_cast<const ColumnNullable &>(rhs_);
    auto [has_null, res] = compareAtCheckNull(n, m, nullable_rhs, null_direction_hint);
    if (has_null)
        return res;
    const IColumn & nested_rhs = nullable_rhs.getNestedColumn();
    return getNestedColumn().compareAt(n, m, nested_rhs, null_direction_hint);
}

void ColumnNullable::getPermutation(
    const TiDB::ITiDBCollator & collator,
    bool reverse,
    size_t limit,
    int null_direction_hint,
    DB::IColumn::Permutation & res) const
{
    /// Cannot pass limit because of unknown amount of NULLs.
    getNestedColumn().getPermutation(collator, reverse, 0, null_direction_hint, res);
    adjustPermutationWithNullDirection(reverse, limit, null_direction_hint, res);
}

void ColumnNullable::getPermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res) const
{
    /// Cannot pass limit because of unknown amount of NULLs.
    getNestedColumn().getPermutation(reverse, 0, null_direction_hint, res);
    adjustPermutationWithNullDirection(reverse, limit, null_direction_hint, res);
}

void ColumnNullable::adjustPermutationWithNullDirection(
    bool reverse,
    size_t limit,
    int null_direction_hint,
    Permutation & res) const
{
    if ((null_direction_hint > 0) != reverse)
    {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res.size();

        if (!limit)
            limit = end_idx;
        else
            limit = std::min(end_idx, limit);

        while (read_idx < limit && !isNullAt(res[read_idx]))
        {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }
    }
    else
    {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !isNullAt(res[read_idx]))
        {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0)
        {
            if (!isNullAt(res[read_idx]))
            {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }
    }
}

void ColumnNullable::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnNullable::reserve(size_t n)
{
    getNestedColumn().reserve(n);
    getNullMapData().reserve(n);
}

void ColumnNullable::reserveWithTotalMemoryHint(size_t n, Int64 total_memory_hint)
{
    getNullMapColumn().reserve(n);
    total_memory_hint -= n * sizeof(UInt8);
    getNestedColumn().reserveWithTotalMemoryHint(n, total_memory_hint);
}

size_t ColumnNullable::byteSize() const
{
    return getNestedColumn().byteSize() + getNullMapColumn().byteSize();
}

size_t ColumnNullable::byteSize(size_t offset, size_t limit) const
{
    return getNestedColumn().byteSize(offset, limit) + getNullMapColumn().byteSize(offset, limit);
}

size_t ColumnNullable::allocatedBytes() const
{
    return getNestedColumn().allocatedBytes() + getNullMapColumn().allocatedBytes();
}


namespace
{
/// The following function implements a slightly more general version
/// of getExtremes() than the implementation from ColumnVector.
/// It takes into account the possible presence of nullable values.
template <typename T>
void getExtremesFromNullableContent(const ColumnVector<T> & col, const NullMap & null_map, Field & min, Field & max)
{
    const auto & data = col.getData();
    size_t size = data.size();

    if (size == 0)
    {
        min = Null();
        max = Null();
        return;
    }

    bool has_not_null = false;
    bool has_not_nan = false;

    T cur_min = 0;
    T cur_max = 0;

    for (size_t i = 0; i < size; ++i)
    {
        const T x = data[i];

        if (null_map[i])
            continue;

        if (!has_not_null)
        {
            cur_min = x;
            cur_max = x;
            has_not_null = true;
            has_not_nan = !isNaN(x);
            continue;
        }

        if (isNaN(x))
            continue;

        if (!has_not_nan)
        {
            cur_min = x;
            cur_max = x;
            has_not_nan = true;
            continue;
        }

        if (x < cur_min)
            cur_min = x;
        else if (x > cur_max)
            cur_max = x;
    }

    if (has_not_null)
    {
        min = static_cast<typename NearestFieldType<T>::Type>(cur_min);
        max = static_cast<typename NearestFieldType<T>::Type>(cur_max);
    }
}

} // namespace

template <typename... Ts, typename F>
static void castNestedColumn(const IColumn * nested_column, F && f)
{
    ((typeid_cast<const Ts *>(nested_column) ? (f(*typeid_cast<const Ts *>(nested_column))) : false) || ...);
}

void ColumnNullable::getExtremes(Field & min, Field & max) const
{
    min = Null();
    max = Null();

    const auto & null_map = getNullMapData();

    castNestedColumn<
        ColumnInt8,
        ColumnInt16,
        ColumnInt32,
        ColumnInt64,
        ColumnUInt8,
        ColumnUInt16,
        ColumnUInt32,
        ColumnUInt64,
        ColumnFloat32,
        ColumnFloat64>(nested_column.get(), [&](const auto & column) {
        using ColumnType = std::decay_t<decltype(column)>;
        using ValueType = typename ColumnType::value_type;
        getExtremesFromNullableContent<ValueType>(column, null_map, min, max);
        return true;
    });
}

ColumnPtr ColumnNullable::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    ColumnPtr replicated_data = getNestedColumn().replicateRange(start_row, end_row, offsets);
    ColumnPtr replicated_null_map = getNullMapColumn().replicateRange(start_row, end_row, offsets);
    return ColumnNullable::create(replicated_data, replicated_null_map);
}

template <bool negative>
void ColumnNullable::applyNullMapImpl(const ColumnUInt8 & map)
{
    NullMap & arr1 = getNullMapData();
    const NullMap & arr2 = map.getData();

    if (arr1.size() != arr2.size())
        throw Exception("Inconsistent sizes of ColumnNullable objects", ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0, size = arr1.size(); i < size; ++i)
        arr1[i] |= negative ^ arr2[i];
}


void ColumnNullable::applyNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<false>(map);
}

void ColumnNullable::applyNegatedNullMap(const ColumnUInt8 & map)
{
    applyNullMapImpl<true>(map);
}


void ColumnNullable::applyNullMap(const ColumnNullable & other)
{
    applyNullMap(other.getNullMapColumn());
}


void ColumnNullable::checkConsistency() const
{
    if (null_map->size() != getNestedColumn().size())
        throw Exception(
            fmt::format(
                "Logical error: Sizes of nested column and null map of Nullable column are not equal: null size is : "
                "{} column size is : {}",
                null_map->size(),
                getNestedColumn().size()),
            ErrorCodes::SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT);
}


ColumnPtr makeNullable(const ColumnPtr & column)
{
    if (column->isColumnNullable())
        return column;

    if (column->isColumnConst())
        return ColumnConst::create(
            makeNullable(static_cast<const ColumnConst &>(*column).getDataColumnPtr()),
            column->size());

    return ColumnNullable::create(column, ColumnUInt8::create(column->size(), 0));
}

std::tuple<const IColumn *, const NullMap *> removeNullable(const IColumn * column)
{
    if (!column->isColumnNullable())
        return {column, nullptr};
    const auto * nullable_column = assert_cast<const ColumnNullable *>(column);
    const auto * res = &nullable_column->getNestedColumn();
    const auto * nullmap = &nullable_column->getNullMapData();
    return {res, nullmap};
}

} // namespace DB
