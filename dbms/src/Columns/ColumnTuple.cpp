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

#include <Columns/ColumnTuple.h>
#include <DataStreams/ColumnGathererStream.h>

#include <ext/map.h>
#include <ext/range.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
} // namespace ErrorCodes


std::string ColumnTuple::getName() const
{
    std::stringstream res;
    res << "Tuple(";
    bool is_first = true;
    for (const auto & column : columns)
    {
        if (!is_first)
            res << ", ";
        is_first = false;
        res << column->getName();
    }
    res << ")";
    return res.str();
}

ColumnTuple::ColumnTuple(MutableColumns && mutable_columns)
{
    columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
    {
        if (column->isColumnConst())
            throw Exception{"ColumnTuple cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

        columns.push_back(std::move(column));
    }
}

ColumnTuple::Ptr ColumnTuple::create(const Columns & columns)
{
    for (const auto & column : columns)
        if (column->isColumnConst())
            throw Exception{"ColumnTuple cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

    auto column_tuple = ColumnTuple::create(MutableColumns());
    column_tuple->columns = columns;

    return column_tuple;
}

MutableColumnPtr ColumnTuple::cloneEmpty() const
{
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->cloneEmpty();

    return ColumnTuple::create(std::move(new_columns));
}

Field ColumnTuple::operator[](size_t n) const
{
    return Tuple{ext::map<TupleBackend>(columns, [n](const auto & column) { return (*column)[n]; })};
}

void ColumnTuple::get(size_t n, Field & res) const
{
    const size_t tuple_size = columns.size();
    res = Tuple(TupleBackend(tuple_size));
    TupleBackend & res_arr = DB::get<Tuple &>(res).toUnderType();
    for (const auto i : ext::range(0, tuple_size))
        columns[i]->get(n, res_arr[i]);
}

StringRef ColumnTuple::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insert(const Field & x)
{
    const TupleBackend & tuple = DB::get<const Tuple &>(x).toUnderType();

    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size)
        throw Exception(
            "Cannot insert value of different size into tuple",
            ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->assumeMutableRef().insert(tuple[i]);
}

void ColumnTuple::insertFrom(const IColumn & src_, size_t n)
{
    const auto & src = static_cast<const ColumnTuple &>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size)
        throw Exception(
            "Cannot insert value of different size into tuple",
            ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->assumeMutableRef().insertFrom(*src.columns[i], n);
}

void ColumnTuple::insertDefault()
{
    for (auto & column : columns)
        column->assumeMutableRef().insertDefault();
}

void ColumnTuple::popBack(size_t n)
{
    for (auto & column : columns)
        column->assumeMutableRef().popBack(n);
}

StringRef ColumnTuple::serializeValueIntoArena(
    size_t n,
    Arena & arena,
    char const *& begin,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    size_t values_size = 0;
    for (const auto & column : columns)
        values_size += column->serializeValueIntoArena(n, arena, begin, collator, sort_key_container).size;

    return StringRef(begin, values_size);
}

const char * ColumnTuple::deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr & collator)
{
    for (auto & column : columns)
        pos = column->assumeMutableRef().deserializeAndInsertFromArena(pos, collator);

    return pos;
}

void ColumnTuple::updateHashWithValue(
    size_t n,
    SipHash & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    for (const auto & column : columns)
        column->updateHashWithValue(n, hash, collator, sort_key_container);
}

void ColumnTuple::updateHashWithValues(
    IColumn::HashValues & hash_values,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    for (const auto & column : columns)
        column->updateHashWithValues(hash_values, collator, sort_key_container);
}

void ColumnTuple::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container) const
{
    auto s = size();

    RUNTIME_CHECK_MSG(
        hash.getData().size() == s,
        "size of WeakHash32({}) does not match size of column({})",
        hash.getData().size(),
        s);

    for (const auto & column : columns)
        column->updateWeakHash32(hash, collator, sort_key_container);
}

void ColumnTuple::updateWeakHash32(
    WeakHash32 & hash,
    const TiDB::TiDBCollatorPtr & collator,
    String & sort_key_container,
    const BlockSelective & selective) const
{
    const size_t rows = selective.size();

    RUNTIME_CHECK_MSG(
        hash.getData().size() == rows,
        "size of WeakHash32({}) doesn't match size of column({})",
        hash.getData().size(),
        rows);

    for (const auto & column : columns)
        column->updateWeakHash32(hash, collator, sort_key_container, selective);
}

void ColumnTuple::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        columns[i]->assumeMutableRef().insertRangeFrom(
            *static_cast<const ColumnTuple &>(src).columns[i],
            start,
            length);
}

ColumnPtr ColumnTuple::filter(const Filter & filt, ssize_t result_size_hint) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->filter(filt, result_size_hint);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::permute(const Permutation & perm, size_t limit) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->permute(perm, limit);

    return ColumnTuple::create(new_columns);
}

ColumnPtr ColumnTuple::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const
{
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        new_columns[i] = columns[i]->replicateRange(start_row, end_row, offsets);

    return ColumnTuple::create(new_columns);
}

MutableColumns ColumnTuple::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    return scatterImplForColumnTuple<false>(num_columns, selector, {});
}

MutableColumns ColumnTuple::scatter(
    ColumnIndex num_columns,
    const Selector & selector,
    const BlockSelective & selective) const
{
    return scatterImplForColumnTuple<true>(num_columns, selector, selective);
}

template <bool selective_block>
MutableColumns ColumnTuple::scatterImplForColumnTuple(
    ColumnIndex num_columns,
    const Selector & selector,
    const BlockSelective & selective) const
{
    const size_t tuple_size = columns.size();
    std::vector<MutableColumns> scattered_tuple_elements(tuple_size);

    for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
    {
        if constexpr (selective_block)
            scattered_tuple_elements[tuple_element_idx]
                = columns[tuple_element_idx]->scatter(num_columns, selector, selective);
        else
            scattered_tuple_elements[tuple_element_idx] = columns[tuple_element_idx]->scatter(num_columns, selector);
    }

    MutableColumns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        MutableColumns new_columns(tuple_size);
        for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
            new_columns[tuple_element_idx] = std::move(scattered_tuple_elements[tuple_element_idx][scattered_idx]);
        res[scattered_idx] = ColumnTuple::create(std::move(new_columns));
    }

    return res;
}

void ColumnTuple::scatterTo(ScatterColumns & scatterColumns, const Selector & selector) const
{
    scatterToImplForColumnTuple<false>(scatterColumns, selector, {});
}

void ColumnTuple::scatterTo(
    ScatterColumns & scatterColumns,
    const Selector & selector,
    const BlockSelective & selective) const
{
    scatterToImplForColumnTuple<true>(scatterColumns, selector, selective);
}

template <bool selective_block>
void ColumnTuple::scatterToImplForColumnTuple(
    ScatterColumns & scatterColumns,
    const Selector & selector,
    const BlockSelective & selective) const
{
    const size_t tuple_size = columns.size();
    ColumnIndex scattered_num_columns = scatterColumns.size();
    std::vector<MutableColumns> scattered_tuple_elements(tuple_size);
    for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
    {
        for (size_t scatter_idx = 0; scatter_idx < scattered_num_columns; ++scatter_idx)
        {
            auto col = static_cast<ColumnTuple &>(scatterColumns[scatter_idx]->assumeMutableRef())
                           .columns[tuple_element_idx]
                           ->assumeMutable();
            scattered_tuple_elements[tuple_element_idx].push_back(std::move(col));
        }
        if constexpr (selective_block)
            columns[tuple_element_idx]->scatterTo(scattered_tuple_elements[tuple_element_idx], selector, selective);
        else
            columns[tuple_element_idx]->scatterTo(scattered_tuple_elements[tuple_element_idx], selector);
    }

    for (size_t scattered_idx = 0; scattered_idx < scattered_num_columns; ++scattered_idx)
    {
        MutableColumns new_columns(tuple_size);
        for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx)
            new_columns[tuple_element_idx] = std::move(scattered_tuple_elements[tuple_element_idx][scattered_idx]);
        scatterColumns[scattered_idx] = ColumnTuple::create(std::move(new_columns));
    }
}

int ColumnTuple::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        if (int res
            = columns[i]->compareAt(n, m, *static_cast<const ColumnTuple &>(rhs).columns[i], nan_direction_hint))
            return res;

    return 0;
}

template <bool positive>
struct ColumnTuple::Less
{
    ColumnRawPtrs plain_columns;
    int nan_direction_hint;

    Less(const Columns & columns, int nan_direction_hint_)
        : nan_direction_hint(nan_direction_hint_)
    {
        for (const auto & column : columns)
            plain_columns.push_back(column.get());
    }

    bool operator()(size_t a, size_t b) const
    {
        for (const auto * plain_column : plain_columns)
        {
            int res = plain_column->compareAt(a, b, *plain_column, nan_direction_hint);
            if (res < 0)
                return positive;
            else if (res > 0)
                return !positive;
        }
        return false;
    }
};

void ColumnTuple::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    if (limit >= rows)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<true>(columns, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), Less<true>(columns, nan_direction_hint));
    }
}

void ColumnTuple::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnTuple::reserve(size_t n)
{
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i)
        getColumn(i).reserve(n);
}

size_t ColumnTuple::byteSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize();
    return res;
}

size_t ColumnTuple::byteSize(size_t offset, size_t limit) const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize(offset, limit);
    return res;
}

size_t ColumnTuple::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res;
}

void ColumnTuple::getExtremes(Field & min, Field & max) const
{
    const size_t tuple_size = columns.size();

    min = Tuple(TupleBackend(tuple_size));
    max = Tuple(TupleBackend(tuple_size));

    auto & min_backend = min.get<Tuple &>().toUnderType();
    auto & max_backend = max.get<Tuple &>().toUnderType();

    for (const auto i : ext::range(0, tuple_size))
        columns[i]->getExtremes(min_backend[i], max_backend[i]);
}

void ColumnTuple::forEachSubcolumn(ColumnCallback callback)
{
    for (auto & column : columns)
        callback(column);
}


} // namespace DB
