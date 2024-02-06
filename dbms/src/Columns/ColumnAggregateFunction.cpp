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

#include <AggregateFunctions/AggregateFunctionState.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/WriteBufferFromArena.h>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PARAMETER_OUT_OF_BOUND;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes


ColumnAggregateFunction::~ColumnAggregateFunction()
{
    if (!func->hasTrivialDestructor() && !src)
        for (auto * val : data)
            func->destroy(val);
}

void ColumnAggregateFunction::addArena(ArenaPtr arena_)
{
    arenas.push_back(arena_);
}

void ColumnAggregateFunction::insertRangeFrom(const IColumn & from, size_t start, size_t length)
{
    const auto & from_concrete = static_cast<const ColumnAggregateFunction &>(from);

    if (start + length > from_concrete.getData().size())
        throw Exception(
            fmt::format(
                "Parameters are out of bound in ColumnAggregateFunction::insertRangeFrom method, start={}, length={}, "
                "from.size()={}",
                start,
                length,
                from_concrete.getData().size()),
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (!empty() && src.get() != &from_concrete)
    {
        /// Must create new states of aggregate function and take ownership of it,
        ///  because ownership of states of aggregate function cannot be shared for individual rows,
        ///  (only as a whole).

        size_t end = start + length;
        for (size_t i = start; i < end; ++i)
            insertFrom(from, i);
    }
    else
    {
        /// Keep shared ownership of aggregation states.
        src = from_concrete.getPtr();

        auto & data = getData();
        size_t old_size = data.size();
        data.resize(old_size + length);
        memcpy(&data[old_size], &from_concrete.getData()[start], length * sizeof(data[0]));
    }
}


ColumnPtr ColumnAggregateFunction::filter(const Filter & filter, ssize_t result_size_hint) const
{
    size_t size = getData().size();
    if (size != filter.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (size == 0)
        return cloneEmpty();

    auto res = createView();
    auto & res_data = res->getData();

    if (result_size_hint)
    {
        if (result_size_hint < 0)
            result_size_hint = countBytesInFilter(filter);
        res_data.reserve(result_size_hint);
    }

    for (size_t i = 0; i < size; ++i)
        if (filter[i])
            res_data.push_back(getData()[i]);

    /// To save RAM in case of too strong filtering.
    if (res_data.size() * 2 < res_data.capacity())
        res_data = Container(res_data.cbegin(), res_data.cend());

    return res;
}


ColumnPtr ColumnAggregateFunction::permute(const Permutation & perm, size_t limit) const
{
    size_t size = getData().size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = createView();

    res->getData().resize(limit);
    for (size_t i = 0; i < limit; ++i)
        res->getData()[i] = getData()[perm[i]];

    return res;
}

/// Is required to support operations with Set
void ColumnAggregateFunction::updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &)
    const
{
    WriteBufferFromOwnString wbuf;
    func->serialize(getData()[n], wbuf);
    hash.update(wbuf.str().c_str(), wbuf.str().size());
}

void ColumnAggregateFunction::updateHashWithValues(
    IColumn::HashValues & hash_values,
    const TiDB::TiDBCollatorPtr &,
    String &) const
{
    for (size_t i = 0, size = getData().size(); i < size; ++i)
    {
        WriteBufferFromOwnString wbuf;
        func->serialize(getData()[i], wbuf);
        hash_values[i].update(wbuf.str().c_str(), wbuf.str().size());
    }
}

void ColumnAggregateFunction::updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const
{
    auto s = data.size();
    if (hash.getData().size() != data.size())
        throw Exception(
            fmt::format(
                "Size of WeakHash32 does not match size of column: column size is {}, hash size is {}",
                s,
                hash.getData().size()),
            ErrorCodes::LOGICAL_ERROR);

    auto & hash_data = hash.getData();

    std::vector<UInt8> v;
    for (size_t i = 0; i < s; ++i)
    {
        WriteBufferFromVector<std::vector<UInt8>> wbuf(v);
        func->serialize(data[i], wbuf);
        wbuf.finalize();
        hash_data[i] = ::updateWeakHash32(v.data(), v.size(), hash_data[i]);
    }
}

/// NOTE: Highly overestimates size of a column if it was produced in AggregatingBlockInputStream (it contains size of other columns)
size_t ColumnAggregateFunction::byteSize() const
{
    size_t res = getData().size() * sizeof(getData()[0]);

    for (const auto & arena : arenas)
        res += arena->size();

    return res;
}


/// Like byteSize(), highly overestimates size
size_t ColumnAggregateFunction::allocatedBytes() const
{
    size_t res = getData().allocated_bytes();

    for (const auto & arena : arenas)
        res += arena->size();

    return res;
}

size_t ColumnAggregateFunction::estimateByteSizeForSpill() const
{
    static const std::unordered_set<String>
        trivial_agg_func_name{"sum", "min", "max", "count", "avg", "first_row", "any"};
    if (trivial_agg_func_name.find(func->getName()) != trivial_agg_func_name.end())
    {
        size_t res = func->sizeOfData() * size();
        /// For trivial agg, we can estimate each element's size as `func->sizeofData()`, and
        /// if the result is String, use `APPROX_STRING_SIZE` as the average size of the String
        if (removeNullable(func->getReturnType())->isString())
            res += size() * ColumnString::APPROX_STRING_SIZE;
        return res;
    }
    else
    {
        /// For non-trivial agg like uniqXXX/group_concat, can't estimate the memory usage, so just return allocateBytes(),
        /// it will highly overestimates size of a column if it was produced in AggregatingBlockInputStream (it contains size of other columns)
        return allocatedBytes();
    }
}

MutableColumnPtr ColumnAggregateFunction::cloneEmpty() const
{
    return create(func, Arenas(1, std::make_shared<Arena>()));
}

Field ColumnAggregateFunction::operator[](size_t n) const
{
    Field field = String();
    {
        WriteBufferFromString buffer(field.get<String &>());
        func->serialize(getData()[n], buffer);
    }
    return field;
}

void ColumnAggregateFunction::get(size_t n, Field & res) const
{
    res = String();
    {
        WriteBufferFromString buffer(res.get<String &>());
        func->serialize(getData()[n], buffer);
    }
}

StringRef ColumnAggregateFunction::getDataAt(size_t n) const
{
    return StringRef(reinterpret_cast<const char *>(&getData()[n]), sizeof(getData()[n]));
}

void ColumnAggregateFunction::insertData(const char * pos, size_t /*length*/)
{
    getData().push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
}

void ColumnAggregateFunction::insertFrom(const IColumn & src, size_t n)
{
    /// Must create new state of aggregate function and take ownership of it,
    ///  because ownership of states of aggregate function cannot be shared for individual rows,
    ///  (only as a whole, see comment above).
    insertDefault();
    insertMergeFrom(src, n);
}

void ColumnAggregateFunction::insertFrom(ConstAggregateDataPtr __restrict place)
{
    insertDefault();
    insertMergeFrom(place);
}

void ColumnAggregateFunction::insertMergeFrom(ConstAggregateDataPtr __restrict place)
{
    func->merge(getData().back(), place, &createOrGetArena());
}

void ColumnAggregateFunction::insertMergeFrom(const IColumn & src_, size_t n)
{
    insertMergeFrom(static_cast<const ColumnAggregateFunction &>(src_).getData()[n]);
}

Arena & ColumnAggregateFunction::createOrGetArena()
{
    if (unlikely(arenas.empty()))
        arenas.emplace_back(std::make_shared<Arena>());
    return *arenas.back().get();
}

void ColumnAggregateFunction::insert(const Field & x)
{
    IAggregateFunction * function = func.get();

    Arena & arena = createOrGetArena();

    getData().push_back(arena.alloc(function->sizeOfData()));
    function->create(getData().back());
    ReadBufferFromString read_buffer(x.get<const String &>());
    function->deserialize(getData().back(), read_buffer, &arena);
}

void ColumnAggregateFunction::insertDefault()
{
    IAggregateFunction * function = func.get();

    Arena & arena = createOrGetArena();

    getData().push_back(arena.alloc(function->sizeOfData()));
    function->create(getData().back());
}

StringRef ColumnAggregateFunction::serializeValueIntoArena(
    size_t n,
    Arena & dst,
    const char *& begin,
    const TiDB::TiDBCollatorPtr &,
    String &) const
{
    IAggregateFunction * function = func.get();
    WriteBufferFromArena out(dst, begin);
    function->serialize(getData()[n], out);
    return out.finish();
}

const char * ColumnAggregateFunction::deserializeAndInsertFromArena(
    const char * src_arena,
    const TiDB::TiDBCollatorPtr &)
{
    IAggregateFunction * function = func.get();

    /** Parameter "src_arena" points to Arena, from which we will deserialize the state.
      * And "dst_arena" is another Arena, that aggregate function state will use to store its data.
      */
    Arena & dst_arena = createOrGetArena();

    getData().push_back(dst_arena.alloc(function->sizeOfData()));
    function->create(getData().back());

    /** We will read from src_arena.
      * There is no limit for reading - it is assumed, that we can read all that we need after src_arena pointer.
      * Buf ReadBufferFromMemory requires some bound. We will use arbitrary big enough number, that will not overflow pointer.
      * NOTE Technically, this is not compatible with C++ standard,
      *  as we cannot legally compare pointers after last element + 1 of some valid memory region.
      *  Probably this will not work under UBSan.
      */
    ReadBufferFromMemory read_buffer(src_arena, std::numeric_limits<char *>::max() - src_arena);
    function->deserialize(getData().back(), read_buffer, &dst_arena);

    return read_buffer.position();
}

void ColumnAggregateFunction::popBack(size_t n)
{
    size_t size = data.size();
    size_t new_size = size - n;

    if (!src)
        for (size_t i = new_size; i < size; ++i)
            func->destroy(data[i]);

    data.resize_assume_reserved(new_size);
}

ColumnPtr ColumnAggregateFunction::replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets)
    const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    assert(start_row < end_row);
    assert(end_row <= size);

    if (size == 0)
        return cloneEmpty();

    auto res = createView();
    auto & res_data = res->getData();
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

MutableColumns ColumnAggregateFunction::scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector)
    const
{
    /// Columns with scattered values will point to this column as the owner of values.
    MutableColumns columns(num_columns);
    for (auto & column : columns)
        column = createView();

    size_t num_rows = size();

    {
        size_t reserve_size = 1.1 * num_rows / num_columns; /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<ColumnAggregateFunction &>(*columns[selector[i]]).data.push_back(data[i]);

    return columns;
}

void ColumnAggregateFunction::scatterTo(
    ScatterColumns & columns [[maybe_unused]],
    const Selector & selector [[maybe_unused]]) const
{
    throw TiFlashException("ColumnAggregateFunction does not support scatterTo", Errors::Coprocessor::Unimplemented);
}

void ColumnAggregateFunction::getPermutation(
    bool /*reverse*/,
    size_t /*limit*/,
    int /*nan_direction_hint*/,
    IColumn::Permutation & res) const
{
    size_t s = getData().size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnAggregateFunction::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnAggregateFunction::getExtremes(Field & min, Field & max) const
{
    /// Place serialized default values into min/max.

    PODArrayWithStackMemory<char, 16> place_buffer(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();

    String serialized;

    func->create(place);
    try
    {
        WriteBufferFromString buffer(serialized);
        func->serialize(place, buffer);
    }
    catch (...)
    {
        func->destroy(place);
        throw;
    }
    func->destroy(place);

    min = serialized;
    max = serialized;
}

} // namespace DB
