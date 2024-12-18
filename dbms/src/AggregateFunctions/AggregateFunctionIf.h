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

#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

/** Not an aggregate function, but an adapter of aggregate functions,
  * which any aggregate function `agg(x)` makes an aggregate function of the form `aggIf(x, cond)`.
  * The adapted aggregate function takes two arguments - a value and a condition,
  * and calculates the nested aggregate function for the values when the condition is satisfied.
  * For example, avgIf(x, cond) calculates the average x if `cond`.
  */
class AggregateFunctionIf final : public IAggregateFunctionHelper<AggregateFunctionIf>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionIf(AggregateFunctionPtr nested, const DataTypes & types)
        : nested_func(nested)
        , num_arguments(types.size())
    {
        if (num_arguments == 0)
            throw Exception(
                "Aggregate function " + getName() + " require at least one argument",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!typeid_cast<const DataTypeUInt8 *>(types.back().get()))
            throw Exception(
                "Last argument for aggregate function " + getName() + " must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return nested_func->getName() + "If"; }

    DataTypePtr getReturnType() const override { return nested_func->getReturnType(); }

    void create(AggregateDataPtr __restrict place) const override { nested_func->create(place); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_func->destroy(place); }

    bool hasTrivialDestructor() const override { return nested_func->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return nested_func->sizeOfData(); }

    size_t alignOfData() const override { return nested_func->alignOfData(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (static_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num])
            nested_func->add(place, columns, row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena)
        const override
    {
        if (static_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num])
            nested_func->decrease(place, columns, row_num, arena);
    }

    void reset(AggregateDataPtr __restrict place) const override { nested_func->reset(place); }

    void prepareWindow(AggregateDataPtr __restrict place) const override { nested_func->prepareWindow(place); }

    void addBatch(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatch(start_offset, batch_size, places, place_offset, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlace(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatchSinglePlace(start_offset, batch_size, place, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlaceNotNull(
        size_t start_offset,
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t) const override
    {
        nested_func
            ->addBatchSinglePlaceNotNull(start_offset, batch_size, place, columns, null_map, arena, num_arguments - 1);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        nested_func->mergeBatch(batch_size, places, place_offset, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        nested_func->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, arena);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override { return nested_func->allocatesMemoryInArena(); }

    bool isState() const override { return nested_func->isState(); }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
