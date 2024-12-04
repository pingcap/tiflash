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
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes


/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `aggArray(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
class AggregateFunctionArray final : public IAggregateFunctionHelper<AggregateFunctionArray>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionArray(AggregateFunctionPtr nested_, const DataTypes & arguments)
        : nested_func(nested_)
        , num_arguments(arguments.size())
    {
        for (const auto & type : arguments)
            if (!typeid_cast<const DataTypeArray *>(type.get()))
                throw Exception(
                    "All arguments for aggregate function " + getName() + " must be arrays",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return nested_func->getName() + "Array"; }

    DataTypePtr getReturnType() const override { return nested_func->getReturnType(); }

    void create(AggregateDataPtr __restrict place) const override { nested_func->create(place); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_func->destroy(place); }

    bool hasTrivialDestructor() const override { return nested_func->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return nested_func->sizeOfData(); }

    size_t alignOfData() const override { return nested_func->alignOfData(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        addOrDecrease<true>(place, columns, row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena)
        const override
    {
        addOrDecrease<false>(place, columns, row_num, arena);
    }

    template <bool is_add>
    void addOrDecrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
    {
        const IColumn * nested[num_arguments];

        for (size_t i = 0; i < num_arguments; ++i)
            nested[i] = &static_cast<const ColumnArray &>(*columns[i]).getData();

        const auto & first_array_column = static_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets = first_array_column.getOffsets();

        size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];

        /// Sanity check. NOTE We can implement specialization for a case with single argument, if the check will hurt performance.
        for (size_t i = 1; i < num_arguments; ++i)
        {
            const auto & ith_column = static_cast<const ColumnArray &>(*columns[i]);
            const IColumn::Offsets & ith_offsets = ith_column.getOffsets();

            if (ith_offsets[row_num] != end || (row_num != 0 && ith_offsets[row_num - 1] != begin))
                throw Exception(
                    "Arrays passed to " + getName() + " aggregate function have different sizes",
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

        for (size_t i = begin; i < end; ++i)
            if constexpr (is_add)
                nested_func->add(place, nested, i, arena);
            else
                nested_func->decrease(place, nested, i, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
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

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
