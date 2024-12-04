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
#include <Common/ArenaAllocator.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#define AGGREGATE_FUNCTION_MAX_INTERSECTIONS_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_LARGE_ARRAY_SIZE;
} // namespace ErrorCodes


/** maxIntersections: returns maximum count of the intersected intervals defined by start_column and end_column values,
  * maxIntersectionsPosition: returns leftmost position of maximum intersection of intervals.
  */

/// Similar to GroupArrayNumericData.
template <typename T>
struct MaxIntersectionsData
{
    /// Left or right end of the interval and signed weight; with positive sign for begin of interval and negative sign for end of interval.
    using Value = std::pair<T, Int64>;

    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<Value, 32, Allocator>;

    Array value;
};

enum class AggregateFunctionIntersectionsKind
{
    Count,
    Position
};

template <typename PointType>
class AggregateFunctionIntersectionsMax final
    : public IAggregateFunctionDataHelper<MaxIntersectionsData<PointType>, AggregateFunctionIntersectionsMax<PointType>>
{
private:
    AggregateFunctionIntersectionsKind kind;

public:
    AggregateFunctionIntersectionsMax(AggregateFunctionIntersectionsKind kind_, const DataTypes & arguments)
        : kind(kind_)
    {
        if (!arguments[0]->isNumber())
            throw Exception{
                getName() + ": first argument must be represented by integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[1]->isNumber())
            throw Exception{
                getName() + ": second argument must be represented by integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[0]->equals(*arguments[1]))
            throw Exception{getName() + ": arguments must have the same type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    String getName() const override
    {
        return kind == AggregateFunctionIntersectionsKind::Count ? "maxIntersections" : "maxIntersectionsPosition";
    }

    DataTypePtr getReturnType() const override
    {
        if (kind == AggregateFunctionIntersectionsKind::Count)
            return std::make_shared<DataTypeUInt64>();
        else
            return std::make_shared<DataTypeNumber<PointType>>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        PointType left = static_cast<const ColumnVector<PointType> &>(*columns[0]).getData()[row_num];
        PointType right = static_cast<const ColumnVector<PointType> &>(*columns[1]).getData()[row_num];

        if (!isNaN(left))
            this->data(place).value.push_back(std::make_pair(left, static_cast<Int64>(1)), arena);

        if (!isNaN(right))
            this->data(place).value.push_back(std::make_pair(right, static_cast<Int64>(-1)), arena);
    }

    // TODO move to helper
    void decrease(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override
    {
        throw Exception("");
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.end(), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&value[0]), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_MAX_INTERSECTIONS_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = this->data(place).value;

        value.resize(size, arena);
        buf.read(reinterpret_cast<char *>(&value[0]), size * sizeof(value[0]));
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Int64 current_intersections = 0;
        Int64 max_intersections = 0;
        PointType position_of_max_intersections = 0;

        /// const_cast because we will sort the array
        auto & array = const_cast<typename MaxIntersectionsData<PointType>::Array &>(this->data(place).value);

        std::sort(array.begin(), array.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

        for (const auto & point_weight : array)
        {
            current_intersections += point_weight.second;
            if (current_intersections > max_intersections)
            {
                max_intersections = current_intersections;
                position_of_max_intersections = point_weight.first;
            }
        }

        if (kind == AggregateFunctionIntersectionsKind::Count)
        {
            auto & result_column = static_cast<ColumnUInt64 &>(to).getData();
            result_column.push_back(max_intersections);
        }
        else
        {
            auto & result_column = static_cast<ColumnVector<PointType> &>(to).getData();
            result_column.push_back(position_of_max_intersections);
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
