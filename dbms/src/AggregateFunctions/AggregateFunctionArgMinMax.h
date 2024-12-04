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

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h> // SingleValueDataString used in embedded compiler
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <common/StringRef.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// For possible values for template parameters, see AggregateFunctionMinMaxAny.h
template <typename ResultData, typename ValueData>
struct AggregateFunctionArgMinMaxData
{
    using ResultData_t = ResultData;
    using ValueData_t = ValueData;

    ResultData result; // the argument at which the minimum/maximum value is reached.
    ValueData value; // value for which the minimum/maximum is calculated.
};

/// Returns the first arg value found for the minimum/maximum value. Example: argMax(arg, value).
template <typename Data>
class AggregateFunctionArgMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>
{
private:
    DataTypePtr type_res;
    DataTypePtr type_val;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res, const DataTypePtr & type_val)
        : type_res(type_res)
        , type_val(type_val)
    {
        if (!type_val->isComparable())
            throw Exception(
                "Illegal type " + type_val->getName() + " of second argument of aggregate function " + getName()
                    + " because the values of that data type are not comparable",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return StringRef(Data::ValueData_t::name()) == StringRef("min") ? "argMin" : "argMax";
    }

    DataTypePtr getReturnType() const override { return type_res; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(*columns[1], row_num, arena))
            this->data(place).result.change(*columns[0], row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override
    {
        // TODO move to helper
        throw Exception("");
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(this->data(rhs).value, arena))
            this->data(place).result.change(this->data(rhs).result, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).result.write(buf, *type_res);
        this->data(place).value.write(buf, *type_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).result.read(buf, *type_res, arena);
        this->data(place).value.read(buf, *type_val, arena);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result.insertResultInto(to);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB
