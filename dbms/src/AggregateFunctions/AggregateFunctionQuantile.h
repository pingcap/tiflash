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
#include <AggregateFunctions/QuantilesCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <type_traits>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Generic aggregate function for calculation of quantiles.
  * It depends on quantile calculation data structure. Look at Quantile*.h for various implementations.
  */

template <
    /// Type of first argument.
    typename Value,
    /// Data structure and implementation of calculation. Look at QuantileExact.h for example.
    typename Data,
    /// Structure with static member "name", containing the name of the aggregate function.
    typename Name,
    /// If true, the function accept second argument
    /// (in can be "weight" to calculate quantiles or "determinator" that is used instead of PRNG).
    /// Second argument is always obtained through 'getUInt' method.
    bool have_second_arg,
    /// If non-void, the function will return float of specified type with possibly interpolated results and NaN if there was no values.
    /// Otherwise it will return Value type and default value if there was no values.
    /// As an example, the function cannot return floats, if the SQL type of argument is Date or DateTime.
    typename FloatReturnType,
    /// If true, the function will accept multiple parameters with quantile levels
    ///  and return an Array filled with many values of that quantiles.
    bool returns_many>
class AggregateFunctionQuantile final
    : public IAggregateFunctionDataHelper<
          Data,
          AggregateFunctionQuantile<Value, Data, Name, have_second_arg, FloatReturnType, returns_many>>
{
private:
    static constexpr bool returns_float = !std::is_same_v<FloatReturnType, void>;

    QuantileLevels<Float64> levels;

    /// Used when there are single level to get.
    Float64 level = 0.5;

    DataTypePtr argument_type;

public:
    AggregateFunctionQuantile(const DataTypePtr & argument_type, const Array & params)
        : levels(params, returns_many)
        , level(levels.levels[0])
        , argument_type(argument_type)
    {
        if (!returns_many && levels.size() > 1)
            throw Exception(
                "Aggregate function " + getName() + " require one parameter or less",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    String getName() const override { return Name::name; }

    DataTypePtr getReturnType() const override
    {
        DataTypePtr res;

        if constexpr (returns_float)
            res = std::make_shared<DataTypeNumber<FloatReturnType>>();
        else
            res = argument_type;

        if constexpr (returns_many)
            return std::make_shared<DataTypeArray>(res);
        else
            return res;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (have_second_arg)
            this->data(place).add(
                static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num],
                columns[1]->getUInt(row_num));
        else
            this->data(place).add(static_cast<const ColumnVector<Value> &>(*columns[0]).getData()[row_num]);
    }

    // TODO move to helper
    void decrease(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override { throw Exception("");}

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like compactization) before serializing.
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like sorting) for obtain a result.
        auto & data = this->data(const_cast<AggregateDataPtr>(place));

        if constexpr (returns_many)
        {
            ColumnArray & arr_to = static_cast<ColumnArray &>(to);
            ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

            size_t size = levels.size();
            offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

            if (!size)
                return;

            if constexpr (returns_float)
            {
                auto & data_to = static_cast<ColumnVector<FloatReturnType> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
            else
            {
                auto & data_to = static_cast<ColumnVector<Value> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(data_to.size() + size);

                data.getMany(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
            }
        }
        else
        {
            if constexpr (returns_float)
                static_cast<ColumnVector<FloatReturnType> &>(to).getData().push_back(data.getFloat(level));
            else
                static_cast<ColumnVector<Value> &>(to).getData().push_back(data.get(level));
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

} // namespace DB


/// These must be exposed in header for the purpose of dynamic compilation.
#include <AggregateFunctions/QuantileExact.h>
#include <AggregateFunctions/QuantileExactWeighted.h>
#include <AggregateFunctions/QuantileReservoirSampler.h>
#include <AggregateFunctions/QuantileReservoirSamplerDeterministic.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include <AggregateFunctions/QuantileTiming.h>

namespace DB
{
struct NameQuantile
{
    static constexpr auto name = "quantile";
};
struct NameQuantiles
{
    static constexpr auto name = "quantiles";
};
struct NameQuantileDeterministic
{
    static constexpr auto name = "quantileDeterministic";
};
struct NameQuantilesDeterministic
{
    static constexpr auto name = "quantilesDeterministic";
};

struct NameQuantileExact
{
    static constexpr auto name = "quantileExact";
};
struct NameQuantileExactWeighted
{
    static constexpr auto name = "quantileExactWeighted";
};
struct NameQuantilesExact
{
    static constexpr auto name = "quantilesExact";
};
struct NameQuantilesExactWeighted
{
    static constexpr auto name = "quantilesExactWeighted";
};

struct NameQuantileTiming
{
    static constexpr auto name = "quantileTiming";
};
struct NameQuantileTimingWeighted
{
    static constexpr auto name = "quantileTimingWeighted";
};
struct NameQuantilesTiming
{
    static constexpr auto name = "quantilesTiming";
};
struct NameQuantilesTimingWeighted
{
    static constexpr auto name = "quantilesTimingWeighted";
};

struct NameQuantileTDigest
{
    static constexpr auto name = "quantileTDigest";
};
struct NameQuantileTDigestWeighted
{
    static constexpr auto name = "quantileTDigestWeighted";
};
struct NameQuantilesTDigest
{
    static constexpr auto name = "quantilesTDigest";
};
struct NameQuantilesTDigestWeighted
{
    static constexpr auto name = "quantilesTDigestWeighted";
};

} // namespace DB
