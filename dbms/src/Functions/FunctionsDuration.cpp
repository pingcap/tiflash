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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsDuration.h>
#include <Functions/IFunction.h>
#include <fmt/format.h>

namespace DB
{
DataTypePtr FunctionConvertDurationFromNanos::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!arguments[0].type->isInteger())
    {
        throw Exception(
            fmt::format("Illegal type {} of first argument of function {}", arguments[0].type->getName(), getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    if (!arguments[1].type->isInteger() || !arguments[1].column->isColumnConst())
    {
        throw Exception(
            fmt::format("Illegal type {} of second argument of function {}", arguments[1].type->getName(), getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    auto fsp = arguments[1].column.get()->getInt(0);
    return std::make_shared<DataTypeMyDuration>(fsp);
}

void FunctionConvertDurationFromNanos::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
}

template <typename Impl>
DataTypePtr FunctionDurationSplit<Impl>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!arguments[0].type->isMyTime())
    {
        throw Exception(
            fmt::format("Illegal type {} of first argument of function {}", arguments[0].type->getName(), getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    return std::make_shared<DataTypeInt64>();
};

template <typename Impl>
void FunctionDurationSplit<Impl>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    const auto * dur_type = checkAndGetDataType<DataTypeMyDuration>(block.getByPosition(arguments[0]).type.get());
    if (dur_type == nullptr)
    {
        throw Exception(
            fmt::format(
                "Illegal column {} of first argument of function {}",
                block.getByPosition(arguments[0]).column->getName(),
                name),
            ErrorCodes::ILLEGAL_COLUMN);
    }
    const auto * duration_col = checkAndGetColumn<ColumnVector<DataTypeMyDuration::FieldType>>(
        block.getByPosition(arguments[0]).column.get());
    if (duration_col != nullptr)
    {
        const auto & vec_duration = duration_col->getData();
        auto col_result = ColumnVector<Int64>::create();
        auto & vec_result = col_result->getData();
        size_t size = duration_col->size();
        vec_result.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            MyDuration dur(vec_duration[i], dur_type->getFsp());
            vec_result[i] = Impl::apply(dur);
        }
        block.getByPosition(result).column = std::move(col_result);
    }
    else
        throw Exception(
            fmt::format(
                "Illegal column {} of first argument of function {}",
                block.getByPosition(arguments[0]).column->getName(),
                name),
            ErrorCodes::ILLEGAL_COLUMN);
};

template <typename Impl>
DataTypePtr FunctionMyDurationToSec<Impl>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!arguments[0].type->isMyTime())
    {
        throw Exception(
            fmt::format(
                "Illegal type {} of the first argument of function {}",
                arguments[0].type->getName(),
                getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    return std::make_shared<DataTypeInt64>();
}

template <typename Impl>
void FunctionMyDurationToSec<Impl>::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    const auto * from_type = checkAndGetDataType<DataTypeMyDuration>(block.getByPosition(arguments[0]).type.get());
    if (from_type == nullptr)
    {
        throw Exception(
            fmt::format(
                "Illegal column {} of the first argument of function {}",
                block.getByPosition(arguments[0]).column->getName(),
                name),
            ErrorCodes::ILLEGAL_COLUMN);
    }

    using FromFieldType = typename DataTypeMyDuration::FieldType;
    const auto * col_from
        = checkAndGetColumn<ColumnVector<FromFieldType>>(block.getByPosition(arguments[0]).column.get());
    if (col_from != nullptr)
    {
        const typename ColumnVector<FromFieldType>::Container & vec_from = col_from->getData();
        const size_t size = vec_from.size();
        auto col_to = ColumnVector<Int64>::create(size);
        typename ColumnVector<Int64>::Container & vec_to = col_to->getData();

        for (size_t i = 0; i < size; ++i)
        {
            MyDuration val(vec_from[i], from_type->getFsp());
            vec_to[i] = Impl::apply(val);
        }
        block.getByPosition(result).column = std::move(col_to);
    }
    else
        throw Exception(
            fmt::format(
                "Illegal column {} of the first argument of function {}",
                block.getByPosition(arguments[0]).column->getName(),
                name),
            ErrorCodes::ILLEGAL_COLUMN);
}

struct DurationSplitHourImpl
{
    static constexpr auto name = "hour";
    static Int64 apply(const MyDuration & dur) { return dur.hours(); }
};

struct DurationSplitMinuteImpl
{
    static constexpr auto name = "minute";
    static Int64 apply(const MyDuration & dur) { return dur.minutes(); }
};

struct DurationSplitSecondImpl
{
    static constexpr auto name = "second";
    static Int64 apply(const MyDuration & dur) { return dur.seconds(); }
};

struct DurationSplitMicroSecondImpl
{
    static constexpr auto name = "microSecond";
    static Int64 apply(const MyDuration & dur) { return dur.microSecond(); }
};

struct TiDBTimeToSecTransformerImpl
{
    static constexpr auto name = "tidbTimeToSec";
    static Int64 apply(const MyDuration & val)
    {
        Int64 sign = 1;
        if (val.isNeg())
        {
            sign = -1;
        }
        return sign * (val.hours() * 3600 + val.minutes() * 60 + val.seconds());
    }
};

using FunctionDurationHour = FunctionDurationSplit<DurationSplitHourImpl>;
using FunctionDurationMinute = FunctionDurationSplit<DurationSplitMinuteImpl>;
using FunctionDurationSecond = FunctionDurationSplit<DurationSplitSecondImpl>;
using FunctionDurationMicroSecond = FunctionDurationSplit<DurationSplitMicroSecondImpl>;

using FunctionToTiDBTimeToSec = FunctionMyDurationToSec<TiDBTimeToSecTransformerImpl>;

void registerFunctionsDuration(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConvertDurationFromNanos>();

    factory.registerFunction<FunctionDurationHour>();
    factory.registerFunction<FunctionDurationMinute>();
    factory.registerFunction<FunctionDurationSecond>();
    factory.registerFunction<FunctionDurationMicroSecond>();

    factory.registerFunction<FunctionToTiDBTimeToSec>();
    factory.registerFunction<FunctionExtractMyDuration>();
}
} // namespace DB
