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

#include <Columns/ColumnString.h>
#include <Common/MyDuration.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsDateTime.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <common/DateLUT.h>

#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionConvertDurationFromNanos : public IFunction
{
public:
    static constexpr auto name = "FunctionConvertDurationFromNanos";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConvertDurationFromNanos>(); };
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
};

template <typename Impl>
class FunctionDurationSplit : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDurationSplit>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
};

template <typename Impl>
class FunctionMyDurationToSec : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMyDurationToSec>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
};

class FunctionExtractMyDuration : public IFunction
{
public:
    static constexpr auto name = "extractMyDuration";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionExtractMyDuration>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw TiFlashException(
                fmt::format("First argument for function {} (unit) must be String", getName()),
                Errors::Coprocessor::BadRequest);

        if (!arguments[1]->isMyTime())
            throw TiFlashException(
                fmt::format(
                    "Illegal type {} of second argument of function {}. Must be Duration.",
                    arguments[1]->getName(),
                    getName()),
                Errors::Coprocessor::BadRequest);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw TiFlashException(
                fmt::format("First argument for function {} must be constant String", getName()),
                Errors::Coprocessor::BadRequest);

        String unit = Poco::toLower(unit_column->getValue<String>());

        auto col_from = block.getByPosition(arguments[1]).column;

        size_t rows = block.rows();
        auto col_to = ColumnInt64::create(rows);
        auto & vec_to = col_to->getData();

        if (unit == "hour")
            dispatch<ExtractMyDurationImpl::extractHour>(col_from, vec_to);
        else if (unit == "minute")
            dispatch<ExtractMyDurationImpl::extractMinute>(col_from, vec_to);
        else if (unit == "second")
            dispatch<ExtractMyDurationImpl::extractSecond>(col_from, vec_to);
        else if (unit == "microsecond")
            dispatch<ExtractMyDurationImpl::extractMicrosecond>(col_from, vec_to);
        else if (unit == "second_microsecond")
            dispatch<ExtractMyDurationImpl::extractSecondMicrosecond>(col_from, vec_to);
        else if (unit == "minute_microsecond")
            dispatch<ExtractMyDurationImpl::extractMinuteMicrosecond>(col_from, vec_to);
        else if (unit == "minute_second")
            dispatch<ExtractMyDurationImpl::extractMinuteSecond>(col_from, vec_to);
        else if (unit == "hour_microsecond")
            dispatch<ExtractMyDurationImpl::extractHourMicrosecond>(col_from, vec_to);
        else if (unit == "hour_second")
            dispatch<ExtractMyDurationImpl::extractHourSecond>(col_from, vec_to);
        else if (unit == "hour_minute")
            dispatch<ExtractMyDurationImpl::extractHourMinute>(col_from, vec_to);
        else if (unit == "day_microsecond")
            dispatch<ExtractMyDurationImpl::extractDayMicrosecond>(col_from, vec_to);
        else if (unit == "day_second")
            dispatch<ExtractMyDurationImpl::extractDaySecond>(col_from, vec_to);
        else if (unit == "day_minute")
            dispatch<ExtractMyDurationImpl::extractDayMinute>(col_from, vec_to);
        else if (unit == "day_hour")
            dispatch<ExtractMyDurationImpl::extractDayHour>(col_from, vec_to);
        else
            throw TiFlashException(
                fmt::format("Function {} does not support '{}' unit", getName(), unit),
                Errors::Coprocessor::BadRequest);

        block.getByPosition(result).column = std::move(col_to);
    }

private:
    using Func = Int64 (*)(Int64);

    template <Func F>
    static void dispatch(const ColumnPtr col_from, PaddedPODArray<Int64> & vec_to)
    {
        if (const auto * from = checkAndGetColumn<ColumnInt64>(col_from.get()); from)
        {
            const auto & data = from->getData();
            vectorDuration<F>(data, vec_to);
        }
    }

    template <Func F>
    static void vectorDuration(const ColumnInt64::Container & vec_from, PaddedPODArray<Int64> & vec_to)
    {
        vec_to.resize(vec_from.size());
        for (size_t i = 0; i < vec_from.size(); i++)
        {
            vec_to[i] = F(vec_from[i]);
        }
    }
};

} // namespace DB
