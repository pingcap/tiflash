// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionsConditional.h>
#include <WindowFunctions/IWindowFunction.h>
#include <WindowFunctions/WindowFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

struct WindowFunctionRank final : public IWindowFunction
{
    static constexpr auto name = "rank";

    explicit WindowFunctionRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public IWindowFunction
{
    static constexpr auto name = "dense_rank";

    explicit WindowFunctionDenseRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->peer_group_number);
    }
};

struct WindowFunctionRowNumber final : public IWindowFunction
{
    static constexpr auto name = "row_number";

    explicit WindowFunctionRowNumber(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->current_row_number);
    }
};

struct WindowFunctionLead final : public IWindowFunction
{
    static constexpr auto name = "lead";

    explicit WindowFunctionLead(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        if (argument_types.size() <= 2)
        {
            if (removeNullable(argument_types[0])->getTypeId() == TypeIndex::Float32)
            {
                auto origin_type = std::make_shared<DataTypeFloat64>();
                return argument_types[0]->isNullable() ? makeNullable(origin_type) : origin_type;
            }
            else
                return argument_types[0];
        }
        else
        {
            return FunctionIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), removeNullable(argument_types[0]), argument_types[2]});
        }
    }

    // todo hanlde nullable, etc
    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->current_row_number);
    }
};

void registerWindowFunctions(WindowFunctionFactory & factory)
{
    factory.registerFunction<WindowFunctionRank>();
    factory.registerFunction<WindowFunctionDenseRank>();
    factory.registerFunction<WindowFunctionRowNumber>();
}
} // namespace DB
